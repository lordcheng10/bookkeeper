/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.bookkeeper.bookie.storage.directentrylogger;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.bookie.EntryLogMetadata;
import org.apache.bookkeeper.bookie.storage.EntryLogIds;
import org.apache.bookkeeper.common.util.OrderedExecutor;
import org.apache.bookkeeper.common.util.nativeio.NativeIO;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.slogger.Slogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.util.MathUtils;

@Slf4j
public class DirectEntryLoggerForEntryLogPerLedger extends DirectEntryLogger {

    private final LoadingCache<Long, WriterWithMetadata> ledgerIdEntryLogMap;

    private final AtomicReferenceArray<Lock> lockArrayPool;
    private final int maximumNumberOfActiveEntryLogs;
    private final int entrylogMapAccessExpiryTimeInSeconds;
    private final CacheLoader<Long, WriterWithMetadata> entryLogAndLockTupleCacheLoader;


    public DirectEntryLoggerForEntryLogPerLedger(ServerConfiguration conf,
                                                 File ledgerDir, EntryLogIds ids, NativeIO nativeIO,
                                                 ByteBufAllocator allocator, ExecutorService writeExecutor,
                                                 ExecutorService flushExecutor, long maxFileSize,
                                                 int maxSaneEntrySize, long totalWriteBufferSize,
                                                 long totalReadBufferSize, int readBufferSize,
                                                 int numReadThreads, int maxFdCacheTimeSeconds,
                                                 Slogger slogParent, StatsLogger stats) throws IOException {
        super(conf, ledgerDir, ids, nativeIO, allocator, writeExecutor, flushExecutor, maxFileSize, maxSaneEntrySize,
                totalWriteBufferSize, totalReadBufferSize, readBufferSize, numReadThreads, maxFdCacheTimeSeconds,
                slogParent, stats);
        this.maximumNumberOfActiveEntryLogs = conf.getMaximumNumberOfActiveEntryLogs();
        this.entrylogMapAccessExpiryTimeInSeconds = conf.getEntrylogMapAccessExpiryTimeInSeconds();
        this.lockArrayPool = new AtomicReferenceArray<>(maximumNumberOfActiveEntryLogs * 2);
        this.entryLogAndLockTupleCacheLoader = new CacheLoader<Long, WriterWithMetadata>() {
            @Override
            public WriterWithMetadata load(Long key) throws Exception {
               return createWriterWithMetadata(key);
            }
        };
        ledgerIdEntryLogMap = CacheBuilder.newBuilder()
                .expireAfterAccess(entrylogMapAccessExpiryTimeInSeconds, TimeUnit.SECONDS)
                .maximumSize(maximumNumberOfActiveEntryLogs)
                .removalListener(new RemovalListener<Long, WriterWithMetadata>() {
                    @Override
                    public void onRemoval(RemovalNotification<Long, WriterWithMetadata> expiredLedgerEntryLogMapEntry) {
                        long ledgerId = expiredLedgerEntryLogMapEntry.getKey();
                        WriterWithMetadata writer = expiredLedgerEntryLogMapEntry.getValue();
                        Lock lock = getLock(ledgerId);
                        lock.lock();
                        try {
                            if (!writer.getIsClosed().get()) {
                                CompletableFuture<Void> flushPromise = new CompletableFuture<>();
                                synchronized (this) {
                                    pendingFlushes.add(flushPromise);
                                }
                                flushAndCloseWriter(ledgerId, writer, flushPromise);
                            }
                        } finally {
                            lock.unlock();
                        }
                    }
                }).build(entryLogAndLockTupleCacheLoader);
    }


    @Override
    public long addEntry(long ledgerId, ByteBuf buf) throws IOException {
        long start = System.nanoTime();
        long offset;
        Lock lock = getLock(ledgerId);
        lock.lock();
        try {
            WriterWithMetadata writer = getWriterForLedger(ledgerId);
            if (writer.shouldRoll(buf, maxFileSize)) {
                CompletableFuture<Void> flushPromise = new CompletableFuture<>();
                synchronized (this) {
                    pendingFlushes.add(flushPromise);
                }
                // roll the log. asynchronously flush and close current log
                flushAndCloseWriter(ledgerId, writer, flushPromise);
                ledgerIdEntryLogMap.put(ledgerId, createWriterWithMetadata(ledgerId));
                writer = getWriterForLedger(ledgerId);
            }

            offset = writer.addEntry(ledgerId, buf);
        } catch (Throwable e) {
            log.info("chenlin:Received unexpected exception while fetching entry from map", e);
            throw new IOException(e);
        } finally {
            lock.unlock();
        }
        stats.getAddEntryStats().registerSuccessfulEvent(System.nanoTime() - start, TimeUnit.NANOSECONDS);
        return offset;
    }

    @Override
    public void flush() throws IOException {
        long start = System.nanoTime();
        List<Future<?>> outstandingFlushes;
        synchronized (this) {
            outstandingFlushes = this.pendingFlushes;
            this.pendingFlushes = new ArrayList<>();
        }

        ledgerIdEntryLogMap.asMap().forEach((ledgerId, flushWriter) -> {
            CompletableFuture<Void> future = new CompletableFuture<>();
            ((OrderedExecutor) flushExecutor).executeOrdered(ledgerId, () -> {
                long flushStart = System.nanoTime();
                Lock lock = getLock(ledgerId);
                lock.lock();
                try {
                    flushWriter.flush();
                    stats.getWriterFlushStats().registerSuccessfulEvent(
                            System.nanoTime() - flushStart, TimeUnit.NANOSECONDS);
                    future.complete(null);
                } catch (Throwable t) {
                    stats.getWriterFlushStats().registerFailedEvent(
                            System.nanoTime() - flushStart, TimeUnit.NANOSECONDS);
                    future.completeExceptionally(t);
                } finally {
                    lock.unlock();
                }
            });
            outstandingFlushes.add(future);
        });

        for (Future<?> f : outstandingFlushes) {
            try {
                f.get();
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                throw new IOException("Interruped while flushing", ie);
            } catch (ExecutionException ee) {
                if (ee.getCause() instanceof IOException) {
                    throw (IOException) ee.getCause();
                } else {
                    throw new IOException("Exception flushing writer", ee);
                }
            }
        }

        stats.getFlushStats().registerSuccessfulEvent(System.nanoTime() - start, TimeUnit.NANOSECONDS);
    }

    @Override
    protected void flushAndCloseCurrent() throws IOException {
        // remove triggers removalListener, which will flush and close all writers
        ledgerIdEntryLogMap.invalidateAll();
    }

    private void flushAndCloseWriter(long ledgerId, WriterWithMetadata flushWriter, CompletableFuture<Void> flushPromise) {
        if (flushWriter != null) {
            ((OrderedExecutor) flushExecutor).executeOrdered(ledgerId, () -> {
                long start = System.nanoTime();
                try {
                    flushWriter.finalizeAndClose();
                    stats.getWriterFlushStats()
                            .registerSuccessfulEvent(System.nanoTime() - start, TimeUnit.NANOSECONDS);
                    unflushedLogs.remove(flushWriter.logId());
                    flushPromise.complete(null);
                } catch (Throwable t) {
                    slog.info("chenlin3:flushAndCloseWriter ", t);
                    stats.getWriterFlushStats()
                            .registerFailedEvent(System.nanoTime() - start, TimeUnit.NANOSECONDS);
                    flushPromise.completeExceptionally(t);
                }
            });
        } else {
            flushPromise.complete(null);
        }
    }

    private Lock getLock(long ledgerId) {
        int lockIndex = MathUtils.signSafeMod(Long.hashCode(ledgerId), lockArrayPool.length());
        if (lockArrayPool.get(lockIndex) == null) {
            synchronized (this) {
                if (lockArrayPool.get(lockIndex) == null) {
                    lockArrayPool.compareAndSet(lockIndex, null, new ReentrantLock());
                }
            }
        }
        return lockArrayPool.get(lockIndex);
    }

    private WriterWithMetadata getWriterForLedger(long ledgerId) throws IOException {
        try {
            return ledgerIdEntryLogMap.get(ledgerId);
        } catch (Exception e) {
            log.error("Received unexpected exception while fetching entry from map for ledger: " + ledgerId, e);
            throw new IOException("Received unexpected exception while fetching entry from map", e);
        }
    }

    protected LogWriter newDirectWriter(long ledgerId, int newId) throws IOException {
        LogWriter writer =  super.newDirectWriter(newId);
        writer.setLedgerId(ledgerId);
        return writer;
    }

    public WriterWithMetadata createWriterWithMetadata(long ledgerId) throws IOException {
        int newId = ids.nextId();
        log.info("chenlin4:newId: " + newId);
        return new WriterWithMetadata(newDirectWriter(ledgerId, newId),
                new EntryLogMetadata(newId),
                allocator);
    }

}
