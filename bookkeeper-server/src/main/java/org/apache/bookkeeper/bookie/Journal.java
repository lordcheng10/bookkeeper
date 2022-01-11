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

package org.apache.bookkeeper.bookie;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Stopwatch;
import com.google.common.util.concurrent.MoreExecutors;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.util.Recycler;
import io.netty.util.Recycler.Handle;
import io.netty.util.concurrent.DefaultThreadFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.bookkeeper.bookie.LedgerDirsManager.NoWritableLedgerDirException;
import org.apache.bookkeeper.bookie.stats.JournalStats;
import org.apache.bookkeeper.common.collections.BlockingMpscQueue;
import org.apache.bookkeeper.common.collections.RecyclableArrayList;
import org.apache.bookkeeper.common.util.MemoryLimitController;
import org.apache.bookkeeper.common.util.affinity.CpuAffinity;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.WriteCallback;
import org.apache.bookkeeper.stats.Counter;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.util.IOUtils;
import org.apache.bookkeeper.util.MathUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 提供期刊相关管理。
 * Provide journal related management.
 */
public class Journal extends BookieCriticalThread implements CheckpointSource {

    private static final Logger LOG = LoggerFactory.getLogger(Journal.class);
    //条目列表回收器
    private static final RecyclableArrayList.Recycler<QueueEntry> entryListRecycler =
        new RecyclableArrayList.Recycler<QueueEntry>();
    private static final RecyclableArrayList<QueueEntry> EMPTY_ARRAY_LIST = new RecyclableArrayList<>();

    /**
     * 过滤到拾取期刊。
     * Filter to pickup journals.
     */
    public interface JournalIdFilter {
        boolean accept(long journalId);
    }

    /**
     * For testability.
     */
    @FunctionalInterface
    public interface BufferedChannelBuilder {
        BufferedChannelBuilder DEFAULT_BCBUILDER = (FileChannel fc,
                int capacity) -> new BufferedChannel(UnpooledByteBufAllocator.DEFAULT, fc, capacity);

        BufferedChannel create(FileChannel fc, int capacity) throws IOException;
    }



    /**
     * 按指定的日志 ID 文件管理器列出所有日志 ID。
     * List all journal ids by a specified journal id filer.
     *
     * @param journalDir journal dir
     * @param filter journal id filter
     * @return list of filtered ids
     */
    static List<Long> listJournalIds(File journalDir, JournalIdFilter filter) {
        File[] logFiles = journalDir.listFiles();
        if (logFiles == null || logFiles.length == 0) {
            return Collections.emptyList();
        }
        List<Long> logs = new ArrayList<Long>();
        for (File f: logFiles) {
            String name = f.getName();
            if (!name.endsWith(".txn")) {
                continue;
            }
            String idString = name.split("\\.")[0];
            long id = Long.parseLong(idString, 16);
            if (filter != null) {
                if (filter.accept(id)) {
                    logs.add(id);
                }
            } else {
                logs.add(id);
            }
        }
        Collections.sort(logs);
        return logs;
    }

    /**
     * 日志标记的包装器，为日志用户提供检查点以进行检查点。
     * A wrapper over log mark to provide a checkpoint for users of journal
     * to do checkpointing.
     */
    private static class LogMarkCheckpoint implements Checkpoint {
        final LastLogMark mark;

        public LogMarkCheckpoint(LastLogMark checkpoint) {
            this.mark = checkpoint;
        }

        @Override
        public int compareTo(Checkpoint o) {
            if (o == Checkpoint.MAX) {
                return -1;
            } else if (o == Checkpoint.MIN) {
                return 1;
            }
            return mark.getCurMark().compare(((LogMarkCheckpoint) o).mark.getCurMark());
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof LogMarkCheckpoint)) {
                return false;
            }
            return 0 == compareTo((LogMarkCheckpoint) o);
        }

        @Override
        public int hashCode() {
            return mark.hashCode();
        }

        @Override
        public String toString() {
            return mark.toString();
        }
    }

    /**
     * 最后日志标记。
     * Last Log Mark.
     */
    public class LastLogMark {
        private final LogMark curMark;
        //logId和log position位置
        LastLogMark(long logId, long logPosition) {
            this.curMark = new LogMark(logId, logPosition);
        }

        void setCurLogMark(long logId, long logPosition) {
            curMark.setLogMark(logId, logPosition);
        }

        LastLogMark markLog() {
            return new LastLogMark(curMark.getLogFileId(), curMark.getLogFileOffset());
        }

        public LogMark getCurMark() {
            return curMark;
        }

        void rollLog(LastLogMark lastMark) throws NoWritableLedgerDirException {
            byte[] buff = new byte[16];
            ByteBuffer bb = ByteBuffer.wrap(buff);
            // 我们应该记录在 markLog 中标记的 <logId, logPosition> ，这是安全的，
            // 因为 lastMark 之前的记录已被持久化到磁盘（索引和条目记录器）
            // 先更新上一次的
            // we should record <logId, logPosition> marked in markLog
            // which is safe since records before lastMark have been
            // persisted to disk (both index & entry logger)
            // 这里会把上一次的mark位置写到bufer bb中
            lastMark.getCurMark().writeLogMark(bb);

            if (LOG.isDebugEnabled()) {
                LOG.debug("RollLog to persist last marked log : {}", lastMark.getCurMark());
            }

            //遍历所有的ledger 目录
            List<File> writableLedgerDirs = ledgerDirsManager
                    .getWritableLedgerDirsForNewLog();
            for (File dir : writableLedgerDirs) {
                File file = new File(dir, lastMarkFileName);
                FileOutputStream fos = null;
                try {
                    //然后这里会把buffer的数据写到每个
                    fos = new FileOutputStream(file);
                    fos.write(buff);
                    fos.getChannel().force(true);
                    fos.close();
                    fos = null;
                } catch (IOException e) {
                    LOG.error("Problems writing to " + file, e);
                } finally {
                    // if stream already closed in try block successfully,
                    // stream might have nullified, in such case below
                    // call will simply returns
                    IOUtils.close(LOG, fos);
                }
            }
        }

        /**
         * 从 lastMark 文件中读取最后一个标记。
         * 最后一个标记应该首先是 jounal id和对应的jounal文件位置.
         *
         * Read last mark from lastMark file.
         * The last mark should first be max journal log id,
         * and then max log position in max journal log.
         */
        void readLog() {
            byte[] buff = new byte[16];
            ByteBuffer bb = ByteBuffer.wrap(buff);
            LogMark mark = new LogMark();
            for (File dir: ledgerDirsManager.getAllLedgerDirs()) {
                //这里是遍历每个ledger目录，从中读取lastMark文件里的写入的最后一个ledger文件Id和写入位置,读取到buffer中
                File file = new File(dir, lastMarkFileName);
                try {
                    try (FileInputStream fis = new FileInputStream(file)) {
                        int bytesRead = fis.read(buff);
                        if (bytesRead != 16) {
                            throw new IOException("Couldn't read enough bytes from lastMark."
                                                  + " Wanted " + 16 + ", got " + bytesRead);
                        }
                    }
                    bb.clear();
                    //这里回从buffer中读取到mark中
                    mark.readLogMark(bb);
                    if (curMark.compare(mark) < 0) {//如果小于0，就代表这里读出来的mark更大一些，因此用来更新成当前的mark
                        curMark.setLogMark(mark.getLogFileId(), mark.getLogFileOffset());
                    }
                } catch (IOException e) {
                    LOG.error("Problems reading from " + file + " (this is okay if it is the first time starting this "
                            + "bookie");
                }
            }
        }

        @Override
        public String toString() {
            return curMark.toString();
        }
    }

    /**
     * 过滤以返回滚动期刊列表。
     * Filter to return list of journals for rolling.
     */
    private static class JournalRollingFilter implements JournalIdFilter {

        final LastLogMark lastMark;

        JournalRollingFilter(LastLogMark lastMark) {
            this.lastMark = lastMark;
        }

        @Override
        public boolean accept(long journalId) {
            return journalId < lastMark.getCurMark().getLogFileId();
        }
    }

    /**
     * 用于扫描期刊的扫描仪。
     * Scanner used to scan a journal.
     */
    public interface JournalScanner {
        /**
         * 处理日记帐分录。
         * Process a journal entry.
         *
         * @param journalVersion Journal Version
         * @param offset File offset of the journal entry
         * @param entry Journal Entry
         * @throws IOException
         */
        void process(int journalVersion, long offset, ByteBuffer entry) throws IOException;
    }

    /**
     * 要记录的日记帐分录。
     * Journal Entry to Record.
     */
    static class QueueEntry implements Runnable {
        ByteBuf entry;
        long ledgerId;
        long entryId;
        WriteCallback cb;
        Object ctx;
        long enqueueTime;
        boolean ackBeforeSync;

        OpStatsLogger journalAddEntryStats;
        Counter journalCbQueueSize;


        static QueueEntry create(ByteBuf entry, boolean ackBeforeSync, long ledgerId, long entryId,
                WriteCallback cb, Object ctx, long enqueueTime, OpStatsLogger journalAddEntryStats,
                Counter journalCbQueueSize) {
            QueueEntry qe = RECYCLER.get();
            qe.entry = entry;
            qe.ackBeforeSync = ackBeforeSync;
            qe.cb = cb;
            qe.ctx = ctx;
            qe.ledgerId = ledgerId;
            qe.entryId = entryId;
            qe.enqueueTime = enqueueTime;
            qe.journalAddEntryStats = journalAddEntryStats;
            qe.journalCbQueueSize = journalCbQueueSize;
            return qe;
        }

        @Override
        public void run() {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Acknowledge Ledger: {}, Entry: {}", ledgerId, entryId);
            }
            journalCbQueueSize.dec();
            journalAddEntryStats.registerSuccessfulEvent(MathUtils.elapsedNanos(enqueueTime), TimeUnit.NANOSECONDS);
            cb.writeComplete(0, ledgerId, entryId, null, ctx);
            recycle();
        }

        private final Handle<QueueEntry> recyclerHandle;

        private QueueEntry(Handle<QueueEntry> recyclerHandle) {
            this.recyclerHandle = recyclerHandle;
        }

        private static final Recycler<QueueEntry> RECYCLER = new Recycler<QueueEntry>() {
            @Override
            protected QueueEntry newObject(Recycler.Handle<QueueEntry> handle) {
                return new QueueEntry(handle);
            }
        };

        private void recycle() {
            recyclerHandle.recycle(this);
        }
    }

    /**
     * 表示需要强制写入日志的令牌。
     * Token which represents the need to force a write to the Journal.
     */
    @VisibleForTesting
    public class ForceWriteRequest {
        private JournalChannel logFile;
        //攒批写
        private RecyclableArrayList<QueueEntry> forceWriteWaiters;
        private boolean shouldClose;
        private boolean isMarker;
        private long lastFlushedPosition;
        private long logId;
        private long enqueueTime;

        public int process(boolean shouldForceWrite) throws IOException {
            //强制写请求队列数减1
            journalStats.getForceWriteQueueSize().dec();
            //更新在队列中的耗时
            journalStats.getFwEnqueueTimeStats()
                .registerSuccessfulEvent(MathUtils.elapsedNanos(enqueueTime), TimeUnit.NANOSECONDS);

            //如果是isMaker就直接返回
            if (isMarker) {
                return 0;
            }

            try {
                //是否应该强刷,只有shouldForceWrite为true的时候，才可以进入强制flush，否则跳过，然后直接走到后面触发回调
                if (shouldForceWrite) {
                    //如果要强刷的话，这里回强刷
                    long startTime = MathUtils.nowInNano();
                    this.logFile.forceWrite(false);
                    journalStats.getJournalSyncStats()
                        .registerSuccessfulEvent(MathUtils.elapsedNanos(startTime), TimeUnit.NANOSECONDS);
                }
                //这里在flush后，这里会更新logId和lastFlushedPosition
                lastLogMark.setCurLogMark(this.logId, this.lastFlushedPosition);

                //强制刷新成功后，这里回调用callback
                // Notify the waiters that the force write succeeded
                for (int i = 0; i < forceWriteWaiters.size(); i++) {
                    QueueEntry qe = forceWriteWaiters.get(i);
                    if (qe != null) {
                        cbThreadPool.execute(qe);
                    }
                }

                //返回强制刷新的entry数
                return forceWriteWaiters.size();
            } finally {
                //这里如果需要的话，会关闭该文件
                closeFileIfNecessary();
            }
        }

        public void closeFileIfNecessary() {
            // Close if shouldClose is set
            if (shouldClose) {
                // We should guard against exceptions so its
                // safe to call in catch blocks
                try {
                    logFile.close();
                    // Call close only once
                    shouldClose = false;
                } catch (IOException ioe) {
                    LOG.error("I/O exception while closing file", ioe);
                }
            }
        }

        private final Handle<ForceWriteRequest> recyclerHandle;

        private ForceWriteRequest(Handle<ForceWriteRequest> recyclerHandle) {
            this.recyclerHandle = recyclerHandle;
        }

        private void recycle() {
            logFile = null;
            if (forceWriteWaiters != null) {
                forceWriteWaiters.recycle();
                forceWriteWaiters = null;
            }
            recyclerHandle.recycle(this);
        }
    }

    //创建强制刷新请求
    private ForceWriteRequest createForceWriteRequest(JournalChannel logFile,
                          long logId,
                          long lastFlushedPosition,
                          RecyclableArrayList<QueueEntry> forceWriteWaiters,
                          boolean shouldClose,
                          boolean isMarker) {
        ForceWriteRequest req = forceWriteRequestsRecycler.get();
        req.forceWriteWaiters = forceWriteWaiters;
        req.logFile = logFile;
        req.logId = logId;
        req.lastFlushedPosition = lastFlushedPosition;
        req.shouldClose = shouldClose;
        req.isMarker = isMarker;
        req.enqueueTime = MathUtils.nowInNano();
        journalStats.getForceWriteQueueSize().inc();
        return req;
    }

    private final Recycler<ForceWriteRequest> forceWriteRequestsRecycler = new Recycler<ForceWriteRequest>() {
                @Override
                protected ForceWriteRequest newObject(
                        Recycler.Handle<ForceWriteRequest> handle) {
                    return new ForceWriteRequest(handle);
                }
            };

    /**
     * ForceWriteThread 是一个后台线程，它使日志定期持久化。
     * ForceWriteThread is a background thread which makes the journal durable periodically.
     *
     */
    private class ForceWriteThread extends BookieCriticalThread {
        volatile boolean running = true;
        // This holds the queue entries that should be notified after a
        // successful force write
        Thread threadToNotifyOnEx;
        // should we group force writes
        private final boolean enableGroupForceWrites;
        // make flush interval as a parameter
        public ForceWriteThread(Thread threadToNotifyOnEx, boolean enableGroupForceWrites) {
            super("ForceWriteThread");
            this.threadToNotifyOnEx = threadToNotifyOnEx;
            this.enableGroupForceWrites = enableGroupForceWrites;
        }
        @Override
        public void run() {
            LOG.info("ForceWrite Thread started");

            if (conf.isBusyWaitEnabled()) {
                try {
                    CpuAffinity.acquireCore();
                } catch (Exception e) {
                    LOG.warn("Unable to acquire CPU core for Journal ForceWrite thread: {}", e.getMessage(), e);
                }
            }

            boolean shouldForceWrite = true;
            int numReqInLastForceWrite = 0;
            while (running) {
                ForceWriteRequest req = null;
                try {
                    //从队列中取出来
                    req = forceWriteRequests.take();
                    // 强制写入文件，然后通知写入完成
                    // 第一次取出来isMarker是false
                    // Force write the file and then notify the write completions
                    //
                    if (!req.isMarker) {
                        //是否应该强制刷新,刚开始为true
                        if (shouldForceWrite) {
                            // 如果我们要强制写入，任何已经在队列中的请求都将受益于这种强制写入 - 在发出刷新之前发布一个标记，所以在遇到这个标记之前我们可以跳过强制写入
                            // 默认enableGroupForceWrites是打开的
                            // if we are going to force write, any request that is already in the
                            // queue will benefit from this force write - post a marker prior to issuing
                            // the flush so until this marker is encountered we can skip the force write
                            if (enableGroupForceWrites) {
                                //这里往队列中放入一个标记元素
                                forceWriteRequests.put(createForceWriteRequest(req.logFile, 0, 0, null, false, true));
                            }

                            // 如果我们要发出写入，记录最后一次强制写入的请求数，然后重置计数器，以便我们可以在即将发出的写入中累积请求
                            // If we are about to issue a write, record the number of requests in
                            // the last force write and then reset the counter so we can accumulate
                            // requests in the write we are about to issue
                            if (numReqInLastForceWrite > 0) {
                                //这里更新metric
                                journalStats.getForceWriteGroupingCountStats()
                                    .registerSuccessfulValue(numReqInLastForceWrite);
                                numReqInLastForceWrite = 0;
                            }
                        }
                    }
                    //开始强制刷新，shouldForceWrite第一次为true
                    numReqInLastForceWrite += req.process(shouldForceWrite);

                    if (enableGroupForceWrites
                            // if its a marker we should switch back to flushing
                            && !req.isMarker
                            // shouldClose=true代表是要轮转的文件
                            // 这表明这是给定文件中的最后一个请求，因此后续请求将转到另一个文件，因此我们应该在下一个请求时刷新
                            // This indicates that this is the last request in a given file
                            // so subsequent requests will go to a different file so we should
                            // flush on the next request
                            && !req.shouldClose) {
                        //如果不是要轮转的文件，就不强制刷新了
                        shouldForceWrite = false;
                    } else {
                        shouldForceWrite = true;
                    }
                } catch (IOException ioe) {
                    LOG.error("I/O exception in ForceWrite thread", ioe);
                    running = false;
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    LOG.info("ForceWrite thread interrupted");
                    // close is idempotent
                    if (null != req) {
                        req.shouldClose = true;
                        req.closeFileIfNecessary();
                    }
                    running = false;
                } finally {
                    if (req != null) {
                        req.recycle();
                    }
                }
            }
            // Regardless of what caused us to exit, we should notify the
            // the parent thread as it should either exit or be in the process
            // of exiting else we will have write requests hang
            threadToNotifyOnEx.interrupt();
        }
        // shutdown sync thread
        void shutdown() throws InterruptedException {
            running = false;
            this.interrupt();
            this.join();
        }
    }

    static final int PADDING_MASK = -0x100;

    static void writePaddingBytes(JournalChannel jc, ByteBuf paddingBuffer, int journalAlignSize)
            throws IOException {
        int bytesToAlign = (int) (jc.bc.position() % journalAlignSize);
        if (0 != bytesToAlign) {
            int paddingBytes = journalAlignSize - bytesToAlign;
            if (paddingBytes < 8) {
                paddingBytes = journalAlignSize - (8 - paddingBytes);
            } else {
                paddingBytes -= 8;
            }
            paddingBuffer.clear();
            // padding mask
            paddingBuffer.writeInt(PADDING_MASK);
            // padding len
            paddingBuffer.writeInt(paddingBytes);
            // padding bytes
            paddingBuffer.writerIndex(paddingBuffer.writerIndex() + paddingBytes);

            jc.preAllocIfNeeded(paddingBuffer.readableBytes());
            // write padding bytes
            jc.bc.write(paddingBuffer);
        }
    }

    static final long MB = 1024 * 1024L;
    static final int KB = 1024;
    // max journal file size
    final long maxJournalSize;
    // pre-allocation size for the journal files
    final long journalPreAllocSize;
    // write buffer size for the journal files
    final int journalWriteBufferSize;
    // number journal files kept before marked journal
    final int maxBackupJournals;

    final File journalDirectory;
    final ServerConfiguration conf;
    final ForceWriteThread forceWriteThread;
    // 我们将停止分组并发出刷新的时间
    // Time after which we will stop grouping and issue the flush
    private final long maxGroupWaitInNanos;
    // Threshold after which we flush any buffered journal entries
    private final long bufferedEntriesThreshold;
    // Threshold after which we flush any buffered journal writes
    private final long bufferedWritesThreshold;
    // should we flush if the queue is empty
    private final boolean flushWhenQueueEmpty;
    // 我们是否应该提示文件系统在强制写入后从缓存中删除页面
    // should we hint the filesystem to remove pages from cache after force write
    private final boolean removePagesFromCache;
    private final int journalFormatVersionToWrite;
    private final int journalAlignmentSize;
    // control PageCache flush interval when syncData disabled to reduce disk io util
    private final long journalPageCacheFlushIntervalMSec;

    // Should data be fsynced on disk before triggering the callback
    private final boolean syncData;

    private final LastLogMark lastLogMark = new LastLogMark(0, 0);

    private static final String LAST_MARK_DEFAULT_NAME = "lastMark";

    private final String lastMarkFileName;

    /**
     * The thread pool used to handle callback.
     */
    private final ExecutorService cbThreadPool;

    // journal entry queue to commit
    final BlockingQueue<QueueEntry> queue;
    final BlockingQueue<ForceWriteRequest> forceWriteRequests;

    volatile boolean running = true;
    private final LedgerDirsManager ledgerDirsManager;
    private final ByteBufAllocator allocator;
    private final MemoryLimitController memoryLimitController;

    // Expose Stats
    private final JournalStats journalStats;

    public Journal(int journalIndex, File journalDirectory, ServerConfiguration conf,
            LedgerDirsManager ledgerDirsManager) {
        this(journalIndex, journalDirectory, conf, ledgerDirsManager, NullStatsLogger.INSTANCE,
                UnpooledByteBufAllocator.DEFAULT);
    }

    public Journal(int journalIndex, File journalDirectory, ServerConfiguration conf,
            LedgerDirsManager ledgerDirsManager, StatsLogger statsLogger, ByteBufAllocator allocator) {
        super("BookieJournal-" + conf.getBookiePort());
        this.allocator = allocator;

        if (conf.isBusyWaitEnabled()) {
            // To achieve lower latency, use busy-wait blocking queue implementation
            queue = new BlockingMpscQueue<>(conf.getJournalQueueSize());
            forceWriteRequests = new BlockingMpscQueue<>(conf.getJournalQueueSize());
        } else {
            queue = new ArrayBlockingQueue<>(conf.getJournalQueueSize());
            forceWriteRequests = new ArrayBlockingQueue<>(conf.getJournalQueueSize());
        }

        // Adjust the journal max memory in case there are multiple journals configured.
        long journalMaxMemory = conf.getJournalMaxMemorySizeMb() / conf.getJournalDirNames().length * 1024 * 1024;
        this.memoryLimitController = new MemoryLimitController(journalMaxMemory);
        this.ledgerDirsManager = ledgerDirsManager;
        this.conf = conf;
        this.journalDirectory = journalDirectory;
        this.maxJournalSize = conf.getMaxJournalSizeMB() * MB;
        this.journalPreAllocSize = conf.getJournalPreAllocSizeMB() * MB;
        this.journalWriteBufferSize = conf.getJournalWriteBufferSizeKB() * KB;
        this.syncData = conf.getJournalSyncData();
        this.maxBackupJournals = conf.getMaxBackupJournals();
        this.forceWriteThread = new ForceWriteThread(this, conf.getJournalAdaptiveGroupWrites());
        this.maxGroupWaitInNanos = TimeUnit.MILLISECONDS.toNanos(conf.getJournalMaxGroupWaitMSec());
        this.bufferedWritesThreshold = conf.getJournalBufferedWritesThreshold();
        this.bufferedEntriesThreshold = conf.getJournalBufferedEntriesThreshold();
        this.journalFormatVersionToWrite = conf.getJournalFormatVersionToWrite();
        this.journalAlignmentSize = conf.getJournalAlignmentSize();
        this.journalPageCacheFlushIntervalMSec = conf.getJournalPageCacheFlushIntervalMSec();
        if (conf.getNumJournalCallbackThreads() > 0) {
            this.cbThreadPool = Executors.newFixedThreadPool(conf.getNumJournalCallbackThreads(),
                                                         new DefaultThreadFactory("bookie-journal-callback"));
        } else {
            this.cbThreadPool = MoreExecutors.newDirectExecutorService();
        }

        // Unless there is a cap on the max wait (which requires group force writes)
        // we cannot skip flushing for queue empty
        this.flushWhenQueueEmpty = maxGroupWaitInNanos <= 0 || conf.getJournalFlushWhenQueueEmpty();

        this.removePagesFromCache = conf.getJournalRemovePagesFromCache();
        // 如果jounal目录只有一个，那么就只生成一个lastMark文件，否则就生成lastMark.1 lastMark.2....这些文件
        // read last log mark
        if (conf.getJournalDirs().length == 1) {
            lastMarkFileName = LAST_MARK_DEFAULT_NAME;
        } else {
            lastMarkFileName = LAST_MARK_DEFAULT_NAME + "." + journalIndex;
        }
        lastLogMark.readLog();
        if (LOG.isDebugEnabled()) {
            LOG.debug("Last Log Mark : {}", lastLogMark.getCurMark());
        }

        // Expose Stats
        this.journalStats = new JournalStats(statsLogger);
    }

    JournalStats getJournalStats() {
        return this.journalStats;
    }

    public File getJournalDirectory() {
        return journalDirectory;
    }

    public LastLogMark getLastLogMark() {
        return lastLogMark;
    }

    /**
     * Update lastLogMark of the journal
     * Indicates that the file has been processed.
     * @param id
     * @param scanOffset
     */
    void setLastLogMark(Long id, long scanOffset) {
        lastLogMark.setCurLogMark(id, scanOffset);
    }

    /**
     * Application tried to schedule a checkpoint. After all the txns added
     * before checkpoint are persisted, a <i>checkpoint</i> will be returned
     * to application. Application could use <i>checkpoint</i> to do its logic.
     */
    @Override
    public Checkpoint newCheckpoint() {
        return new LogMarkCheckpoint(lastLogMark.markLog());
    }

    /**
     * Telling journal a checkpoint is finished.
     *
     * @throws IOException
     */
    @Override
    public void checkpointComplete(Checkpoint checkpoint, boolean compact) throws IOException {
        if (!(checkpoint instanceof LogMarkCheckpoint)) {
            return; // we didn't create this checkpoint, so dont do anything with it
        }
        LogMarkCheckpoint lmcheckpoint = (LogMarkCheckpoint) checkpoint;
        LastLogMark mark = lmcheckpoint.mark;

        //将mark持久化到磁盘
        mark.rollLog(mark);
        //将小于mark位置的删除掉
        if (compact) {
            // list the journals that have been marked
            List<Long> logs = listJournalIds(journalDirectory, new JournalRollingFilter(mark));
            // keep MAX_BACKUP_JOURNALS journal files before marked journal
            if (logs.size() >= maxBackupJournals) {
                int maxIdx = logs.size() - maxBackupJournals;
                for (int i = 0; i < maxIdx; i++) {
                    long id = logs.get(i);
                    // make sure the journal id is smaller than marked journal id
                    if (id < mark.getCurMark().getLogFileId()) {
                        File journalFile = new File(journalDirectory, Long.toHexString(id) + ".txn");
                        if (!journalFile.delete()) {
                            LOG.warn("Could not delete old journal file {}", journalFile);
                        }
                        LOG.info("garbage collected journal " + journalFile.getName());
                    }
                }
            }
        }
    }

    /**
     * Scan the journal.
     *
     * @param journalId Journal Log Id
     * @param journalPos Offset to start scanning
     * @param scanner Scanner to handle entries
     * @return scanOffset - represents the byte till which journal was read
     * @throws IOException
     */
    public long scanJournal(long journalId, long journalPos, JournalScanner scanner)
        throws IOException {
        JournalChannel recLog;
        if (journalPos <= 0) {
            recLog = new JournalChannel(journalDirectory, journalId, journalPreAllocSize, journalWriteBufferSize);
        } else {
            recLog = new JournalChannel(journalDirectory, journalId, journalPreAllocSize, journalWriteBufferSize,
                    journalPos);
        }
        int journalVersion = recLog.getFormatVersion();
        try {
            ByteBuffer lenBuff = ByteBuffer.allocate(4);
            ByteBuffer recBuff = ByteBuffer.allocate(64 * 1024);
            while (true) {
                // entry start offset
                long offset = recLog.fc.position();
                // start reading entry
                lenBuff.clear();
                fullRead(recLog, lenBuff);
                if (lenBuff.remaining() != 0) {
                    break;
                }
                lenBuff.flip();
                int len = lenBuff.getInt();
                if (len == 0) {
                    break;
                }
                boolean isPaddingRecord = false;
                if (len < 0) {
                    if (len == PADDING_MASK && journalVersion >= JournalChannel.V5) {
                        // skip padding bytes
                        lenBuff.clear();
                        fullRead(recLog, lenBuff);
                        if (lenBuff.remaining() != 0) {
                            break;
                        }
                        lenBuff.flip();
                        len = lenBuff.getInt();
                        if (len == 0) {
                            continue;
                        }
                        isPaddingRecord = true;
                    } else {
                        LOG.error("Invalid record found with negative length: {}", len);
                        throw new IOException("Invalid record found with negative length " + len);
                    }
                }
                recBuff.clear();
                if (recBuff.remaining() < len) {
                    recBuff = ByteBuffer.allocate(len);
                }
                recBuff.limit(len);
                if (fullRead(recLog, recBuff) != len) {
                    // This seems scary, but it just means that this is where we
                    // left off writing
                    break;
                }
                recBuff.flip();
                if (!isPaddingRecord) {
                    scanner.process(journalVersion, offset, recBuff);
                }
            }
            return recLog.fc.position();
        } finally {
            recLog.close();
        }
    }

    public void logAddEntry(ByteBuffer entry, boolean ackBeforeSync, WriteCallback cb, Object ctx)
            throws InterruptedException {
        logAddEntry(Unpooled.wrappedBuffer(entry), ackBeforeSync, cb, ctx);
    }

    /**
     * record an add entry operation in journal.
     */
    public void logAddEntry(ByteBuf entry, boolean ackBeforeSync, WriteCallback cb, Object ctx)
            throws InterruptedException {
        long ledgerId = entry.getLong(entry.readerIndex() + 0);
        long entryId = entry.getLong(entry.readerIndex() + 8);
        logAddEntry(ledgerId, entryId, entry, ackBeforeSync, cb, ctx);
    }

    @VisibleForTesting
    public void logAddEntry(long ledgerId, long entryId, ByteBuf entry,
                            boolean ackBeforeSync, WriteCallback cb, Object ctx)
            throws InterruptedException {
        // Retain entry until it gets written to journal
        entry.retain();

        //这里队列就加1了
        journalStats.getJournalQueueSize().inc();
        journalStats.getJournalCbQueueSize().inc();

        memoryLimitController.reserveMemory(entry.readableBytes());

        //这里放入队列，这里是传入了入队时间的
        queue.put(QueueEntry.create(
                entry, ackBeforeSync,  ledgerId, entryId, cb, ctx, MathUtils.nowInNano(),
                journalStats.getJournalAddEntryStats(),
                journalStats.getJournalCbQueueSize()));
    }

    void forceLedger(long ledgerId, WriteCallback cb, Object ctx) {
        queue.add(QueueEntry.create(
                null, false /* ackBeforeSync */, ledgerId,
                Bookie.METAENTRY_ID_FORCE_LEDGER, cb, ctx, MathUtils.nowInNano(),
                journalStats.getJournalForceLedgerStats(),
                journalStats.getJournalCbQueueSize()));
        // Increment afterwards because the add operation could fail.
        journalStats.getJournalQueueSize().inc();
        journalStats.getJournalCbQueueSize().inc();
    }

    /**
     * Get the length of journal entries queue.
     *
     * @return length of journal entry queue.
     */
    public int getJournalQueueLength() {
        return queue.size();
    }

    /**
     * 用于将日志条目持久保存到日志文件的线程。
     * A thread used for persisting journal entries to journal files.
     *
     * <p>
     * Besides persisting journal entries, it also takes responsibility of
     * rolling journal files when a journal file reaches journal file size
     * limitation.
     * </p>
     * <p>
     * During journal rolling, it first closes the writing journal, generates
     * new journal file using current timestamp, and continue persistence logic.
     * Those journals will be garbage collected in SyncThread.
     * </p>
     * @see org.apache.bookkeeper.bookie.SyncThread
     */
    @Override
    public void run() {
        LOG.info("Starting journal on {}", journalDirectory);

        //判断是否开启了繁忙等待
        if (conf.isBusyWaitEnabled()) {
            try {
                //等要有空闲cpu为当前线程
                CpuAffinity.acquireCore();
            } catch (Exception e) {
                LOG.warn("Unable to acquire CPU core for Journal thread: {}", e.getMessage(), e);
            }
        }

        //toFlush是干嘛的
        RecyclableArrayList<QueueEntry> toFlush = entryListRecycler.newInstance();
        //要flush的entry数
        int numEntriesToFlush = 0;
        //buffer长度
        ByteBuf lenBuff = Unpooled.buffer(4);
        //填充buff
        ByteBuf paddingBuff = Unpooled.buffer(2 * conf.getJournalAlignmentSize());
        paddingBuff.writeZero(paddingBuff.capacity());

        //bc ： buffer channel
        BufferedChannel bc = null;
        //logFile: journal channel
        JournalChannel logFile = null;
        //启动强制写线程
        forceWriteThread.start();
        Stopwatch journalCreationWatcher = Stopwatch.createUnstarted();
        Stopwatch journalFlushWatcher = Stopwatch.createUnstarted();
        long batchSize = 0;
        try {
            //列出该journal目录下所有的文件ID
            List<Long> journalIds = listJournalIds(journalDirectory, null);
            // 不应使用 MathUtils.now()，它使用 System.nanoTime() 和
            // 只能用于测量经过的时间。
            // Should not use MathUtils.now(), which use System.nanoTime() and
            // could only be used to measure elapsed time.
            // http://docs.oracle.com/javase/1.5.0/docs/api/java/lang/System.html#nanoTime%28%29
            //如果journalIds为空，就说明该目录下没有文件，那么此时的logId就用当前时间戳，否则该logId选最后那一个
            //第一个journal文件名，是以当前时间戳来命名的，后面是在该时间戳基础上不停递增+1
            long logId = journalIds.isEmpty() ? System.currentTimeMillis() : journalIds.get(journalIds.size() - 1);
            //上次flush位置
            long lastFlushPosition = 0;
            //当超时的时候，是否分组
            boolean groupWhenTimeout = false;

            //出队列开始时间
            long dequeueStartTime = 0L;
            //上次flush时间
            long lastFlushTimeMs = System.currentTimeMillis();

            //这里是队列元素
            QueueEntry qe = null;
            while (true) {
                // 如果logFile为null，那么需要打开一个文件
                // new journal file to write
                if (null == logFile) {
                    //计算出下一个文件的Id
                    logId = logId + 1;
                    //这个应该是专门用来统计时间的
                    journalCreationWatcher.reset().start();
                    logFile = new JournalChannel(journalDirectory, logId, journalPreAllocSize, journalWriteBufferSize,
                                        journalAlignmentSize, removePagesFromCache,
                                        journalFormatVersionToWrite, getBufferedChannelBuilder());

                    journalStats.getJournalCreationStats().registerSuccessfulEvent(
                            journalCreationWatcher.stop().elapsed(TimeUnit.NANOSECONDS), TimeUnit.NANOSECONDS);

                    //这里就对bc进行了复制，它对应的是一个文件
                    bc = logFile.getBufferedChannel();

                    lastFlushPosition = bc.position();
                }

                //如果qe为null，就从队列中取出写请求
                if (qe == null) {
                    if (dequeueStartTime != 0) {
                        //如果dequeueStartTime!=0，就说明不是第一次进入while循环
                        journalStats.getJournalProcessTimeStats()
                            .registerSuccessfulEvent(MathUtils.elapsedNanos(dequeueStartTime), TimeUnit.NANOSECONDS);
                    }

                    if (numEntriesToFlush == 0) {
                        //如果要flush的为0，那么久取一个出来
                        qe = queue.take();
                        dequeueStartTime = MathUtils.nowInNano();
                        //取出队列减1
                        journalStats.getJournalQueueSize().dec();
                        //这里统计在队列里待了多久
                        journalStats.getJournalQueueStats()
                            .registerSuccessfulEvent(MathUtils.elapsedNanos(qe.enqueueTime), TimeUnit.NANOSECONDS);
                    } else {
                        //maxGroupWaitInNanos这个默认是2秒，也就是说从入队开始算，最多等2秒
                        long pollWaitTimeNanos = maxGroupWaitInNanos
                                - MathUtils.elapsedNanos(toFlush.get(0).enqueueTime);
                        if (flushWhenQueueEmpty || pollWaitTimeNanos < 0) {
                            //如果flushWhenQueueEmpty=true或等待时间没哟了
                            pollWaitTimeNanos = 0;
                        }
                        //这里是poll时间,那这里默认最多可能，等待将近2秒
                        qe = queue.poll(pollWaitTimeNanos, TimeUnit.NANOSECONDS);
                        dequeueStartTime = MathUtils.nowInNano();

                        if (qe != null) {
                            //如果获取到请求了,那么这里减少队列size和更新在队列中带的时间
                            journalStats.getJournalQueueSize().dec();
                            journalStats.getJournalQueueStats()
                                .registerSuccessfulEvent(MathUtils.elapsedNanos(qe.enqueueTime), TimeUnit.NANOSECONDS);
                        }

                        boolean shouldFlush = false;
                        //如果以下三个条件中的任何一个成立，我们应该发出 forceWrite
                        //  1. 如果最旧的待处理条目已待处理的时间超过最大等待时间
                        // We should issue a forceWrite if any of the three conditions below holds good
                        // 1. If the oldest pending entry has been pending for longer than the max wait time
                        if (maxGroupWaitInNanos > 0 && !groupWhenTimeout && (MathUtils
                                .elapsedNanos(toFlush.get(0).enqueueTime) > maxGroupWaitInNanos)) {
                            //maxGroupWaitInNanos默认是2秒，如果设置了并且maxGroupWaitInNanos为false，
                            //并且第一个入队时间距离现在超过了maxGroupWaitInNanos,那么这里就设置groupWhenTimeout为true
                            groupWhenTimeout = true;
                        } else if (maxGroupWaitInNanos > 0 && groupWhenTimeout
                            && (qe == null // no entry to group
                                || MathUtils.elapsedNanos(qe.enqueueTime) < maxGroupWaitInNanos)) {
                            // when group timeout, it would be better to look forward, as there might be lots of
                            // entries already timeout
                            // due to a previous slow write (writing to filesystem which impacted by force write).
                            // Group those entries in the queue
                            // a) already timeout
                            // b) limit the number of entries to group
                            groupWhenTimeout = false;
                            shouldFlush = true;
                            journalStats.getFlushMaxWaitCounter().inc();
                        } else if (qe != null
                                && ((bufferedEntriesThreshold > 0 && toFlush.size() > bufferedEntriesThreshold)
                                || (bc.position() > lastFlushPosition + bufferedWritesThreshold))) {
                            //已经超时了，还需要满足bufferedWritesThreshold条件，才能flush
                            // 2. If we have buffered more than the buffWriteThreshold or bufferedEntriesThreshold
                            groupWhenTimeout = false;
                            shouldFlush = true;
                            journalStats.getFlushMaxOutstandingBytesCounter().inc();
                        } else if (qe == null && flushWhenQueueEmpty) {
                            // We should get here only if we flushWhenQueueEmpty is true else we would wait
                            // for timeout that would put is past the maxWait threshold
                            // 3. If the queue is empty i.e. no benefit of grouping. This happens when we have one
                            // publish at a time - common case in tests.
                            groupWhenTimeout = false;
                            shouldFlush = true;
                            journalStats.getFlushEmptyQueueCounter().inc();
                        }

                        // toFlush is non null and not empty so should be safe to access getFirst
                        if (shouldFlush) {//判断是否应该flush，第一次groupWhenTimeout没有设置shouldFlush=true
                            if (journalFormatVersionToWrite >= JournalChannel.V5) {
                                //如果是V5版本，那么这里写入填充字节
                                writePaddingBytes(logFile, paddingBuff, journalAlignmentSize);
                            }
                            journalFlushWatcher.reset().start();
                            //这里开始flush
                            bc.flush();

                            for (int i = 0; i < toFlush.size(); i++) {
                                QueueEntry entry = toFlush.get(i);
                                //如果syncData=false，或者entry开启了ack在sync之前，那么这里回直接回调
                                if (entry != null && (!syncData || entry.ackBeforeSync)) {
                                    toFlush.set(i, null);
                                    numEntriesToFlush--;
                                    //这里开始触发回调
                                    cbThreadPool.execute(entry);
                                }
                            }

                            lastFlushPosition = bc.position();
                            //这里统计flush耗时
                            journalStats.getJournalFlushStats().registerSuccessfulEvent(
                                    journalFlushWatcher.stop().elapsed(TimeUnit.NANOSECONDS), TimeUnit.NANOSECONDS);

                            // Trace the lifetime of entries through persistence
                            if (LOG.isDebugEnabled()) {
                                for (QueueEntry e : toFlush) {
                                    if (e != null) {
                                        LOG.debug("Written and queuing for flush Ledger: {}  Entry: {}",
                                                  e.ledgerId, e.entryId);
                                    }
                                }
                            }
                            //统计一次flush的entry数
                            journalStats.getForceWriteBatchEntriesStats()
                                .registerSuccessfulValue(numEntriesToFlush);
                            journalStats.getForceWriteBatchBytesStats()
                                .registerSuccessfulValue(batchSize);

                            //这里看看是否要轮转
                            boolean shouldRolloverJournal = (lastFlushPosition > maxJournalSize);
                            //触发自动刷盘，有三种case：
                            //1.开启了syncData数据同步;
                            //2.应该轮转了；
                            //3.达到了flush周期
                            // Trigger data sync to disk in the "Force-Write" thread.
                            // Trigger data sync to disk has three situations:
                            // 1. journalSyncData enabled, usually for SSD used as journal storage
                            // 2. shouldRolloverJournal is true, that is the journal file reaches maxJournalSize
                            // 3. if journalSyncData disabled and shouldRolloverJournal is false, we can use
                            //   journalPageCacheFlushIntervalMSec to control sync frequency, preventing disk
                            //   synchronize frequently, which will increase disk io util.
                            //   when flush interval reaches journalPageCacheFlushIntervalMSec (default: 1s),
                            //   it will trigger data sync to disk
                            if (syncData
                                    || shouldRolloverJournal
                                    || (System.currentTimeMillis() - lastFlushTimeMs
                                    >= journalPageCacheFlushIntervalMSec)) {
                                //这里放入队列
                                //注意这里的ismarker为false
                                //如果是要轮转触发的话，这里传入的shouldClose就为true
                                forceWriteRequests.put(createForceWriteRequest(logFile, logId, lastFlushPosition,
                                        toFlush, shouldRolloverJournal, false));
                                lastFlushTimeMs = System.currentTimeMillis();
                            }
                            //这里将变量重置
                            toFlush = entryListRecycler.newInstance();
                            numEntriesToFlush = 0;

                            batchSize = 0L;
                            // check whether journal file is over file limit
                            if (shouldRolloverJournal) {
                                // if the journal file is rolled over, the journal file will be closed after last
                                // entry is force written to disk.
                                logFile = null;
                                continue;
                            }
                        }
                    }
                }

                // 如果不是在运行，那么这里回break
                if (!running) {
                    LOG.info("Journal Manager is asked to shut down, quit.");
                    break;
                }

                //如果qe为null就继续下一个
                if (qe == null) { // no more queue entry
                    continue;
                }
                if ((qe.entryId == Bookie.METAENTRY_ID_LEDGER_EXPLICITLAC)
                        && (journalFormatVersionToWrite < JournalChannel.V6)) {
                    //这意味着我们正在使用支持持久显式Lac 的新代码，但 journalFormatVersionToWrite 设置为某个较旧的值（< V6）。
                    //在这种情况下，我们不应将此特殊条目 (METAENTRY_ID_LEDGER_EXPLICITLAC) 写入 Journal。
                    /*
                     * this means we are using new code which supports
                     * persisting explicitLac, but "journalFormatVersionToWrite"
                     * is set to some older value (< V6). In this case we
                     * shouldn't write this special entry
                     * (METAENTRY_ID_LEDGER_EXPLICITLAC) to Journal.
                     */
                    memoryLimitController.releaseMemory(qe.entry.readableBytes());
                    qe.entry.release();
                } else if (qe.entryId != Bookie.METAENTRY_ID_FORCE_LEDGER) {
                    //如果不是强制force ledger的entry id
                    int entrySize = qe.entry.readableBytes();
                    //统计写journal的bytes 速率
                    journalStats.getJournalWriteBytes().add(entrySize);
                    // 攒batch的大小batchSize，加4是因为前面四个字节用来存放entrySize大小
                    batchSize += (4 + entrySize);

                    lenBuff.clear();
                    lenBuff.writeInt(entrySize);

                    //如果需要的话，预先分配大小
                    // preAlloc based on size
                    logFile.preAllocIfNeeded(4 + entrySize);

                    //然后这里写到文件里
                    //先写entrysize，再写entry
                    bc.write(lenBuff);
                    bc.write(qe.entry);
                    //最后释放掉内存
                    memoryLimitController.releaseMemory(qe.entry.readableBytes());
                    qe.entry.release();
                }

                //添加到要flush的列表中
                toFlush.add(qe);
                //flush数加1
                numEntriesToFlush++;
                //qe设置未null
                qe = null;
            }
        } catch (IOException ioe) {
            LOG.error("I/O exception in Journal thread!", ioe);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            LOG.info("Journal exits when shutting down");
        } finally {
            // There could be packets queued for forceWrite on this logFile
            // That is fine as this exception is going to anyway take down the
            // the bookie. If we execute this as a part of graceful shutdown,
            // close will flush the file system cache making any previous
            // cached writes durable so this is fine as well.
            IOUtils.close(LOG, bc);
        }
        LOG.info("Journal exited loop!");
    }

    public BufferedChannelBuilder getBufferedChannelBuilder() {
        return (FileChannel fc, int capacity) -> new BufferedChannel(allocator, fc, capacity);
    }

    /**
     * Shuts down the journal.
     */
    public synchronized void shutdown() {
        try {
            if (!running) {
                return;
            }
            LOG.info("Shutting down Journal");
            forceWriteThread.shutdown();
            cbThreadPool.shutdown();
            if (!cbThreadPool.awaitTermination(5, TimeUnit.SECONDS)) {
                LOG.warn("Couldn't shutdown journal callback thread gracefully. Forcing");
            }
            cbThreadPool.shutdownNow();

            running = false;
            this.interrupt();
            this.join();
            LOG.info("Finished Shutting down Journal thread");
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            LOG.warn("Interrupted during shutting down journal : ", ie);
        }
    }

    private static int fullRead(JournalChannel fc, ByteBuffer bb) throws IOException {
        int total = 0;
        while (bb.remaining() > 0) {
            int rc = fc.read(bb);
            if (rc <= 0) {
                return total;
            }
            total += rc;
        }
        return total;
    }

    //
    /**
     * Wait for the Journal thread to exit.
     * This is method is needed in order to mock the journal, we can't mock final method of java.lang.Thread class
     *
     * @throws InterruptedException
     */
    @VisibleForTesting
    public void joinThread() throws InterruptedException {
        join();
    }

    long getMemoryUsage() {
        return memoryLimitController.currentUsage();
    }
}
