/**
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
package org.apache.bookkeeper.replication;

import static org.apache.bookkeeper.replication.ReplicationStats.NUM_DEFER_LEDGER_LOCK_RELEASE_OF_FAILED_LEDGER;
import static org.apache.bookkeeper.replication.ReplicationStats.NUM_ENTRIES_UNABLE_TO_READ_FOR_REPLICATION;
import static org.apache.bookkeeper.replication.ReplicationStats.NUM_FULL_OR_PARTIAL_LEDGERS_REPLICATED;
import static org.apache.bookkeeper.replication.ReplicationStats.REPLICATE_EXCEPTION;
import static org.apache.bookkeeper.replication.ReplicationStats.REPLICATION_WORKER_SCOPE;
import static org.apache.bookkeeper.replication.ReplicationStats.REREPLICATE_OP;

import com.google.common.base.Stopwatch;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.SortedMap;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

import org.apache.bookkeeper.bookie.BookieThread;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BKException.BKNoSuchLedgerExistsOnMetadataServerException;
import org.apache.bookkeeper.client.BKException.BKNotEnoughBookiesException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.BookKeeperAdmin;
import org.apache.bookkeeper.client.LedgerChecker;
import org.apache.bookkeeper.client.LedgerFragment;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.meta.AbstractZkLedgerManagerFactory;
import org.apache.bookkeeper.meta.LedgerManagerFactory;
import org.apache.bookkeeper.meta.LedgerUnderreplicationManager;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.GenericCallback;
import org.apache.bookkeeper.replication.ReplicationException.CompatibilityException;
import org.apache.bookkeeper.replication.ReplicationException.UnavailableException;
import org.apache.bookkeeper.stats.Counter;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.stats.annotations.StatsDoc;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ReplicationWorker will take the fragments one by one from
 * ZKLedgerUnderreplicationManager and replicates to it.
 */
@StatsDoc(
    name = REPLICATION_WORKER_SCOPE,
    help = "replication worker related stats"
)
public class ReplicationWorker implements Runnable {
    private static final Logger LOG = LoggerFactory
            .getLogger(ReplicationWorker.class);
    private static final int REPLICATED_FAILED_LEDGERS_MAXSIZE = 2000;
    public static final int NUM_OF_EXPONENTIAL_BACKOFF_RETRIALS = 5;

    private final LedgerUnderreplicationManager underreplicationManager;
    private final ServerConfiguration conf;
    private volatile boolean workerRunning = false;
    private final BookKeeperAdmin admin;
    private final LedgerChecker ledgerChecker;
    private final BookKeeper bkc;
    private final boolean ownBkc;
    private final Thread workerThread;
    private final long rwRereplicateBackoffMs;
    private final long openLedgerRereplicationGracePeriod;
    private final Timer pendingReplicationTimer;
    private final long lockReleaseOfFailedLedgerGracePeriod;
    private final long baseBackoffForLockReleaseOfFailedLedger;
    private final BiConsumer<Long, Long> onReadEntryFailureCallback;

    // Expose Stats
    private final StatsLogger statsLogger;
    @StatsDoc(
        name = REPLICATE_EXCEPTION,
        help = "replication related exceptions"
    )
    private final StatsLogger exceptionLogger;
    @StatsDoc(
        name = REREPLICATE_OP,
        help = "operation stats of re-replicating ledgers"
    )
    private final OpStatsLogger rereplicateOpStats;
    @StatsDoc(
        name = NUM_FULL_OR_PARTIAL_LEDGERS_REPLICATED,
        help = "the number of ledgers re-replicated"
    )
    private final Counter numLedgersReplicated;
    @StatsDoc(
        name = NUM_DEFER_LEDGER_LOCK_RELEASE_OF_FAILED_LEDGER,
        help = "the number of defer-ledger-lock-releases of failed ledgers"
    )
    private final Counter numDeferLedgerLockReleaseOfFailedLedger;
    @StatsDoc(
            name = NUM_ENTRIES_UNABLE_TO_READ_FOR_REPLICATION,
            help = "the number of entries ReplicationWorker unable to read"
        )
    private final Counter numEntriesUnableToReadForReplication;
    private final Map<String, Counter> exceptionCounters;
    final LoadingCache<Long, AtomicInteger> replicationFailedLedgers;
    final LoadingCache<Long, ConcurrentSkipListSet<Long>> unableToReadEntriesForReplication;

    /**
     * Replication worker for replicating the ledger fragments from
     * UnderReplicationManager to the targetBookie. This target bookie will be a
     * local bookie.
     *
     * @param conf
     *            - configurations
     */
    public ReplicationWorker(final ServerConfiguration conf)
            throws CompatibilityException, KeeperException,
            InterruptedException, IOException {
        this(conf, NullStatsLogger.INSTANCE);
    }

    /**
     * Replication worker for replicating the ledger fragments from
     * UnderReplicationManager to the targetBookie. This target bookie will be a
     * local bookie.
     *
     * @param conf
     *            - configurations
     * @param statsLogger
     *            - stats logger
     */
    public ReplicationWorker(final ServerConfiguration conf,
                             StatsLogger statsLogger)
            throws CompatibilityException, KeeperException,

            InterruptedException, IOException {
        this(conf, Auditor.createBookKeeperClient(conf), true, statsLogger);
    }

    ReplicationWorker(final ServerConfiguration conf,
                      BookKeeper bkc,
                      boolean ownBkc,
                      StatsLogger statsLogger)
            throws CompatibilityException, KeeperException,
            InterruptedException, IOException {
        this.conf = conf;
        this.bkc = bkc;
        this.ownBkc = ownBkc;
        LedgerManagerFactory mFactory = AbstractZkLedgerManagerFactory
                .newLedgerManagerFactory(
                    this.conf,
                    bkc.getMetadataClientDriver().getLayoutManager());
        this.underreplicationManager = mFactory
                .newLedgerUnderreplicationManager();
        this.admin = new BookKeeperAdmin(bkc, statsLogger);
        this.ledgerChecker = new LedgerChecker(bkc);
        this.workerThread = new BookieThread(this, "ReplicationWorker");
        this.openLedgerRereplicationGracePeriod = conf
                .getOpenLedgerRereplicationGracePeriod();
        this.lockReleaseOfFailedLedgerGracePeriod = conf.getLockReleaseOfFailedLedgerGracePeriod();
        this.baseBackoffForLockReleaseOfFailedLedger = this.lockReleaseOfFailedLedgerGracePeriod
                / (long) (Math.pow(2, NUM_OF_EXPONENTIAL_BACKOFF_RETRIALS));
        this.rwRereplicateBackoffMs = conf.getRwRereplicateBackoffMs();
        this.pendingReplicationTimer = new Timer("PendingReplicationTimer");
        this.replicationFailedLedgers = CacheBuilder.newBuilder().maximumSize(REPLICATED_FAILED_LEDGERS_MAXSIZE)
                .build(new CacheLoader<Long, AtomicInteger>() {
                    @Override
                    public AtomicInteger load(Long key) throws Exception {
                        return new AtomicInteger();
                    }
                });
        this.unableToReadEntriesForReplication = CacheBuilder.newBuilder()
                .maximumSize(REPLICATED_FAILED_LEDGERS_MAXSIZE)
                .build(new CacheLoader<Long, ConcurrentSkipListSet<Long>>() {
                    @Override
                    public ConcurrentSkipListSet<Long> load(Long key) throws Exception {
                        return new ConcurrentSkipListSet<Long>();
                    }
                });

        // Expose Stats
        this.statsLogger = statsLogger;
        this.exceptionLogger = statsLogger.scope(REPLICATE_EXCEPTION);
        this.rereplicateOpStats = this.statsLogger.getOpStatsLogger(REREPLICATE_OP);
        this.numLedgersReplicated = this.statsLogger.getCounter(NUM_FULL_OR_PARTIAL_LEDGERS_REPLICATED);
        this.numDeferLedgerLockReleaseOfFailedLedger = this.statsLogger
                .getCounter(NUM_DEFER_LEDGER_LOCK_RELEASE_OF_FAILED_LEDGER);
        this.numEntriesUnableToReadForReplication = this.statsLogger
                .getCounter(NUM_ENTRIES_UNABLE_TO_READ_FOR_REPLICATION);
        this.exceptionCounters = new HashMap<String, Counter>();
        this.onReadEntryFailureCallback = (ledgerid, entryid) -> {
            numEntriesUnableToReadForReplication.inc();
            unableToReadEntriesForReplication.getUnchecked(ledgerid).add(entryid);
        };
    }

    /**
     * Start the replication worker.
     */
    public void start() {
        this.workerThread.start();
    }

    @Override
    public void run() {
        //是否正在运行
        workerRunning = true;
        while (workerRunning) {
            try {
                //开始复制
                rereplicate();
            } catch (InterruptedException e) {
                //被中断了
                LOG.info("InterruptedException "
                        + "while replicating fragments", e);
                //被中断的话，就不retry back了，直接shutdown
                shutdown();
                Thread.currentThread().interrupt();
                return;
            } catch (BKException e) {
                //复制的时候异常了
                LOG.error("BKException while replicating fragments", e);
                //retry back ,默认rwRereplicateBackoffMs=5秒
                waitBackOffTime(rwRereplicateBackoffMs);
            } catch (UnavailableException e) {
                //不可获得异常
                LOG.error("UnavailableException "
                        + "while replicating fragments", e);
                //retry back ,默认rwRereplicateBackoffMs=5秒
                waitBackOffTime(rwRereplicateBackoffMs);
            }
        }

        //退出日志
        LOG.info("ReplicationWorker exited loop!");
    }

    private static void waitBackOffTime(long backoffMs) {
        try {
            Thread.sleep(backoffMs);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    // 将复制不足的片段从失败的bookie分类账复制到targetBookie
    // TODO-chenlin: rereplicate 命名感觉怪怪的， 改为replicate不行吗
    /**
     * Replicates the under replicated fragments from failed bookie ledger to
     * targetBookie.
     */
    private void rereplicate() throws InterruptedException, BKException,
            UnavailableException {
        // 获取under replica的副本,这个实际就是在underreplica目录下去模式匹配对应的leaderId，并且判断是否是自己负责的
        long ledgerIdToReplicate = underreplicationManager
                .getLedgerToRereplicate();

        // 计算经过的时间工具类
        Stopwatch stopwatch = Stopwatch.createStarted();
        boolean success = false;
        try {
            // 开始复制
            success = rereplicate(ledgerIdToReplicate);
        } finally {
            // 统计耗时
            long latencyMillis = stopwatch.stop().elapsed(TimeUnit.MILLISECONDS);
            if (success) {
                // 如果成功就统计成功耗时
                rereplicateOpStats.registerSuccessfulEvent(latencyMillis, TimeUnit.MILLISECONDS);
            } else {
                // 统计失败耗时
                rereplicateOpStats.registerFailedEvent(latencyMillis, TimeUnit.MILLISECONDS);
            }
        }
    }

    private void logBKExceptionAndReleaseLedger(BKException e, long ledgerIdToReplicate)
        throws UnavailableException {
        LOG.info("{} while"
                + " rereplicating ledger {}."
                + " Enough Bookies might not have available"
                + " So, no harm to continue",
            e.getClass().getSimpleName(),
            ledgerIdToReplicate);
        underreplicationManager
            .releaseUnderreplicatedLedger(ledgerIdToReplicate);
        getExceptionCounter(e.getClass().getSimpleName()).inc();
    }

    //如果该ledgerFragment中没有不可读的entry的话，就直接返回true，否则返回false
    private boolean tryReadingFaultyEntries(LedgerHandle lh, LedgerFragment ledgerFragment) {
        //获取ledgerId
        long ledgerId = lh.getId();
        //这个ledger不能读的entries
        ConcurrentSkipListSet<Long> entriesUnableToReadForThisLedger = unableToReadEntriesForReplication
                .getIfPresent(ledgerId);
        //如果entriesUnableToReadForThisLedger等于null，那么久返回true
        if (entriesUnableToReadForThisLedger == null) {
            return true;
        }
        // 该fragment中第一个entryId
        long firstEntryIdOfFragment = ledgerFragment.getFirstEntryId();
        // 该fragment中最后一个entryId
        long lastEntryIdOfFragment = ledgerFragment.getLastKnownEntryId();
        // 从entriesUnableToReadForThisLedger中去获取，不能读的entry消息
        NavigableSet<Long> entriesOfThisFragmentUnableToRead = entriesUnableToReadForThisLedger
                .subSet(firstEntryIdOfFragment, true, lastEntryIdOfFragment, true);
        // 如果为空，那么就返回true
        if (entriesOfThisFragmentUnableToRead.isEmpty()) {
            return true;
        }
        final CountDownLatch multiReadComplete = new CountDownLatch(1);
        final AtomicInteger numOfResponsesToWaitFor = new AtomicInteger(entriesOfThisFragmentUnableToRead.size());
        final AtomicInteger returnRCValue = new AtomicInteger(BKException.Code.OK);
        //遍历该ledger中，不可读的entryId
        for (long entryIdToRead : entriesOfThisFragmentUnableToRead) {
            if (multiReadComplete.getCount() == 0) {
                // 如果 asyncRead 请求已经失败，则中断循环。
                /*
                 * if an asyncRead request had already failed then break the
                 * loop.
                 */
                break;
            }
            //尝试着读该entry
            lh.asyncReadEntries(entryIdToRead, entryIdToRead, (rc, ledHan, seq, ctx) -> {
                long thisEntryId = (Long) ctx;
                if (rc == BKException.Code.OK) {//如果读成功了，那么这里multiReadComplete.countDown
                    entriesUnableToReadForThisLedger.remove(thisEntryId);
                    if (numOfResponsesToWaitFor.decrementAndGet() == 0) {
                        multiReadComplete.countDown();
                    }
                } else {
                    //否则就读失败了
                    LOG.error("Received error: {} while trying to read entry: {} of ledger: {} in ReplicationWorker",
                            rc, entryIdToRead, ledgerId);
                    returnRCValue.compareAndSet(BKException.Code.OK, rc);
                    /*
                     * on receiving a failure error response, multiRead can be
                     * marked completed, since there is not need to wait for
                     * other responses.
                     */
                    multiReadComplete.countDown();
                }
            }, entryIdToRead);
        }
        //等着完成，因为asyncReadEntries是异步的
        try {
            multiReadComplete.await();
        } catch (InterruptedException e) {
            LOG.error("Got interrupted exception while trying to read entries", e);
            Thread.currentThread().interrupt();  // set interrupt flag
            return false;
        }
        //判断读取结果是否正常
        return (returnRCValue.get() == BKException.Code.OK);
    }

    private boolean rereplicate(long ledgerIdToReplicate) throws InterruptedException, BKException,
            UnavailableException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Going to replicate the fragments of the ledger: {}", ledgerIdToReplicate);
        }

        //是否推迟分类帐锁定释放？
        boolean deferLedgerLockRelease = false;

        //打开一个leadeger
        try (LedgerHandle lh = admin.openLedgerNoRecovery(ledgerIdToReplicate)) {
            //获取掉队的fragment,auditorLedgerVerificationPercentage是百分比，判断一个fragment需要被修复的百分比，默认是0
            Set<LedgerFragment> fragments =
                getUnderreplicatedFragments(lh, conf.getAuditorLedgerVerificationPercentage());

            if (LOG.isDebugEnabled()) {
                LOG.debug("Founds fragments {} for replication from ledger: {}", fragments, ledgerIdToReplicate);
            }

            //然后下面就开始遍历修复
            boolean foundOpenFragments = false;
            for (LedgerFragment ledgerFragment : fragments) {
                if (!ledgerFragment.isClosed()) {
                    //如果有某个fragment被打开着，那么就需要标记下
                    foundOpenFragments = true;
                    continue;
                }
                //尝试着读下副本，如果失败，那么就放弃复制该ledger,现在看来这个方法并没有起到过滤的作用
                if (!tryReadingFaultyEntries(lh, ledgerFragment)) {
                    LOG.error("Failed to read faulty entries, so giving up replicating ledgerFragment {}",
                            ledgerFragment);
                    continue;
                }
                try {
                    //开始复制,lh是对应leadger的处理流方法，ledgerFragment是要处理的ledgerFragment
                    admin.replicateLedgerFragment(lh, ledgerFragment, onReadEntryFailureCallback);
                } catch (BKException.BKBookieHandleNotAvailableException e) {
                    LOG.warn("BKBookieHandleNotAvailableException while replicating the fragment", e);
                } catch (BKException.BKLedgerRecoveryException e) {
                    LOG.warn("BKLedgerRecoveryException while replicating the fragment", e);
                } catch (BKException.BKNotEnoughBookiesException e) {
                    //在这里疯狂刷
                    LOG.warn("BKNotEnoughBookiesException while replicating the fragment", e);
                }
            }
            //如果该leadger有未关闭的fragment，或者有bookie丢失，那么久会释放锁，并返回false
            if (foundOpenFragments || isLastSegmentOpenAndMissingBookies(lh)) {
                deferLedgerLockRelease = true;
                //释放锁
                deferLedgerLockRelease(ledgerIdToReplicate);
                return false;
            }

            //获取掉队的fragment
            fragments = getUnderreplicatedFragments(lh, conf.getAuditorLedgerVerificationPercentage());
            if (fragments.size() == 0) {
                //如果掉队的fragment没有了，那么久说明复制成功了，标记删除
                LOG.info("Ledger replicated successfully. ledger id is: " + ledgerIdToReplicate);
                underreplicationManager.markLedgerReplicated(ledgerIdToReplicate);
                return true;
            } else {
                deferLedgerLockRelease = true;
                //否则释放锁
                deferLedgerLockReleaseOfFailedLedger(ledgerIdToReplicate);
                numDeferLedgerLockReleaseOfFailedLedger.inc();
                // 释放 underReplication 账本锁并再次竞争复制以获取挂起的碎片
                // Releasing the underReplication ledger lock and compete
                // for the replication again for the pending fragments
                return false;
            }

        } catch (BKNoSuchLedgerExistsOnMetadataServerException e) {
            // Ledger might have been deleted by user
            LOG.info("BKNoSuchLedgerExistsOnMetadataServerException while opening "
                + "ledger {} for replication. Other clients "
                + "might have deleted the ledger. "
                + "So, no harm to continue", ledgerIdToReplicate);
            underreplicationManager.markLedgerReplicated(ledgerIdToReplicate);
            getExceptionCounter("BKNoSuchLedgerExistsOnMetadataServerException").inc();
            return false;
        } catch (BKNotEnoughBookiesException e) {
            logBKExceptionAndReleaseLedger(e, ledgerIdToReplicate);
            throw e;
        } catch (BKException e) {
            logBKExceptionAndReleaseLedger(e, ledgerIdToReplicate);
            return false;
        } finally {
            // 我们确保我们总是释放复制不足的锁，除非我们决定推迟它。 如果锁已经被释放，这是一个空操作
            // we make sure we always release the underreplicated lock, unless we decided to defer it. If the lock has
            // already been released, this is a no-op
            if (!deferLedgerLockRelease) {
                try {
                    underreplicationManager.releaseUnderreplicatedLedger(ledgerIdToReplicate);
                } catch (UnavailableException e) {
                    LOG.error("UnavailableException while releasing the underreplicated lock for ledger {}:",
                        ledgerIdToReplicate, e);
                    shutdown();
                }
            }
        }
    }


    /**
     * 在检查分类账的碎片时，有一个角落案例
     * 如果最后一个段/集合是打开的，但集合中的某些法定人数没有写入任何内容，则博彩公司可能会失败而不采取任何行动。这很好，直到足够多的博彩公司未能导致法定人数变得不可用，届时分类帐将无法恢复。
     * 例如，如果在 E3Q2 中，只写入了 1 个条目，并且集成中的最后一个 bookie 失败，则没有写入任何内容，因此不需要恢复任何内容。但是，如果倒数第二个博彩公司失败，
     * 我们现在已经失去了第二个条目的法定人数，因此无法查看第二个条目是否已写入。 为了避免这种情况，我们需要检查最终开放集合中的博彩公司是否不可用，如果是，则采取行动。要采取的行动是关闭分类帐，
     * 在宽限期之后，写作客户可能会自行更换有问题的博彩公司。
     * 在封闭的分类账中缺少博彩公司是可以的，因为我们知道最后确认的添加，所以我们可以判断哪些条目应该存在，并在必要时重新复制它们。
     *
     * When checking the fragments of a ledger, there is a corner case
     * where if the last segment/ensemble is open, but nothing has been written to
     * some of the quorums in the ensemble, bookies can fail without any action being
     * taken. This is fine, until enough bookies fail to cause a quorum to become
     * unavailable, by which time the ledger is unrecoverable.
     *
     * <p>For example, if in a E3Q2, only 1 entry is written and the last bookie
     * in the ensemble fails, nothing has been written to it, so nothing needs to be
     * recovered. But if the second to last bookie fails, we've now lost quorum for
     * the second entry, so it's impossible to see if the second has been written or
     * not.
     *
     * <p>To avoid this situation, we need to check if bookies in the final open ensemble
     * are unavailable, and take action if so. The action to take is to close the ledger,
     * after a grace period as the writting client may replace the faulty bookie on its
     * own.
     *
     * <p>Missing bookies in closed ledgers are fine, as we know the last confirmed add, so
     * we can tell which entries are supposed to exist and rereplicate them if necessary.
     */
    private boolean isLastSegmentOpenAndMissingBookies(LedgerHandle lh) throws BKException {
        //如果leadger已经关闭，那么久返回false
        LedgerMetadata md = admin.getLedgerMetadata(lh);
        if (md.isClosed()) {
            return false;
        }

        //获取该ledger的ensemble
        SortedMap<Long, ? extends List<BookieId>> ensembles = admin.getLedgerMetadata(lh).getAllEnsembles();
        //获取最后的key所在的ensemble
        List<BookieId> finalEnsemble = ensembles.get(ensembles.lastKey());
        //获取可获得的bookie
        Collection<BookieId> available = admin.getAvailableBookies();
        for (BookieId b : finalEnsemble) {
            if (!available.contains(b)) {
                //如果不包含，那么就说明是miss了
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Bookie {} is missing from the list of Available Bookies. ledger {}:ensemble {}.",
                            b, lh.getId(), finalEnsemble);
                }
                return true;
            }
        }
        return false;
    }

    /**
     * Gets the under replicated fragments.
     */
    private Set<LedgerFragment> getUnderreplicatedFragments(LedgerHandle lh, Long ledgerVerificationPercentage)
            throws InterruptedException {
        CheckerCallback checkerCb = new CheckerCallback();
        ledgerChecker.checkLedger(lh, checkerCb, ledgerVerificationPercentage);
        Set<LedgerFragment> fragments = checkerCb.waitAndGetResult();
        return fragments;
    }

    void scheduleTaskWithDelay(TimerTask timerTask, long delayPeriod) {
        pendingReplicationTimer.schedule(timerTask, delayPeriod);
    }

    /**
     * Schedules a timer task for releasing the lock which will be scheduled
     * after open ledger fragment replication time. Ledger will be fenced if it
     * is still in open state when timer task fired.
     */
    private void deferLedgerLockRelease(final long ledgerId) {
        long gracePeriod = this.openLedgerRereplicationGracePeriod;
        TimerTask timerTask = new TimerTask() {
            @Override
            public void run() {
                boolean isRecoveryOpen = false;
                LedgerHandle lh = null;
                try {
                    lh = admin.openLedgerNoRecovery(ledgerId);
                    if (isLastSegmentOpenAndMissingBookies(lh)) {
                        // Need recovery open, close the old ledger handle.
                        lh.close();
                        // Recovery open could result in client write failure.
                        LOG.warn("Missing bookie(s) from last segment. Opening Ledger{} for Recovery.", ledgerId);
                        lh = admin.openLedger(ledgerId);
                        isRecoveryOpen = true;
                    }
                    if (!isRecoveryOpen){
                        Set<LedgerFragment> fragments =
                            getUnderreplicatedFragments(lh, conf.getAuditorLedgerVerificationPercentage());
                        for (LedgerFragment fragment : fragments) {
                            if (!fragment.isClosed()) {
                                // Need recovery open, close the old ledger handle.
                                lh.close();
                                // Recovery open could result in client write failure.
                                LOG.warn("Open Fragment{}. Opening Ledger{} for Recovery.",
                                        fragment.getEnsemble(), ledgerId);
                                lh = admin.openLedger(ledgerId);
                                isRecoveryOpen = true;
                                break;
                            }
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    LOG.info("InterruptedException while fencing the ledger {}"
                            + " for rereplication of postponed ledgers", ledgerId, e);
                } catch (BKNoSuchLedgerExistsOnMetadataServerException bknsle) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Ledger {} was deleted, safe to continue", ledgerId, bknsle);
                    }
                } catch (BKException e) {
                    LOG.error("BKException while fencing the ledger {}"
                            + " for rereplication of postponed ledgers", ledgerId, e);
                } finally {
                    try {
                        if (lh != null) {
                            lh.close();
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        LOG.info("InterruptedException while closing ledger {}", ledgerId, e);
                    } catch (BKException e) {
                        // Lets go ahead and release the lock. Catch actual
                        // exception in normal replication flow and take
                        // action.
                        LOG.warn("BKException while closing ledger {} ", ledgerId, e);
                    } finally {
                        try {
                            underreplicationManager
                                    .releaseUnderreplicatedLedger(ledgerId);
                        } catch (UnavailableException e) {
                            LOG.error("UnavailableException while replicating fragments of ledger {}",
                                    ledgerId, e);
                            shutdown();
                        }
                    }
                }
            }
        };
        scheduleTaskWithDelay(timerTask, gracePeriod);
    }

    /**
     * Schedules a timer task for releasing the lock.
     */
    private void deferLedgerLockReleaseOfFailedLedger(final long ledgerId) {
        int numOfTimesFailedSoFar = replicationFailedLedgers.getUnchecked(ledgerId).getAndIncrement();
        /*
         * for the first NUM_OF_EXPONENTIAL_BACKOFF_RETRIALS retrials do
         * exponential backoff, starting from
         * baseBackoffForLockReleaseOfFailedLedger
         */
        long delayOfLedgerLockReleaseInMSecs = (numOfTimesFailedSoFar >= NUM_OF_EXPONENTIAL_BACKOFF_RETRIALS)
                ? this.lockReleaseOfFailedLedgerGracePeriod
                : this.baseBackoffForLockReleaseOfFailedLedger * (int) Math.pow(2, numOfTimesFailedSoFar);
        LOG.error(
                "ReplicationWorker failed to replicate Ledger : {} for {} number of times, "
                + "so deferring the ledger lock release by {} msecs",
                ledgerId, numOfTimesFailedSoFar, delayOfLedgerLockReleaseInMSecs);
        //构建一个释放锁任务
        TimerTask timerTask = new TimerTask() {
            @Override
            public void run() {
                try {
                    underreplicationManager.releaseUnderreplicatedLedger(ledgerId);
                } catch (UnavailableException e) {
                    LOG.error("UnavailableException while replicating fragments of ledger {}", ledgerId, e);
                    shutdown();
                }
            }
        };
        scheduleTaskWithDelay(timerTask, delayOfLedgerLockReleaseInMSecs);
    }

    /**
     * Stop the replication worker service.
     */
    public void shutdown() {
        LOG.info("Shutting down replication worker");

        synchronized (this) {
            if (!workerRunning) {
                return;
            }
            workerRunning = false;
        }
        LOG.info("Shutting down ReplicationWorker");
        this.pendingReplicationTimer.cancel();
        try {
            this.workerThread.interrupt();
            this.workerThread.join();
        } catch (InterruptedException e) {
            LOG.error("Interrupted during shutting down replication worker : ",
                    e);
            Thread.currentThread().interrupt();
        }
        if (ownBkc) {
            try {
                bkc.close();
            } catch (InterruptedException e) {
                LOG.warn("Interrupted while closing the Bookie client", e);
                Thread.currentThread().interrupt();
            } catch (BKException e) {
                LOG.warn("Exception while closing the Bookie client", e);
            }
        }
        try {
            underreplicationManager.close();
        } catch (UnavailableException e) {
            LOG.warn("Exception while closing the "
                    + "ZkLedgerUnderrepliationManager", e);
        }
    }

    /**
     * Gives the running status of ReplicationWorker.
     */
    boolean isRunning() {
        return workerRunning && workerThread.isAlive();
    }

    /**
     * Ledger checker call back.
     */
    private static class CheckerCallback implements
            GenericCallback<Set<LedgerFragment>> {
        private Set<LedgerFragment> result = null;
        private CountDownLatch latch = new CountDownLatch(1);

        @Override
        public void operationComplete(int rc, Set<LedgerFragment> result) {
            this.result = result;
            latch.countDown();
        }

        /**
         * Wait until operation complete call back comes and return the ledger
         * fragments set.
         */
        Set<LedgerFragment> waitAndGetResult() throws InterruptedException {
            latch.await();
            return result;
        }
    }

    private Counter getExceptionCounter(String name) {
        Counter counter = this.exceptionCounters.get(name);
        if (counter == null) {
            counter = this.exceptionLogger.getCounter(name);
            this.exceptionCounters.put(name, counter);
        }
        return counter;
    }

}
