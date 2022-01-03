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
package org.apache.bookkeeper.client;

import io.netty.buffer.ByteBuf;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.bookkeeper.client.BKException.Code;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.proto.BookieClient;
import org.apache.bookkeeper.proto.BookieProtocol;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.GenericCallback;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.ReadEntryCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A utility class to check the complete ledger and finds the UnderReplicated fragments if any.
 *
 * <p>NOTE: This class is tended to be used by this project only. External users should not rely on it directly.
 */
public class LedgerChecker {
    private static final Logger LOG = LoggerFactory.getLogger(LedgerChecker.class);

    public final BookieClient bookieClient;
    public final BookieWatcher bookieWatcher;

    static class InvalidFragmentException extends Exception {
        private static final long serialVersionUID = 1467201276417062353L;
    }

    /**
     * This will collect all the entry read call backs and finally it will give
     * call back to previous call back API which is waiting for it once it meets
     * the expected call backs from down.
     */
    private static class ReadManyEntriesCallback implements ReadEntryCallback {
        AtomicBoolean completed = new AtomicBoolean(false);
        final AtomicLong numEntries;
        final LedgerFragment fragment;
        final GenericCallback<LedgerFragment> cb;

        ReadManyEntriesCallback(long numEntries, LedgerFragment fragment,
                GenericCallback<LedgerFragment> cb) {
            //第一个是entry数
            this.numEntries = new AtomicLong(numEntries);
            //对应fragment
            this.fragment = fragment;
            //回调：ReadManyEntriesCallback->LedgerFragmentCallback->FullLedgerCallback->ProcessLostFragmentsCb-> MultiCallback ->final callback
            this.cb = cb;
        }

        @Override
        public void readEntryComplete(int rc, long ledgerId, long entryId,
                ByteBuf buffer, Object ctx) {
            if (rc == BKException.Code.OK) {
                if (numEntries.decrementAndGet() == 0
                        && !completed.getAndSet(true)) {
                    //如果没有异常，那么久需要该fragment中的所有entry都每一次才行.也就是要等到numEntries.decrementAndGet() == 0
                    cb.operationComplete(rc, fragment);
                }
            } else if (!completed.getAndSet(true)) {
                //只要有一个entry读取有异常，那么久直接调LedgerFragmentCallback.operationComplete
                cb.operationComplete(rc, fragment);
            }
        }
    }

    /**
     * 对应的回调函数，这里就嵌套了4层了：LedgerFragmentCallback->FullLedgerCallback->ProcessLostFragmentsCb-> MultiCallback ->final callback
     * This will collect the bad bookies inside a ledger fragment.
     */
    private static class LedgerFragmentCallback implements GenericCallback<LedgerFragment> {

        private final LedgerFragment fragment;
        private final int bookieIndex;
        // bookie index -> return code
        private final Map<Integer, Integer> badBookies;
        private final AtomicInteger numBookies;
        private final GenericCallback<LedgerFragment> cb;

        LedgerFragmentCallback(LedgerFragment lf,
                               int bookieIndex,
                               GenericCallback<LedgerFragment> cb,
                               Map<Integer, Integer> badBookies,
                               AtomicInteger numBookies) {
            //对应副本
            this.fragment = lf;
            //该副本所在bookie id
            this.bookieIndex = bookieIndex;
            //回调函数
            //对应的回调函数，这里就嵌套了4层了：LedgerFragmentCallback->FullLedgerCallback->ProcessLostFragmentsCb-> MultiCallback ->final callback
            this.cb = cb;
            //专门收集该副本对应的坏的bookie
            this.badBookies = badBookies;
            //副本数
            this.numBookies = numBookies;
        }

        @Override
        public void operationComplete(int rc, LedgerFragment lf) {
            if (BKException.Code.OK != rc) {
                //只要异常类型不为OK，那么久把该bookie放入badBookies中
                synchronized (badBookies) {
                    badBookies.put(bookieIndex, rc);
                }
            }
            if (numBookies.decrementAndGet() == 0) {
                //如果完成的次数等于副本数，那么就走到这里了
                if (badBookies.isEmpty()) {
                    //如果没有损坏的bookie副本，那么久传入OK，表示不进行标记
                    cb.operationComplete(BKException.Code.OK, fragment);
                } else {
                    int rcToReturn = BKException.Code.NoBookieAvailableException;
                    //否则就遍历每个bad bookie，看他们的错误码，检查是否有ClientClosedException类型的错误码，如果有的话，就中断遍历，
                    //并且在FullLedgerCallback回调中，针对NoBookieAvailableException类型的异常是不会进行标记的
                    for (Map.Entry<Integer, Integer> entry : badBookies.entrySet()) {
                        rcToReturn = entry.getValue();
                        if (entry.getValue() == BKException.Code.ClientClosedException) {
                            break;
                        }
                    }
                    // fragment.subset(badBookies.keySet())会将包含bad bookie的fragment返回，也就是说只会传入包含bad bookie的fragment，其他不会传入进行标记
                    //这里传入的错误码rcToReturn就以遍历最后的那个entry对应的错误码为准
                    cb.operationComplete(rcToReturn,
                            fragment.subset(badBookies.keySet()));
                }
            }
        }
    }

    public LedgerChecker(BookKeeper bkc) {
        this(bkc.getBookieClient(), bkc.getBookieWatcher());
    }

    public LedgerChecker(BookieClient client, BookieWatcher watcher) {
        bookieClient = client;
        bookieWatcher = watcher;
    }

    /**
     * Verify a ledger fragment to collect bad bookies.
     *
     * @param fragment
     *          fragment to verify
     * @param cb
     *          callback
     * @throws InvalidFragmentException
     */
    private void verifyLedgerFragment(LedgerFragment fragment,
                                      GenericCallback<LedgerFragment> cb,
                                      Long percentageOfLedgerFragmentToBeVerified)
            throws InvalidFragmentException, BKException {
        // 要检查的bookie集合
        Set<Integer> bookiesToCheck = fragment.getBookiesIndexes();
        if (bookiesToCheck.isEmpty()) {
            //如果bookiesToCheck为空，那么callback传入的错误码就为OK，意思就是这个fragment副本不需要复制恢复
            cb.operationComplete(BKException.Code.OK, fragment);
            return;
        }

        // 副本数
        AtomicInteger numBookies = new AtomicInteger(bookiesToCheck.size());
        Map<Integer, Integer> badBookies = new HashMap<Integer, Integer>();
        for (Integer bookieIndex : bookiesToCheck) {
            //对每个副本单独进行检查
            //对应的回调函数，这里就嵌套了4层了：LedgerFragmentCallback->FullLedgerCallback->ProcessLostFragmentsCb-> MultiCallback ->final callback
            LedgerFragmentCallback lfCb = new LedgerFragmentCallback(
                    fragment, bookieIndex, cb, badBookies, numBookies);
            //这里才是真正校验一个fragment对应在某个bookie上的正常情况的
            verifyLedgerFragment(fragment, bookieIndex, lfCb, percentageOfLedgerFragmentToBeVerified);
        }
    }

    /**
     *
     * 对应的回调函数，这里就嵌套了4层了：LedgerFragmentCallback->FullLedgerCallback->ProcessLostFragmentsCb-> MultiCallback ->final callback
     * Verify a bookie inside a ledger fragment.
     *
     * @param fragment
     *          ledger fragment
     * @param bookieIndex
     *          bookie index in the fragment
     * @param cb
     *          callback
     * @throws InvalidFragmentException
     */
    private void verifyLedgerFragment(LedgerFragment fragment,
                                      int bookieIndex,
                                      GenericCallback<LedgerFragment> cb,
                                      long percentageOfLedgerFragmentToBeVerified)
            throws InvalidFragmentException {
        //获取第一个entry id
        long firstStored = fragment.getFirstStoredEntryId(bookieIndex);
        //最后一个entry id
        long lastStored = fragment.getLastStoredEntryId(bookieIndex);

        //根据bookieIndex获取bookieId
        BookieId bookie = fragment.getAddress(bookieIndex);
        if (null == bookie) {
            //如果没有，那么久直接异常
            throw new InvalidFragmentException();
        }

        if (firstStored == LedgerHandle.INVALID_ENTRY_ID) {
            // this fragment is not on this bookie
            if (lastStored != LedgerHandle.INVALID_ENTRY_ID) {
                //如果第一个entry id为INVALID_ENTRY_ID，最后一个不是，那么久抛异常InvalidFragmentException
                throw new InvalidFragmentException();
            }
            //否则只是第一个entryid为INVALID_ENTRY_ID的话，那么久传入OK错误码,意思就是该bookie不放入bad bookie中，不进行标记对应的fragment
            cb.operationComplete(BKException.Code.OK, fragment);
        } else if (bookieWatcher.isBookieUnavailable(fragment.getAddress(bookieIndex))) {
            //如果该bookie不在当前或者的bookie中，那么错误码传入BookieHandleNotAvailableException，这个会将其放入bad bookie集合中
            // fragment is on this bookie, but already know it's unavailable, so skip the call
            cb.operationComplete(BKException.Code.BookieHandleNotAvailableException, fragment);
        } else if (firstStored == lastStored) {
            //如果第一个和最后一个entry id相等，那么久尝试去读下
            //对应的回调函数，这里就嵌套了5层了：
            //ReadManyEntriesCallback->LedgerFragmentCallback->FullLedgerCallback->ProcessLostFragmentsCb-> MultiCallback ->final callback
            ReadManyEntriesCallback manycb = new ReadManyEntriesCallback(1,
                    fragment, cb);
            //尝试着去读一下
            bookieClient.readEntry(bookie, fragment.getLedgerId(), firstStored,
                                   manycb, null, BookieProtocol.FLAG_NONE);
        } else {
            //如果第一个entry id不等于最后一个entry id，那么久执行下面的逻辑
            if (lastStored <= firstStored) {
                //如果最后一个entry id小于第一个entry id，那么肯定要返回错误异常,这里的回调对应的是LedgerFragmentCallback，该回调只要是异常就会放到bad bookie中
                cb.operationComplete(Code.IncorrectParameterException, null);
                return;
            }

            //用最后一个entry id减去第一个entry id，得到该fragment中有多少entry
            long lengthOfLedgerFragment = lastStored - firstStored + 1;

            //这里会按照比例计算需要校对的entry数，默认percentageOfLedgerFragmentToBeVerified=0，所以numberOfEntriesToBeVerified=0
            //虽然numberOfEntriesToBeVerified为0，但是还是会校验第一个和最后一个entry
            int numberOfEntriesToBeVerified =
                (int) (lengthOfLedgerFragment * (percentageOfLedgerFragmentToBeVerified / 100.0));

            TreeSet<Long> entriesToBeVerified = new TreeSet<Long>();

            if (numberOfEntriesToBeVerified < lengthOfLedgerFragment) {
                //判断要校验的数是否小于lengthOfLedgerFragment,否则的话就全部加到药校验的entriesToBeVerified中
                // Evenly pick random entries over the length of the fragment
                if (numberOfEntriesToBeVerified > 0) {
                    //这里划分桶
                    int lengthOfBucket = (int) (lengthOfLedgerFragment / numberOfEntriesToBeVerified);
                    //相当于随机找出numberOfEntriesToBeVerified个entry来校对
                    for (long index = firstStored;
                         index < (lastStored - lengthOfBucket - 1);
                         index += lengthOfBucket) {
                        long potentialEntryId = ThreadLocalRandom.current().nextInt((lengthOfBucket)) + index;
                        if (fragment.isStoredEntryId(potentialEntryId, bookieIndex)) {
                            entriesToBeVerified.add(potentialEntryId);
                        }
                    }
                }
                //最后再加上第一个和最后一个
                entriesToBeVerified.add(firstStored);
                entriesToBeVerified.add(lastStored);
            } else {
                //如果要校验的entry数量大于等于总entry数，那么久直接全部加到校验集合中
                // Verify the entire fragment
                while (firstStored <= lastStored) {
                    if (fragment.isStoredEntryId(firstStored, bookieIndex)) {
                        entriesToBeVerified.add(firstStored);
                    }
                    firstStored++;
                }
            }
            //回调有5层：
            //ReadManyEntriesCallback->LedgerFragmentCallback->FullLedgerCallback->ProcessLostFragmentsCb-> MultiCallback ->final callback
            ReadManyEntriesCallback manycb = new ReadManyEntriesCallback(entriesToBeVerified.size(),
                    fragment, cb);
            //对于每个entry 都会尝试着读下
            for (Long entryID: entriesToBeVerified) {
                bookieClient.readEntry(bookie, fragment.getLedgerId(), entryID, manycb, null, BookieProtocol.FLAG_NONE);
            }
        }
    }

    /**
     * Callback for checking whether an entry exists or not.
     * It is used to differentiate the cases where it has been written
     * but now cannot be read, and where it never has been written.
     */
    private static class EntryExistsCallback implements ReadEntryCallback {
        AtomicBoolean entryMayExist = new AtomicBoolean(false);
        final AtomicInteger numReads;
        final GenericCallback<Boolean> cb;

        EntryExistsCallback(int numReads,
                            GenericCallback<Boolean> cb) {
            this.numReads = new AtomicInteger(numReads);
            this.cb = cb;
        }

        @Override
        public void readEntryComplete(int rc, long ledgerId, long entryId,
                                      ByteBuf buffer, Object ctx) {
            if (BKException.Code.NoSuchEntryException != rc && BKException.Code.NoSuchLedgerExistsException != rc
                    && BKException.Code.NoSuchLedgerExistsOnMetadataServerException != rc) {
                entryMayExist.set(true);
            }

            if (numReads.decrementAndGet() == 0) {
                cb.operationComplete(rc, entryMayExist.get());
            }
        }
    }

    /**
     * This will collect all the fragment read call backs and finally it will
     * give call back to above call back API which is waiting for it once it
     * meets the expected call backs from down.
     */
    private static class FullLedgerCallback implements
            GenericCallback<LedgerFragment> {
        final Set<LedgerFragment> badFragments;
        final AtomicLong numFragments;
        final GenericCallback<Set<LedgerFragment>> cb;

        FullLedgerCallback(long numFragments,
                GenericCallback<Set<LedgerFragment>> cb) {
            //初始化badFragments,这个集合专门装fragment损坏的副本
            badFragments = new HashSet<LedgerFragment>();
            //这个是所有的副本
            this.numFragments = new AtomicLong(numFragments);
            //对应的回调函数，这里就嵌套了3层了：FullLedgerCallback->ProcessLostFragmentsCb-> MultiCallback ->final callback
            this.cb = cb;
        }

        //对应的回调函数，这里就嵌套了3层了：FullLedgerCallback->ProcessLostFragmentsCb-> MultiCallback ->final callback
        @Override
        public void operationComplete(int rc, LedgerFragment result) {
            if (rc == BKException.Code.ClientClosedException) {
                //如果错误异常是close 异常，那么就不会把这些fragments标记为复制
                cb.operationComplete(BKException.Code.ClientClosedException, badFragments);
                return;
            } else if (rc != BKException.Code.OK) {
                //除此之外的所有异常，都会标记
                badFragments.add(result);
            }
            if (numFragments.decrementAndGet() == 0) {
                //当完成次数等于副本数numFragments时，就调用ProcessLostFragmentsCb回调函数
                cb.operationComplete(BKException.Code.OK, badFragments);
            }
        }
    }

    /**
     * Check that all the fragments in the passed in ledger, and report those
     * which are missing.
     */
    public void checkLedger(final LedgerHandle lh,
                            final GenericCallback<Set<LedgerFragment>> cb) {
        checkLedger(lh, cb, 0L);
    }

    public void checkLedger(final LedgerHandle lh,//lh是该ledger的处理类，提供该类的读写等操作
                            //这里的cb是套了2层：ProcessLostFragmentsCb-> MultiCallback ->final callback
                            final GenericCallback<Set<LedgerFragment>> cb,
                            //percentageOfLedgerFragmentToBeVerified是检查比例，默认是0
                            long percentageOfLedgerFragmentToBeVerified) {
        // 构建一个所有fragment 副本的集合
        // build a set of all fragment replicas
        final Set<LedgerFragment> fragments = new HashSet<LedgerFragment>();

        Long curEntryId = null;
        List<BookieId> curEnsemble = null;
        for (Map.Entry<Long, ? extends List<BookieId>> e : lh
                .getLedgerMetadata().getAllEnsembles().entrySet()) {
            //这里应该是在遍历每个entryId
            if (curEntryId != null) {
                Set<Integer> bookieIndexes = new HashSet<Integer>();
                for (int i = 0; i < curEnsemble.size(); i++) {
                    bookieIndexes.add(i);
                }
                //这不就是一个entry 一个fragment吗
                fragments.add(new LedgerFragment(lh, curEntryId,
                        e.getKey() - 1, bookieIndexes));
            }
            curEntryId = e.getKey();
            curEnsemble = e.getValue();
        }

        /* Checking the last segment of the ledger can be complicated in some cases.
         * In the case that the ledger is closed, we can just check the fragments of
         * the segment as normal even if no data has ever been written to.
         * In the case that the ledger is open, but enough entries have been written,
         * for lastAddConfirmed to be set above the start entry of the segment, we
         * can also check as normal.
         * However, if ledger is open, sometimes lastAddConfirmed cannot be trusted,
         * such as when it's lower than the first entry id, or not set at all,
         * we cannot be sure if there has been data written to the segment.
         * For this reason, we have to send a read request
         * to the bookies which should have the first entry. If they respond with
         * NoSuchEntry we can assume it was never written. If they respond with anything
         * else, we must assume the entry has been written, so we run the check.
         */
        if (curEntryId != null) {//如果curEntryId不为null，那么久说明至少还有一个entry
            //这里是获取最后一个entry id
            long lastEntry = lh.getLastAddConfirmed();
            //如果lh没有密封，并且最后确认的entryId还小于curEntryId，那么久将lastEntry移到curEntryId的位置
            if (!lh.isClosed() && lastEntry < curEntryId) {
                lastEntry = curEntryId;
            }

            // 这里回记录最后一个entry对应的bookie集合
            Set<Integer> bookieIndexes = new HashSet<Integer>();
            for (int i = 0; i < curEnsemble.size(); i++) {
                bookieIndexes.add(i);
            }
            //构建对应的fragment
            final LedgerFragment lastLedgerFragment = new LedgerFragment(lh, curEntryId,
                    lastEntry, bookieIndexes);

            // 检查没有设置最后确认条目的情况
            // Check for the case that no last confirmed entry has been set
            if (curEntryId == lastEntry) {//有个问题，为啥要区分这个条件呢，全部都放入fragments集合，都尝试读下不行吗，这里看起来即便该条件满足，该entry也是需要读一下的呀
                //要读的entryId
                final long entryToRead = curEntryId;

                //对应的回调函数，这里就嵌套了3层了：EntryExistsCallback->ProcessLostFragmentsCb-> MultiCallback ->final callback
                //这个回调是为了判断该entry是否存在(存在判断的条件是，至少存在一个副本，如果副本都不存在，那么久不放入检查fragments集合中)
                final EntryExistsCallback eecb = new EntryExistsCallback(lh.getLedgerMetadata().getWriteQuorumSize(),
                                              new GenericCallback<Boolean>() {
                                                  @Override
                                                  public void operationComplete(int rc, Boolean result) {
                                                      if (result) {
                                                          //如果存在，才放入fragments集合中
                                                          fragments.add(lastLedgerFragment);
                                                      }
                                                      checkFragments(fragments, cb,
                                                          percentageOfLedgerFragmentToBeVerified);
                                                  }
                                              });

                //获取写集合
                DistributionSchedule.WriteSet writeSet = lh.getDistributionSchedule().getWriteSet(entryToRead);
                for (int i = 0; i < writeSet.size(); i++) {
                    //获取bookie
                    BookieId addr = curEnsemble.get(writeSet.get(i));
                    //读取ledger
                    bookieClient.readEntry(addr, lh.getId(), entryToRead,
                                           eecb, null, BookieProtocol.FLAG_NONE);
                }

                writeSet.recycle();
                return;
            } else {
                fragments.add(lastLedgerFragment);
            }
        }
        checkFragments(fragments, cb, percentageOfLedgerFragmentToBeVerified);
    }

    private void checkFragments(Set<LedgerFragment> fragments,
                                GenericCallback<Set<LedgerFragment>> cb,
                                long percentageOfLedgerFragmentToBeVerified) {
        //如果一个副本都没有，那么久直接返回
        if (fragments.size() == 0) { // no fragments to verify
            //传入OK，实际也不会标记任何ledger，因为fragments为空
            cb.operationComplete(BKException.Code.OK, fragments);
            return;
        }

        //对应的回调函数，这里就嵌套了3层了：FullLedgerCallback->ProcessLostFragmentsCb-> MultiCallback ->final callback
        // verify all the collected fragment replicas
        FullLedgerCallback allFragmentsCb = new FullLedgerCallback(fragments
                .size(), cb);
        for (LedgerFragment r : fragments) {
            //遍历每个fragment,然后开始检查
            LOG.debug("Checking fragment {}", r);
            try {
                verifyLedgerFragment(r, allFragmentsCb, percentageOfLedgerFragmentToBeVerified);
            } catch (InvalidFragmentException ife) {
                //遇到这种异常，也会将其放入badFragments集合中，待后面一块进行标记复制
                LOG.error("Invalid fragment found : {}", r);
                allFragmentsCb.operationComplete(
                        BKException.Code.IncorrectParameterException, r);
            } catch (BKException e) {
                //此类异常就不用标记了
                LOG.error("BKException when checking fragment : {}", r, e);
            }
        }
    }

}
