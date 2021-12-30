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
import org.apache.bookkeeper.net.BookieSocketAddress;
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
            this.numEntries = new AtomicLong(numEntries);
            this.fragment = fragment;
            this.cb = cb;
        }

        @Override
        public void readEntryComplete(int rc, long ledgerId, long entryId,
                ByteBuf buffer, Object ctx) {
            if (rc == BKException.Code.OK) {
                //如果读取成功，而且是第一次完成，放置多次调用cb.operationCom..
                if (numEntries.decrementAndGet() == 0
                        && !completed.getAndSet(true)) {
                    //这里是传入OK
                    cb.operationComplete(rc, fragment);
                }
            } else if (!completed.getAndSet(true)) {
                //这里直接传入错误码
                cb.operationComplete(rc, fragment);
            }
        }
    }

    /**
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
            this.fragment = lf;
            this.bookieIndex = bookieIndex;
            this.cb = cb;
            this.badBookies = badBookies;
            this.numBookies = numBookies;
        }

        @Override
        public void operationComplete(int rc, LedgerFragment lf) {
            if (BKException.Code.OK != rc) {
                //如果不是OK的错误码，那么久直接放入badBookies中
                synchronized (badBookies) {
                    badBookies.put(bookieIndex, rc);
                }
            }
            if (numBookies.decrementAndGet() == 0) {
                if (badBookies.isEmpty()) {
                    //如果完成后，bad bookie为空，那么久传OK错误码，表示不复制fragment
                    cb.operationComplete(BKException.Code.OK, fragment);
                } else {
                    //如果有损坏的bookie
                    int rcToReturn = BKException.Code.NoBookieAvailableException;
                    for (Map.Entry<Integer, Integer> entry : badBookies.entrySet()) {
                        rcToReturn = entry.getValue();
                        //遍历每个bookie，看是否有客户端关闭连接的异常，遇到这种异常就不遍历，这样上面的rcToReturn就保留了相应的错误码
                        if (entry.getValue() == BKException.Code.ClientClosedException) {
                            break;
                        }
                    }
                    cb.operationComplete(rcToReturn,
                            //这里是传入包含这些bad bookie的fragment
                            fragment.subset(badBookies.keySet()));
                }
            }
        }
    }

    public LedgerChecker(BookKeeper bkc) {
        bookieClient = bkc.getBookieClient();
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
        Set<Integer> bookiesToCheck = fragment.getBookiesIndexes();
        //bookiesToCheck是空就直接返回
        if (bookiesToCheck.isEmpty()) {
            //注意这里传入的是OK，所以不会把该fragment放入bad中
            cb.operationComplete(BKException.Code.OK, fragment);
            return;
        }

        //这里会遍历每个副本，然后都去检验下
        AtomicInteger numBookies = new AtomicInteger(bookiesToCheck.size());
        //bad  bookie用来收集损坏的bookie
        Map<Integer, Integer> badBookies = new HashMap<Integer, Integer>();
        for (Integer bookieIndex : bookiesToCheck) {
            //回调
            LedgerFragmentCallback lfCb = new LedgerFragmentCallback(
                    fragment, bookieIndex, cb, badBookies, numBookies);
            //这里回具体去校验对应bookie上的副本
            verifyLedgerFragment(fragment, bookieIndex, lfCb, percentageOfLedgerFragmentToBeVerified);
        }
    }

    /**
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

        long firstStored = fragment.getFirstStoredEntryId(bookieIndex);
        long lastStored = fragment.getLastStoredEntryId(bookieIndex);

        BookieSocketAddress bookie = fragment.getAddress(bookieIndex);
        if (null == bookie) {
            throw new InvalidFragmentException();
        }

        if (firstStored == LedgerHandle.INVALID_ENTRY_ID) {
            // this fragment is not on this bookie
            if (lastStored != LedgerHandle.INVALID_ENTRY_ID) {
                throw new InvalidFragmentException();
            }
            //这里传入OK，代表不需要修复该fragment
            cb.operationComplete(BKException.Code.OK, fragment);
        } else if (firstStored == lastStored) {
            //如果第一个entryId和最后一个相等，那么就尝试读取下
            ReadManyEntriesCallback manycb = new ReadManyEntriesCallback(1,
                    fragment, cb);
            bookieClient.readEntry(bookie, fragment.getLedgerId(), firstStored,
                                   manycb, null, BookieProtocol.FLAG_NONE);
        } else {
            //如果开始和结束的entry并不是相等的
            if (lastStored <= firstStored) {
                cb.operationComplete(Code.IncorrectParameterException, null);
                return;
            }

            long lengthOfLedgerFragment = lastStored - firstStored + 1;

            int numberOfEntriesToBeVerified =
                (int) (lengthOfLedgerFragment * (percentageOfLedgerFragmentToBeVerified / 100.0));

            TreeSet<Long> entriesToBeVerified = new TreeSet<Long>();

            if (numberOfEntriesToBeVerified < lengthOfLedgerFragment) {
                // Evenly pick random entries over the length of the fragment
                if (numberOfEntriesToBeVerified > 0) {
                    int lengthOfBucket = (int) (lengthOfLedgerFragment / numberOfEntriesToBeVerified);
                    for (long index = firstStored;
                         index < (lastStored - lengthOfBucket - 1);
                         index += lengthOfBucket) {
                        long potentialEntryId = ThreadLocalRandom.current().nextInt((lengthOfBucket)) + index;
                        if (fragment.isStoredEntryId(potentialEntryId, bookieIndex)) {
                            entriesToBeVerified.add(potentialEntryId);
                        }
                    }
                }
                entriesToBeVerified.add(firstStored);
                entriesToBeVerified.add(lastStored);
            } else {
                // Verify the entire fragment
                while (firstStored <= lastStored) {
                    if (fragment.isStoredEntryId(firstStored, bookieIndex)) {
                        entriesToBeVerified.add(firstStored);
                    }
                    firstStored++;
                }
            }
            ReadManyEntriesCallback manycb = new ReadManyEntriesCallback(entriesToBeVerified.size(),
                    fragment, cb);
            for (Long entryID: entriesToBeVerified) {
                bookieClient.readEntry(bookie, fragment.getLedgerId(), entryID, manycb, null, BookieProtocol.FLAG_NONE);
            }
        }
    }

    /**
     * 用于检查条目是否存在的回调,它用于区分已写入但现在无法读取的情况，以及从未写入的情况。
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
                //只要不报这些异常，就是存在的
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
            // 存放损坏的fragment
            badFragments = new HashSet<LedgerFragment>();
            //初始化要检查的fragment
            this.numFragments = new AtomicLong(numFragments);
            // 回调方法
            this.cb = cb;
        }

        @Override
        public void operationComplete(int rc, LedgerFragment result) {
            if (rc == BKException.Code.ClientClosedException) {
                //如果客户端关闭了，那么久直接完成,这里传入了一次错误码，所以该bad不会检查
                cb.operationComplete(BKException.Code.ClientClosedException, badFragments);
                return;
            } else if (rc != BKException.Code.OK) {
                //否则只要是其他异常都放到badFragments中
                badFragments.add(result);
            }
            if (numFragments.decrementAndGet() == 0) {
                //这里会通过回调将损坏的fragment传回ProcessLostFragmentsCb去标记复制
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

    //lh : 该ledger的处理方法
    //cb: 回调
    //percentageOfLedgerFragmentToBeVerified：要校验的百分比
    public void checkLedger(final LedgerHandle lh,
                            final GenericCallback<Set<LedgerFragment>> cb, //ProcessLostFragmentsCb
                            long percentageOfLedgerFragmentToBeVerified) {
        // 这里装着要复制的fragment
        // build a set of all fragment replicas
        final Set<LedgerFragment> fragments = new HashSet<LedgerFragment>();

        // 这里主要作用是遍历出ledger的所有fragment，然后放入fragments中
        //这里是遍历该ledger所有的entry
        Long curEntryId = null;
        List<BookieSocketAddress> curEnsemble = null;
        for (Map.Entry<Long, ? extends List<BookieSocketAddress>> e : lh
                .getLedgerMetadata().getAllEnsembles().entrySet()) {
            if (curEntryId != null) {
                Set<Integer> bookieIndexes = new HashSet<Integer>();
                for (int i = 0; i < curEnsemble.size(); i++) {
                    bookieIndexes.add(i);
                }
                //这里把遍历的所有fragment都放入fragments中
                fragments.add(new LedgerFragment(lh, curEntryId,
                        e.getKey() - 1, bookieIndexes));
            }
            curEntryId = e.getKey();
            curEnsemble = e.getValue();
        }

        //在某些情况下，检查分类帐的最后一部分可能很复杂。 在账本关闭的情况下，即使没有数据被写入，我们也可以正常检查段的片段。 在账本打开的情况下，但是已经写入了足够多的条目，
        //对于 lastAddConfirmed 设置在段的开始条目之上，我们也可以正常检查。 但是，如果ledger是open的，有时lastAddConfirmed是不能被信任的，比如当它低于第一个条目id，
        //或者根本没有设置时，我们无法确定是否有数据写入segment。 出于这个原因，我们必须向应该有第一个条目的博彩公司发送读取请求。 如果他们回复 NoSuchEntry，我们可以假设它从未被写入。
        //如果他们回复任何其他内容，我们必须假设条目已写入，因此我们运行检查。

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
        if (curEntryId != null) {
            //如果当前curEntryId不为null，那么就说明该ledger不是空ledger
            long lastEntry = lh.getLastAddConfirmed();

            //如果该ledger还未关闭，并且curEntryId更大一些，那么久更新lastEntry
            if (!lh.isClosed() && lastEntry < curEntryId) {
                lastEntry = curEntryId;
            }

            // 遍历当前的ensemble，生产bookieIndexes集合
            Set<Integer> bookieIndexes = new HashSet<Integer>();
            for (int i = 0; i < curEnsemble.size(); i++) {
                bookieIndexes.add(i);
            }
            //最后一个fragment
            final LedgerFragment lastLedgerFragment = new LedgerFragment(lh, curEntryId,
                    lastEntry, bookieIndexes);

            // 检查没有设置最后确认条目的情况
            // Check for the case that no last confirmed entry has been set
            if (curEntryId == lastEntry) {
                final long entryToRead = curEntryId;
                final EntryExistsCallback eecb = new EntryExistsCallback(lh.getLedgerMetadata().getWriteQuorumSize(),
                                              new GenericCallback<Boolean>() {
                                                  @Override
                                                  public void operationComplete(int rc, Boolean result) {
                                                      //这里的result是代表该entry是否存在
                                                      //这个回调，需要将getWriteQuorumSize副本都读完才会回调
                                                      if (result) {
                                                          //result=true，代表存在，就直接加进去检查，如果不存在就不加进去，也就是说不检查，都不存在了，不管是不是副本全部丢失，都没办法复制了
                                                          fragments.add(lastLedgerFragment);
                                                      }
                                                      // 否则就直接check了
                                                      checkFragments(fragments, cb,
                                                          percentageOfLedgerFragmentToBeVerified);
                                                  }
                                              });

                // 这里写集合
                DistributionSchedule.WriteSet writeSet = lh.getDistributionSchedule().getWriteSet(entryToRead);
                for (int i = 0; i < writeSet.size(); i++) {
                    // 这里获取写集合地址
                    BookieSocketAddress addr = curEnsemble.get(writeSet.get(i));
                    //针对每一个副本都尝试读一下
                    bookieClient.readEntry(addr, lh.getId(), entryToRead,
                                           eecb, null, BookieProtocol.FLAG_NONE);
                }
                writeSet.recycle();
                return;
            } else {
                //将最后一个fragment加入集合
                fragments.add(lastLedgerFragment);
            }
        }
        //如果是走后面else
        checkFragments(fragments, cb, percentageOfLedgerFragmentToBeVerified);
    }

    private void checkFragments(Set<LedgerFragment> fragments,
                                GenericCallback<Set<LedgerFragment>> cb,
                                long percentageOfLedgerFragmentToBeVerified) {
        //如果没有检查的就直接返回
        if (fragments.size() == 0) { // no fragments to verify
            //注意这里传入的错误码是OK
            cb.operationComplete(BKException.Code.OK, fragments);
            return;
        }

        // verify all the collected fragment replicas
        FullLedgerCallback allFragmentsCb = new FullLedgerCallback(fragments
                .size(), cb);
        //遍历每个fragment
        for (LedgerFragment r : fragments) {
            LOG.debug("Checking fragment {}", r);
            try {
                //校验fragment
                //percentageOfLedgerFragmentToBeVerified是要校验的fragment百分比，也就是要校验多少比例的fragment
                //allFragmentsCb：这个是一个回调
                verifyLedgerFragment(r, allFragmentsCb, percentageOfLedgerFragmentToBeVerified);
            } catch (InvalidFragmentException ife) {
                LOG.error("Invalid fragment found : {}", r);
                allFragmentsCb.operationComplete(
                        BKException.Code.IncorrectParameterException, r);
            } catch (BKException e) {
                LOG.error("BKException when checking fragment : {}", r, e);
            }
        }
    }

}
