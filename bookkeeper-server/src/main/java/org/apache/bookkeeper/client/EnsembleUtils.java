/*
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
package org.apache.bookkeeper.client;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.bookkeeper.client.api.LedgerMetadata;

import org.apache.bookkeeper.net.BookieId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class EnsembleUtils {
    private static final Logger LOG = LoggerFactory.getLogger(EnsembleUtils.class);

    //replaceBookiesInEnsemble: 替换ensemble中的bookie
    //bookieWatcher：bookie的watcher
    // metadata: Ledger的metadata
    //failedBookies： 失败的bookie
    //logContext：日志内容
    static List<BookieId> replaceBookiesInEnsemble(BookieWatcher bookieWatcher,
                                                              LedgerMetadata metadata,
                                                              List<BookieId> oldEnsemble,
                                                              Map<Integer, BookieId> failedBookies,
                                                              String logContext)
            throws BKException.BKNotEnoughBookiesException {
        //初始化新的ensemble
        List<BookieId> newEnsemble = new ArrayList<>(oldEnsemble);

        // 获取ensemble size，对应的ledger有多少bookie
        int ensembleSize = metadata.getEnsembleSize();
        // 可写的bookie
        int writeQ = metadata.getWriteQuorumSize();
        // 确认的bookie
        int ackQ = metadata.getAckQuorumSize();
        // 自定义metadata
        Map<String, byte[]> customMetadata = metadata.getCustomMetadata();
        // 排除的bookie
        Set<BookieId> exclude = new HashSet<>(failedBookies.values());
        // 遍历失败的bookie节点
        int replaced = 0;
        for (Map.Entry<Integer, BookieId> entry : failedBookies.entrySet()) {
            //获取bookie idx
            int idx = entry.getKey();
            //获取bookieId
            BookieId addr = entry.getValue();
            if (LOG.isDebugEnabled()) {
                LOG.debug("{} replacing bookie: {} index: {}", logContext, addr, idx);
            }

            //失败的节点不在对应ensemble里，那就不用管
            if (!newEnsemble.get(idx).equals(addr)) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("{} Not changing failed bookie {} at index {}, already changed to {}",
                              logContext, addr, idx, newEnsemble.get(idx));
                }
                continue;
            }
            try {
                //选取bookie
                BookieId newBookie = bookieWatcher.replaceBookie(
                        ensembleSize, writeQ, ackQ, customMetadata, newEnsemble, idx, exclude);
                newEnsemble.set(idx, newBookie);

                replaced++;
            } catch (BKException.BKNotEnoughBookiesException e) {
                // 如果一台都没替换到，那么久抛异常
                // if there is no bookie replaced, we throw not enough bookie exception
                if (replaced <= 0) {
                    throw e;
                } else {
                    //否则就只是退出循环
                    break;
                }
            }
        }

        //返回新的ensemble节点集合
        return newEnsemble;
    }

    static Set<Integer> diffEnsemble(List<BookieId> e1,
                                     List<BookieId> e2) {
        checkArgument(e1.size() == e2.size(), "Ensembles must be of same size");
        Set<Integer> diff = new HashSet<>();
        for (int i = 0; i < e1.size(); i++) {
            if (!e1.get(i).equals(e2.get(i))) {
                diff.add(i);
            }
        }
        return diff;
    }
}
