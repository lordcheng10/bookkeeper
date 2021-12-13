/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.bookkeeper.meta;

import java.io.IOException;
import java.util.function.Consumer;
import org.apache.bookkeeper.net.BookieId;

/**
 * 用于处理分类账审计员选择的接口。
 * Interface to handle the ledger auditor election.
 */
public interface LedgerAuditorManager extends AutoCloseable {

    /**
     * 可由LedgerAuditorManager触发的事件。
     * Events that can be triggered by the LedgerAuditorManager.
     */
    enum AuditorEvent {
        SessionLost, //session断开
        VoteWasDeleted, // 选票被删除
    }

    /**
     * 试着成为auditor
     * Try to become the auditor. If there's already another auditor, it will wait until this
     * current instance has become the auditor.
     *
     * @param bookieId the identifier for current bookie
     * @param listener listener that will receive AuditorEvent notifications
     * @return
     */
    void tryToBecomeAuditor(String bookieId, Consumer<AuditorEvent> listener) throws IOException, InterruptedException;

    /**
     * 返回有关当前审计员的信息。
     * Return the information regarding the current auditor.
     * @return
     */
    BookieId getCurrentAuditor() throws IOException, InterruptedException;
}
