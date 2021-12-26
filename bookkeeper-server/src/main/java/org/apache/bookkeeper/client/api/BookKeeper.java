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
package org.apache.bookkeeper.client.api;

import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.client.impl.BookKeeperBuilderImpl;
import org.apache.bookkeeper.common.annotation.InterfaceAudience.Public;
import org.apache.bookkeeper.common.annotation.InterfaceStability.Unstable;
import org.apache.bookkeeper.conf.ClientConfiguration;

/**
 * 这是 BookKeeper 客户端 API 的入口点。
 * This is the entry point for BookKeeper client side API.
 *
 * @since 4.6
 */
@Public
@Unstable
public interface BookKeeper extends AutoCloseable {

    /**
     * 创建一个可用于启动新 BookKeeper 客户端的新构建器。
     * Create a new builder which can be used to boot a new BookKeeper client.
     *
     * @param clientConfiguration the configuration for the client 传入构建client所需的配置
     * @return a builder 返回一个构建器
     */
    static BookKeeperBuilder newBuilder(final ClientConfiguration clientConfiguration) {
        return new BookKeeperBuilderImpl(clientConfiguration);
    }

    /**
     * 开始创建新的ledger
     * Start the creation of a new ledger.
     *
     * @return a builder for the new ledger 返回一个ledger构建器
     */
    CreateBuilder newCreateLedgerOp();

    /**
     * 打开一个现存的ledger
     * Open an existing ledger.
     *
     * @return a builder useful to create a readable handler for an existing ledger 用于为现有分类帐创建可读处理程序的构建器
     */
    OpenBuilder newOpenLedgerOp();

    /**
     * 删除现有分类帐。
     * Delete an existing ledger.
     *
     * @return a builder useful to delete an existing ledger 用于删除现有分类帐的构建器
     */
    DeleteBuilder newDeleteLedgerOp();

    /**
     * 列出分类帐。
     * List ledgers.
     *
     * @return a builder useful to list ledgers. 用于列出分类帐的构建器。
     */
    ListLedgersResultBuilder newListLedgersOp();

    /**
     * 获取一个ledger的metadata信息
     * Get ledger metadata of a given ledger id.
     *
     * @param ledgerId id of the ledger. 传入ledger id
     * @return a <code>CompletableFuture</code> instance containing ledger metadata. 返回ledger metadata的complete futrue结果
     */
    CompletableFuture<LedgerMetadata> getLedgerMetadata(long ledgerId);

    /**
     * 关闭客户端并释放所有资源。
     * Close the client and release every resource.
     *
     * @throws BKException
     * @throws InterruptedException
     */
    @Override
    void close() throws BKException, InterruptedException;

}
