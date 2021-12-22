/*
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
 */
package org.apache.bookkeeper.server.http.service;

import static com.google.common.base.Preconditions.checkNotNull;
import com.google.common.collect.Lists;
import java.io.File;
import java.net.URI;
import java.util.Arrays;
import java.util.List;
import org.apache.bookkeeper.bookie.BookieException;
import org.apache.bookkeeper.bookie.BookieImpl;
import org.apache.bookkeeper.bookie.LegacyCookieValidation;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.discover.RegistrationManager;
import org.apache.bookkeeper.http.HttpServer;
import org.apache.bookkeeper.http.service.HttpEndpointService;
import org.apache.bookkeeper.http.service.HttpServiceRequest;
import org.apache.bookkeeper.http.service.HttpServiceResponse;
import org.apache.bookkeeper.meta.MetadataBookieDriver;
import org.apache.bookkeeper.meta.MetadataDrivers;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * 处理 Bookkeeper 的 HttpEndpointService 扩展存储相关的 http 请求。 PUT 方法将扩展这个 bookie 的存储。
 * 在运行命令之前，用户应该使用新的空分类帐/索引目录更新 conf 文件中的目录信息。
 * HttpEndpointService that handle Bookkeeper expand storage related http request.
 * The PUT method will expand this bookie's storage.
 * User should update the directories info in the conf file with new empty ledger/index
 * directories, before running the command.
 */
public class ExpandStorageService implements HttpEndpointService {

    static final Logger LOG = LoggerFactory.getLogger(ExpandStorageService.class);

    protected ServerConfiguration conf;

    public ExpandStorageService(ServerConfiguration conf) {
        checkNotNull(conf);
        this.conf = conf;
    }

    /**
     * 添加新的空分类帐/索引目录。
     * 在运行命令之前更新 conf 文件中的目录信息。
     *
     * Add new empty ledger/index directories.
     * Update the directories info in the conf file before running the command.
     */
    @Override
    public HttpServiceResponse handle(HttpServiceRequest request) throws Exception {
        HttpServiceResponse response = new HttpServiceResponse();

        //如果是PUT类型的请求
        if (HttpServer.Method.PUT == request.getMethod()) {
            // 获取当前的ledger目录，对应配置：ledgerDirectories
            File[] ledgerDirectories = BookieImpl.getCurrentDirectories(conf.getLedgerDirs());
            //  获取当前的journal目录，对应配置：journalDirectories
            File[] journalDirectories = BookieImpl.getCurrentDirectories(conf.getJournalDirs());
            // index目录，对应配置：indexDirectories
            File[] indexDirectories;
            if (null == conf.getIndexDirs()) {
                //如果没有设置index 目录，那么久直接复用ledger目录
                indexDirectories = ledgerDirectories;
            } else {
                //否则的话，就获取index目录
                indexDirectories = BookieImpl.getCurrentDirectories(conf.getIndexDirs());
            }

            //所有的ledger目录集合，这个集合会把ledger和index目录都包含进去
            List<File> allLedgerDirs = Lists.newArrayList();
            allLedgerDirs.addAll(Arrays.asList(ledgerDirectories));
            if (indexDirectories != ledgerDirectories) {
                allLedgerDirs.addAll(Arrays.asList(indexDirectories));
            }

            // 这里的try(){}catch(){}写法是jdk1.7后引入的，主要是为了方便IO流资源的释放
            // 该语句确保了每个资源,在语句结束时关闭。所谓的资源是指在程序完成后，必须关闭的流对象。
            //写在()里面的流对象对应的类都实现了自动关闭接口AutoCloseable；
            // try (创建流对象语句，如果多个,使用';'隔开){}catch(Exception e){}
            try (MetadataBookieDriver driver = MetadataDrivers.getBookieDriver(
                         URI.create(conf.getMetadataServiceUri()))) {
                driver.initialize(conf, NullStatsLogger.INSTANCE);

                try (RegistrationManager registrationManager = driver.createRegistrationManager()) {
                    // 旧版 Cookie 验证
                    LegacyCookieValidation validation = new LegacyCookieValidation(conf, registrationManager);
                    // 获取当前config中的journal目录
                    List<File> dirs = Lists.newArrayList(journalDirectories);
                    // 全部加入目录集合dirs中
                    dirs.addAll(allLedgerDirs);
                    //验证所有目录结构的cookie
                    validation.checkCookies(dirs);
                }
            } catch (BookieException e) {
                LOG.error("Exception occurred while updating cookie for storage expansion", e);
                response.setCode(HttpServer.StatusCode.INTERNAL_ERROR);
                response.setBody("Exception while updating cookie for storage expansion");
                return response;
            }

            String jsonResponse = "Success expand storage";
            LOG.debug("output body:" + jsonResponse);
            response.setBody(jsonResponse);
            response.setCode(HttpServer.StatusCode.OK);
            return response;
        } else {
            //如果不是PUT类型的请求，就一律报错
            response.setCode(HttpServer.StatusCode.NOT_FOUND);
            response.setBody("Not found method. Should be PUT method");
            return response;
        }
    }
}
