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

import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.UncheckedExecutionException;
import java.util.Collections;
import java.util.Map;
import org.apache.bookkeeper.common.util.JsonUtil;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.http.HttpServer;
import org.apache.bookkeeper.http.service.HttpEndpointService;
import org.apache.bookkeeper.http.service.HttpServiceRequest;
import org.apache.bookkeeper.http.service.HttpServiceResponse;
import org.apache.bookkeeper.meta.LedgerUnderreplicationManager;
import org.apache.bookkeeper.meta.MetadataDrivers;
import org.apache.commons.lang3.ObjectUtils;

/**
 * 处理与自动恢复状态相关的 http 请求的 HttpEndpointService。
 *   <p></p>GET 方法返回自动恢复的当前状态。 输出类似于 {"enabled" : true}。
 *   <p>PUT 方法需要一个参数“已启用”，如果其值为“真”，则启用自动恢复，否则禁用自动恢复。 如果 Autorecovery 状态已与所需的状态相同，则该行为是幂等的。 输出将是操作后的当前状态。
 *
 * HttpEndpointService that handles Autorecovery status related http requests.
 *
 * <p></p>The GET method returns the current status of Autorecovery. The output would be like {"enabled" : true}.
 *
 * <p>The PUT method requires a parameter 'enabled', and enables Autorecovery if its value is 'true',
 * and disables Autorecovery otherwise. The behaviour is idempotent if Autorecovery status is already
 * the same as desired. The output would be the current status after the action.
 *
 */
public class AutoRecoveryStatusService implements HttpEndpointService {
    protected final ServerConfiguration conf;

    public AutoRecoveryStatusService(ServerConfiguration conf) {
        this.conf = conf;
    }

    @Override
    public HttpServiceResponse handle(HttpServiceRequest request) throws Exception {
        return MetadataDrivers.runFunctionWithLedgerManagerFactory(conf,
                ledgerManagerFactory -> {
                    try (LedgerUnderreplicationManager ledgerUnderreplicationManager = ledgerManagerFactory
                            .newLedgerUnderreplicationManager()) {
                        switch (request.getMethod()) {
                            case GET:
                                return handleGetStatus(ledgerUnderreplicationManager);
                            case PUT:
                                return handlePutStatus(request, ledgerUnderreplicationManager);
                            default:
                                return new HttpServiceResponse("Not found method. Should be GET or PUT method",
                                        HttpServer.StatusCode.NOT_FOUND);
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new UncheckedExecutionException(e);
                    } catch (Exception e) {
                        throw new UncheckedExecutionException(e);
                    }
                });
    }

    private HttpServiceResponse handleGetStatus(LedgerUnderreplicationManager ledgerUnderreplicationManager)
            throws Exception {
        //获取isLedgerReplicationEnabled是否开启，并返回给客户端
        String body = JsonUtil.toJson(ImmutableMap.of("enabled",
                ledgerUnderreplicationManager.isLedgerReplicationEnabled()));
        return new HttpServiceResponse(body, HttpServer.StatusCode.OK);
    }

    private HttpServiceResponse handlePutStatus(HttpServiceRequest request,
                                                LedgerUnderreplicationManager ledgerUnderreplicationManager)
            throws Exception {
        // 参数
        Map<String, String> params = ObjectUtils.defaultIfNull(request.getParams(), Collections.emptyMap());
        // 获取enable参数
        String enabled = params.get("enabled");
        //  如果没有该参数就报错
        if (enabled == null) {
            return new HttpServiceResponse("Param 'enabled' not found in " + params,
                    HttpServer.StatusCode.BAD_REQUEST);
        }
        //看看是否开启，如果要开启就删除对应zk目录
        if (Boolean.parseBoolean(enabled)) {
            if (!ledgerUnderreplicationManager.isLedgerReplicationEnabled()) {
                ledgerUnderreplicationManager.enableLedgerReplication();
            }
        } else {
            //否则就创建对应zk目录
            if (ledgerUnderreplicationManager.isLedgerReplicationEnabled()) {
                ledgerUnderreplicationManager.disableLedgerReplication();
            }
        }

        //返回当前状态
        // use the current status as the response
        String body = JsonUtil.toJson(ImmutableMap.of("enabled",
                ledgerUnderreplicationManager.isLedgerReplicationEnabled()));
        return new HttpServiceResponse(body, HttpServer.StatusCode.OK);
    }
}
