/*
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

package org.apache.bookkeeper.server.service;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import org.apache.bookkeeper.common.component.ComponentInfoPublisher;

import org.apache.bookkeeper.common.component.ComponentInfoPublisher.EndpointInfo;
import org.apache.bookkeeper.http.HttpServer;
import org.apache.bookkeeper.http.HttpServerLoader;
import org.apache.bookkeeper.server.component.ServerLifecycleComponent;
import org.apache.bookkeeper.server.conf.BookieConfiguration;
import org.apache.bookkeeper.server.http.BKHttpServiceProvider;
import org.apache.bookkeeper.stats.StatsLogger;

/**
 * A {@link ServerLifecycleComponent} that runs http service.
 */
public class HttpService extends ServerLifecycleComponent {

    public static final String NAME = "http-service";
    //http server
    private HttpServer server;

    //provider: 服务提供者，用来专门创建http服务的；
    //conf：配置
    //statsLogger: metric
    public HttpService(BKHttpServiceProvider provider,
                       BookieConfiguration conf,
                       StatsLogger statsLogger) {
        super(NAME, conf, statsLogger);
        //这里通过配置来加载对应的http server实现类，默认是创建VertxHttpServer类对象
        HttpServerLoader.loadHttpServer(conf.getServerConf());
        server = HttpServerLoader.get();
        checkNotNull(server, "httpServerClass is not configured or it could not be started,"
                + " please check your configuration and logs");
        //使用provider来初始化
        server.initialize(provider);
    }

    @Override
    protected void doStart() {
        //启动HttpServer
        server.startServer(conf.getServerConf().getHttpServerPort(), conf.getServerConf().getHttpServerHost());
    }

    @Override
    protected void doStop() {
        // no-op
    }

    @Override
    protected void doClose() throws IOException {
        // 停止HttpServer
        server.stopServer();
    }

    //将自己需要注册的端口，发布到组件中
    @Override
    public void publishInfo(ComponentInfoPublisher componentInfoPublisher) {
        //将端口注册到组件中
        if (conf.getServerConf().isHttpServerEnabled()) {
            EndpointInfo endpoint = new EndpointInfo("httpserver",
                    conf.getServerConf().getHttpServerPort(),
                    "0.0.0.0",
                    "http", null, null);
            componentInfoPublisher.publishEndpoint(endpoint);
        }
    }

}
