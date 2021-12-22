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
package org.apache.bookkeeper.http.vertx;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.AsyncResult;
import io.vertx.core.Vertx;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.BodyHandler;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;

import org.apache.bookkeeper.http.HttpRouter;
import org.apache.bookkeeper.http.HttpServer;
import org.apache.bookkeeper.http.HttpServiceProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Http Server 的 顶点 IO实现。
 * Vertx.io implementation of Http Server.
 */
public class VertxHttpServer implements HttpServer {

    static final Logger LOG = LoggerFactory.getLogger(VertxHttpServer.class);
    //顶点，看起来像是图结构一样
    private final Vertx vertx;
    //是否正在运行
    private boolean isRunning;
    //http 服务提供者
    private HttpServiceProvider httpServiceProvider;
    //监听端口listeningPort
    private int listeningPort = -1;

    //顶点http服务
    public VertxHttpServer() {
        this.vertx = Vertx.vertx();
    }

    //获取监听端口
    int getListeningPort() {
        return listeningPort;
    }

    //初始化httpServiceProvider
    @Override
    public void initialize(HttpServiceProvider httpServiceProvider) {
        this.httpServiceProvider = httpServiceProvider;
    }

    //默认传入端口就可以启动服务，ip默认是0.0.0.0
    @Override
    public boolean startServer(int port) {
        return startServer(port, "0.0.0.0");
    }

    @Override
    public boolean startServer(int port, String host) {
        // http 服务异步获取结果
        CompletableFuture<AsyncResult<io.vertx.core.http.HttpServer>> future = new CompletableFuture<>();
        // http handler工厂
        VertxHttpHandlerFactory handlerFactory = new VertxHttpHandlerFactory(httpServiceProvider);
        //路由器
        Router router = Router.router(vertx);
        router.route().handler(BodyHandler.create());
        //请求路由器，按照不同请求类型路由到不同的handler中去执行. 这里的handlerFactory是上面的handlerFactory工厂类，
        //该工厂类会调用httpServiceProvider来创建对应的HttpEndpointService实现类
        HttpRouter<VertxAbstractHandler> requestRouter = new HttpRouter<VertxAbstractHandler>(handlerFactory) {
           //绑定的方式是所有handler都绑定到get put post delete类型请求上
            @Override
            public void bindHandler(String endpoint, VertxAbstractHandler handler) {
                //如果是get请求就走相应的handle处理，所以我不管是发get请求还是put请求，按理都会进入到对应的handler中,
                //只不过会在对应的handler中，会根据相应的类型来进行不同的处理,同一个请求这里会不会执行4次啊
                //这里只是说会把一个handle绑定到四种类型的请求上，但并不是说一个请求会被处理四次，框架底层应该会根据对应类型来放入到对应的hanlder去处理
                //当然因为是四种类型的请求都用的同一个handler方法，所以在方法里面，需要根据不同类型请求来做不同逻辑处理
                router.get(endpoint).handler(handler);
                router.put(endpoint).handler(handler);
                router.post(endpoint).handler(handler);
                router.delete(endpoint).handler(handler);
            }
        };
        //绑定所有handle处理方法
        requestRouter.bindAll();
        vertx.deployVerticle(new AbstractVerticle() {
            @Override
            public void start() throws Exception {
                LOG.info("Starting Vertx HTTP server on port {}", port);
                //当创建成功后，会调用future的complete方法，来完成该future，这样下面调用future.get的时候，才能被唤醒
                vertx.createHttpServer().requestHandler(router).listen(port, host, future::complete);
            }
        });
        // 判断http服务启动成功没
        try {
            AsyncResult<io.vertx.core.http.HttpServer> asyncResult = future.get();
            if (asyncResult.succeeded()) {
                LOG.info("Vertx Http server started successfully");
                listeningPort = asyncResult.result().actualPort();
                isRunning = true;
                return true;
            } else {
                LOG.error("Failed to start org.apache.bookkeeper.http server on port {}", port, asyncResult.cause());
            }
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            LOG.error("Failed to start org.apache.bookkeeper.http server on port {}", port, ie);
        } catch (ExecutionException e) {
            LOG.error("Failed to start org.apache.bookkeeper.http server on port {}", port, e);
        }
        return false;
    }

    @Override
    public void stopServer() {
        CountDownLatch shutdownLatch = new CountDownLatch(1);
        try {
            httpServiceProvider.close();
        } catch (IOException ioe) {
            LOG.error("Error while close httpServiceProvider", ioe);
        }
        vertx.close(asyncResult -> {
            isRunning = false;
            shutdownLatch.countDown();
            LOG.info("HTTP server is shutdown");
        });
        try {
            shutdownLatch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.error("Interrupted while shutting down org.apache.bookkeeper.http server");
        }
    }

    @Override
    public boolean isRunning() {
        return isRunning;
    }
}
