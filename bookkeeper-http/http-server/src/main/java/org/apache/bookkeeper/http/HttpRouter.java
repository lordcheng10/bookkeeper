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
package org.apache.bookkeeper.http;

import java.util.HashMap;
import java.util.Map;

/**
 * Provide the mapping of http endpoints and handlers and function
 * to bind endpoint to the corresponding handler.
 */
public abstract class HttpRouter<Handler> {

    // Define endpoints here.
    //heartbeat只是探测下是否活着
    public static final String HEARTBEAT                    = "/heartbeat";
    // 看起来是获取对应的bookie配置(加载到内存的conf配置文件配置)
    public static final String SERVER_CONFIG                = "/api/v1/config/server_config";
    // 获取metric，这个和promethus暴露的接口的差不多
    public static final String METRICS                      = "/metrics";

    // 指定删除某个leadger
    // ledger
    public static final String DELETE_LEDGER                = "/api/v1/ledger/delete";
    // list所有的leadger
    public static final String LIST_LEDGER                  = "/api/v1/ledger/list";
    // 获取ledger元数据
    public static final String GET_LEDGER_META              = "/api/v1/ledger/metadata";
    // 读取某个ledger的数据
    public static final String READ_LEDGER_ENTRY            = "/api/v1/ledger/read";
    // bookie
    //list 集群的所有bookie节点
    public static final String LIST_BOOKIES                 = "/api/v1/bookie/list_bookies";
    // 获取整个集群的bookie详细信息
    public static final String LIST_BOOKIE_INFO             = "/api/v1/bookie/list_bookie_info";
    // 获取上次标记位置
    public static final String LAST_LOG_MARK                = "/api/v1/bookie/last_log_mark";
    // 获取对应的存储文件
    public static final String LIST_DISK_FILE               = "/api/v1/bookie/list_disk_file";
    // 获取扩展存储,配置文件中，新增了磁盘目录后，重新启动后回将配置文件中的目录重新写到zk上。当然会校验下
    public static final String EXPAND_STORAGE               = "/api/v1/bookie/expand_storage";

    // 强制触发GC
    public static final String GC                           = "/api/v1/bookie/gc";

    // 获取gc详情
    public static final String GC_DETAILS                   = "/api/v1/bookie/gc_details";
    // 获取bookie状态信息
    public static final String BOOKIE_STATE                 = "/api/v1/bookie/state";
    // 检查bookie是否ready，是否完成启动
    public static final String BOOKIE_IS_READY              = "/api/v1/bookie/is_ready";
    // 获取bookie的信息：主要包括磁盘容量使用情况,
    public static final String BOOKIE_INFO                  = "/api/v1/bookie/info";

    // autorecovery
    // 开启/关闭/查下 autorecovery
    public static final String AUTORECOVERY_STATUS          = "/api/v1/autorecovery/status";
    // 给定bookie，可以将该bookie上的ledger数据，随机打散到其他bookie节点上
    public static final String RECOVERY_BOOKIE              = "/api/v1/autorecovery/bookie";
    public static final String LIST_UNDER_REPLICATED_LEDGER = "/api/v1/autorecovery/list_under_replicated_ledger";
    public static final String WHO_IS_AUDITOR               = "/api/v1/autorecovery/who_is_auditor";
    public static final String TRIGGER_AUDIT                = "/api/v1/autorecovery/trigger_audit";
    public static final String LOST_BOOKIE_RECOVERY_DELAY   = "/api/v1/autorecovery/lost_bookie_recovery_delay";
    //decommission: 退役
    public static final String DECOMMISSION                 = "/api/v1/autorecovery/decommission";


    private final Map<String, Handler> endpointHandlers = new HashMap<>();

    public HttpRouter(AbstractHttpHandlerFactory<Handler> handlerFactory) {
        //这里回放入所有的HttpEndpointService实现类,通过handlerFactory工厂类来创建,这里对应的工厂实现类只有VertxHttpHandlerFactory
        this.endpointHandlers.put(HEARTBEAT, handlerFactory.newHandler(HttpServer.ApiType.HEARTBEAT));
        this.endpointHandlers.put(SERVER_CONFIG, handlerFactory.newHandler(HttpServer.ApiType.SERVER_CONFIG));
        this.endpointHandlers.put(METRICS, handlerFactory.newHandler(HttpServer.ApiType.METRICS));

        // ledger
        this.endpointHandlers.put(DELETE_LEDGER, handlerFactory.newHandler(HttpServer.ApiType.DELETE_LEDGER));
        this.endpointHandlers.put(LIST_LEDGER, handlerFactory.newHandler(HttpServer.ApiType.LIST_LEDGER));
        this.endpointHandlers.put(GET_LEDGER_META, handlerFactory.newHandler(HttpServer.ApiType.GET_LEDGER_META));
        this.endpointHandlers.put(READ_LEDGER_ENTRY, handlerFactory.newHandler(HttpServer.ApiType.READ_LEDGER_ENTRY));

        // bookie
        this.endpointHandlers.put(LIST_BOOKIES, handlerFactory.newHandler(HttpServer.ApiType.LIST_BOOKIES));
        this.endpointHandlers.put(LIST_BOOKIE_INFO, handlerFactory.newHandler(HttpServer.ApiType.LIST_BOOKIE_INFO));
        this.endpointHandlers.put(LAST_LOG_MARK, handlerFactory.newHandler(HttpServer.ApiType.LAST_LOG_MARK));
        this.endpointHandlers.put(LIST_DISK_FILE, handlerFactory.newHandler(HttpServer.ApiType.LIST_DISK_FILE));
        this.endpointHandlers.put(EXPAND_STORAGE, handlerFactory.newHandler(HttpServer.ApiType.EXPAND_STORAGE));
        this.endpointHandlers.put(GC, handlerFactory.newHandler(HttpServer.ApiType.GC));
        this.endpointHandlers.put(GC_DETAILS, handlerFactory.newHandler(HttpServer.ApiType.GC_DETAILS));
        this.endpointHandlers.put(BOOKIE_STATE, handlerFactory.newHandler(HttpServer.ApiType.BOOKIE_STATE));
        this.endpointHandlers.put(BOOKIE_IS_READY, handlerFactory.newHandler(HttpServer.ApiType.BOOKIE_IS_READY));
        this.endpointHandlers.put(BOOKIE_INFO, handlerFactory.newHandler(HttpServer.ApiType.BOOKIE_INFO));

        // autorecovery
        this.endpointHandlers.put(AUTORECOVERY_STATUS, handlerFactory
                .newHandler(HttpServer.ApiType.AUTORECOVERY_STATUS));
        this.endpointHandlers.put(RECOVERY_BOOKIE, handlerFactory.newHandler(HttpServer.ApiType.RECOVERY_BOOKIE));
        this.endpointHandlers.put(LIST_UNDER_REPLICATED_LEDGER,
            handlerFactory.newHandler(HttpServer.ApiType.LIST_UNDER_REPLICATED_LEDGER));
        this.endpointHandlers.put(WHO_IS_AUDITOR, handlerFactory.newHandler(HttpServer.ApiType.WHO_IS_AUDITOR));
        this.endpointHandlers.put(TRIGGER_AUDIT, handlerFactory.newHandler(HttpServer.ApiType.TRIGGER_AUDIT));
        this.endpointHandlers.put(LOST_BOOKIE_RECOVERY_DELAY,
            handlerFactory.newHandler(HttpServer.ApiType.LOST_BOOKIE_RECOVERY_DELAY));
        this.endpointHandlers.put(DECOMMISSION, handlerFactory.newHandler(HttpServer.ApiType.DECOMMISSION));
    }

    /**
     * Bind all endpoints to corresponding handlers.
     */
    public void bindAll() {
        for (Map.Entry<String, Handler> entry : endpointHandlers.entrySet()) {
            bindHandler(entry.getKey(), entry.getValue());
        }
    }

    /**
     * 这里不是相应端口吧  是上面的key，这个名字endpoint有点歧义.
     *
     * Bind the given endpoint to its corresponding handlers.
     * @param endpoint http endpoint
     * @param handler http handler
     */
    public abstract void bindHandler(String endpoint, Handler handler);

}
