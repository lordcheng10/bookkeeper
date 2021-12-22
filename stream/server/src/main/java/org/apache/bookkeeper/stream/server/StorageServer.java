/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.bookkeeper.stream.server;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.bookkeeper.stream.storage.StorageConstants.ZK_METADATA_ROOT_PATH;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import java.io.File;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.UnknownHostException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.clients.config.StorageClientSettings;
import org.apache.bookkeeper.clients.impl.channel.StorageServerChannel;
import org.apache.bookkeeper.clients.impl.internal.StorageServerClientManagerImpl;
import org.apache.bookkeeper.common.component.ComponentInfoPublisher;
import org.apache.bookkeeper.common.component.ComponentStarter;
import org.apache.bookkeeper.common.component.LifecycleComponent;
import org.apache.bookkeeper.common.component.LifecycleComponentStack;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.discover.BookieServiceInfo;
import org.apache.bookkeeper.meta.zk.ZKMetadataDriverBase;
import org.apache.bookkeeper.server.http.BKHttpServiceProvider;
import org.apache.bookkeeper.server.service.HttpService;
import org.apache.bookkeeper.statelib.impl.rocksdb.checkpoint.dlog.DLCheckpointStore;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.stream.proto.common.Endpoint;
import org.apache.bookkeeper.stream.server.conf.BookieConfiguration;
import org.apache.bookkeeper.stream.server.conf.DLConfiguration;
import org.apache.bookkeeper.stream.server.conf.StorageServerConfiguration;
import org.apache.bookkeeper.stream.server.grpc.GrpcServerSpec;
import org.apache.bookkeeper.stream.server.service.BookieService;
import org.apache.bookkeeper.stream.server.service.BookieWatchService;
import org.apache.bookkeeper.stream.server.service.ClusterControllerService;
import org.apache.bookkeeper.stream.server.service.CuratorProviderService;
import org.apache.bookkeeper.stream.server.service.DLNamespaceProviderService;
import org.apache.bookkeeper.stream.server.service.GrpcService;
import org.apache.bookkeeper.stream.server.service.RegistrationServiceProvider;
import org.apache.bookkeeper.stream.server.service.RegistrationStateService;
import org.apache.bookkeeper.stream.server.service.StatsProviderService;
import org.apache.bookkeeper.stream.server.service.StorageService;
import org.apache.bookkeeper.stream.storage.StorageContainerStoreBuilder;
import org.apache.bookkeeper.stream.storage.StorageResources;
import org.apache.bookkeeper.stream.storage.conf.StorageConfiguration;
import org.apache.bookkeeper.stream.storage.impl.cluster.ClusterControllerImpl;
import org.apache.bookkeeper.stream.storage.impl.cluster.ZkClusterControllerLeaderSelector;
import org.apache.bookkeeper.stream.storage.impl.cluster.ZkClusterMetadataStore;
import org.apache.bookkeeper.stream.storage.impl.routing.RoutingHeaderProxyInterceptor;
import org.apache.bookkeeper.stream.storage.impl.sc.DefaultStorageContainerController;
import org.apache.bookkeeper.stream.storage.impl.sc.StorageContainerPlacementPolicyImpl;
import org.apache.bookkeeper.stream.storage.impl.sc.ZkStorageContainerManager;
import org.apache.bookkeeper.stream.storage.impl.store.MVCCStoreFactoryImpl;
import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.distributedlog.DistributedLogConfiguration;

/**
 * 看起来是一个Table Service API
 * 存储服务器是运行存储服务并处理 rpc 请求的服务器。
 * A storage server is a server that run storage service and serving rpc requests.
 */
@Slf4j
public class StorageServer {

    private static class ServerArguments {

        @Parameter(names = {"-c", "--conf"}, description = "Configuration file for storage server")
        private String serverConfigFile;

        @Parameter(names = {"-p", "--port"}, description = "Port to listen on for gPRC server")
        private int port = 4181;

        @Parameter(names = {"-u", "--useHostname"}, description = "Use hostname instead of IP for server ID")
        private boolean useHostname = false;

        @Parameter(names = {"-h", "--help"}, description = "Show this help message")
        private boolean help = false;

    }

    private static void loadConfFile(CompositeConfiguration conf, String confFile)
        throws IllegalArgumentException {
        try {
            Configuration loadedConf = new PropertiesConfiguration(
                new File(confFile).toURI().toURL());
            conf.addConfiguration(loadedConf);
        } catch (MalformedURLException e) {
            log.error("Could not open configuration file {}", confFile, e);
            throw new IllegalArgumentException("Could not open configuration file " + confFile, e);
        } catch (ConfigurationException e) {
            log.error("Malformed configuration file {}", confFile, e);
            throw new IllegalArgumentException("Malformed configuration file " + confFile, e);
        }
        log.info("Loaded configuration file {}", confFile);
    }

    public static Endpoint createLocalEndpoint(int port, boolean useHostname) throws UnknownHostException {
        String hostname;
        log.warn("Determining hostname for stream storage");
        if (useHostname) {
            hostname = InetAddress.getLocalHost().getCanonicalHostName();
        } else {
            hostname = InetAddress.getLocalHost().getHostAddress();
        }

        log.warn("Decided to use hostname {}", hostname);
        return Endpoint.newBuilder()
            .setHostname(hostname)
            .setPort(port)
            .build();
    }

    public static void main(String[] args) {
        int retCode = doMain(args);
        Runtime.getRuntime().exit(retCode);
    }

    static int doMain(String[] args) {
        // 注册线程未捕获的异常处理程序
        // register thread uncaught exception handler
        Thread.setDefaultUncaughtExceptionHandler((thread, exception) ->
            log.error("Uncaught exception in thread {}: {}", thread.getName(), exception.getMessage()));

        // parse the commandline
        ServerArguments arguments = new ServerArguments();
        JCommander jCommander = new JCommander(arguments);
        jCommander.setProgramName("StorageServer");
        jCommander.parse(args);

        if (arguments.help) {
            jCommander.usage();
            return ExitCode.INVALID_CONF.code();
        }

        CompositeConfiguration conf = new CompositeConfiguration();
        if (null != arguments.serverConfigFile) {
            loadConfFile(conf, arguments.serverConfigFile);
        }

        // grpc端口
        int grpcPort = arguments.port;
        // grpc 主机名
        boolean grpcUseHostname = arguments.useHostname;

        // 存储服务
        LifecycleComponent storageServer;
        try {
            // 构建存储服务
            storageServer = buildStorageServer(
                conf,
                grpcPort,
                grpcUseHostname);
        } catch (Exception e) {
            log.error("Invalid storage configuration", e);
            return ExitCode.INVALID_CONF.code();
        }

        // live future
        CompletableFuture<Void> liveFuture =
            ComponentStarter.startComponent(storageServer);
        try {
            liveFuture.get();
        } catch (InterruptedException e) {
            // the server is interrupted.
            Thread.currentThread().interrupt();
            log.info("Storage server is interrupted. Exiting ...");
        } catch (ExecutionException e) {
            log.info("Storage server is exiting ...");
        }
        return ExitCode.OK.code();
    }

    public static LifecycleComponent buildStorageServer(CompositeConfiguration conf,
                                                        int grpcPort)
            throws Exception {
        return buildStorageServer(conf, grpcPort, false, true, NullStatsLogger.INSTANCE);
    }

    public static LifecycleComponent buildStorageServer(CompositeConfiguration conf,
                                                        int grpcPort, boolean useHostname)
            throws Exception {
        return buildStorageServer(conf, grpcPort, false, useHostname, NullStatsLogger.INSTANCE);
    }

    // 配置、grpc端口、是否用hostname、是否启动bookie并开启provider、扩展状态
    public static LifecycleComponent buildStorageServer(CompositeConfiguration conf,
                                                        int grpcPort,
                                                        boolean useHostname,
                                                        boolean startBookieAndStartProvider,
                                                        StatsLogger externalStatsLogger)
            throws Exception {

        final ComponentInfoPublisher componentInfoPublisher = new ComponentInfoPublisher();
        // bookie 服务信息provider
        final Supplier<BookieServiceInfo> bookieServiceInfoProvider =
                () -> buildBookieServiceInfo(componentInfoPublisher);

        //服务构建器
        LifecycleComponentStack.Builder serverBuilder = LifecycleComponentStack.newBuilder()
            .withName("storage-server")
            .withComponentInfoPublisher(componentInfoPublisher);
        // bkConf配置
        BookieConfiguration bkConf = BookieConfiguration.of(conf);
        bkConf.validate();
        // DL配置
        DLConfiguration dlConf = DLConfiguration.of(conf);
        dlConf.validate();

        //存储服务配置
        StorageServerConfiguration serverConf = StorageServerConfiguration.of(conf);
        serverConf.validate();

        //存储配置
        StorageConfiguration storageConf = new StorageConfiguration(conf);
        storageConf.validate();

        // 获取本地端口
        // Get my local endpoint
        Endpoint myEndpoint = createLocalEndpoint(grpcPort, useHostname);

        // 创建共享资源
        // Create shared resources
        StorageResources storageResources = StorageResources.create();

        // 创建状态提供器
        // Create the stats provider
        StatsLogger rootStatsLogger;
        StatsProviderService statsProviderService = null;
        if (startBookieAndStartProvider) {
            statsProviderService = new StatsProviderService(bkConf);
            rootStatsLogger = statsProviderService.getStatsProvider().getStatsLogger("");
            serverBuilder.addComponent(statsProviderService);
            log.info("Bookie configuration : {}", bkConf.asJson());
        } else {
            rootStatsLogger = checkNotNull(externalStatsLogger,
                "External stats logger is not provided while not starting stats provider");
        }

        // 转储配置
        // dump configurations
        log.info("Dlog configuration : {}", dlConf.asJson());
        log.info("Storage configuration : {}", storageConf.asJson());
        log.info("Server configuration : {}", serverConf.asJson());

        // 创建 bookie 服务
        // Create the bookie service
        ServerConfiguration bkServerConf;
        // 判断是否要启动bookie并启动BKHttpServiceProvider
        if (startBookieAndStartProvider) {
            // 添加BookieService
            BookieService bookieService = new BookieService(bkConf, rootStatsLogger, bookieServiceInfoProvider);
            serverBuilder.addComponent(bookieService);
            bkServerConf = bookieService.serverConf();

            // 存储服务里面启动http服务
            // Build http service
            if (bkServerConf.isHttpServerEnabled()) {
                BKHttpServiceProvider provider = new BKHttpServiceProvider.Builder()
                        .setBookieServer(bookieService.getServer())
                        .setServerConfiguration(bkServerConf)
                        .setStatsProvider(statsProviderService.getStatsProvider())
                        .build();
                HttpService httpService =
                        new HttpService(provider,
                                new org.apache.bookkeeper.server.conf.BookieConfiguration(bkServerConf),
                                rootStatsLogger);
                serverBuilder.addComponent(httpService);
                log.info("Load lifecycle component : {}", HttpService.class.getName());
            }

        } else {
            bkServerConf = new ServerConfiguration();
            bkServerConf.loadConf(bkConf.getUnderlyingConf());
        }

        // 创建 bookie watch 服务
        // Create the bookie watch service
        BookieWatchService bkWatchService;
        {
            // DistributedLogConfiguration：分布式log配置
            DistributedLogConfiguration dlogConf = new DistributedLogConfiguration();
            // 加载配置
            dlogConf.loadConf(dlConf);
            // 构建watch服务：用来监听bookie节点存活情况
            bkWatchService = new BookieWatchService(
                dlogConf.getEnsembleSize(),
                bkConf,
                NullStatsLogger.INSTANCE);
        }

        // 创建策展人提供者服务
        // Create the curator provider service
        CuratorProviderService curatorProviderService = new CuratorProviderService(
            bkServerConf, dlConf, rootStatsLogger.scope("curator"));

        // 创建分布式日志命名空间服务
        // Create the distributedlog namespace service
        DLNamespaceProviderService dlNamespaceProvider = new DLNamespaceProviderService(
            bkServerConf,
            dlConf,
            rootStatsLogger.scope("dlog"));

        // 代理通道的客户端设置
        // client settings for the proxy channels
        StorageClientSettings proxyClientSettings = StorageClientSettings.newBuilder()
            .serviceUri("bk://localhost:" + grpcPort)
            .build();
        // 创建范围（流）存储
        // Create range (stream) store
        StorageContainerStoreBuilder storageContainerStoreBuilder = StorageContainerStoreBuilder.newBuilder()
            .withStatsLogger(rootStatsLogger.scope("storage"))
            .withStorageConfiguration(storageConf)
            // the storage resources shared across multiple components
            .withStorageResources(storageResources)
            // the placement policy
            .withStorageContainerPlacementPolicyFactory(() -> {
                long numStorageContainers;
                try (ZkClusterMetadataStore store = new ZkClusterMetadataStore(
                    curatorProviderService.get(),
                    ZKMetadataDriverBase.resolveZkServers(bkServerConf),
                    ZK_METADATA_ROOT_PATH)) {
                    numStorageContainers = store.getClusterMetadata().getNumStorageContainers();
                }
                return StorageContainerPlacementPolicyImpl.of((int) numStorageContainers);
            })
            // the default log backend uri
            .withDefaultBackendUri(dlNamespaceProvider.getDlogUri())
            // with zk-based storage container manager
            .withStorageContainerManagerFactory((storeConf, registry) ->
                new ZkStorageContainerManager(
                    myEndpoint,
                    storageConf,
                    new ZkClusterMetadataStore(
                        curatorProviderService.get(),
                        ZKMetadataDriverBase.resolveZkServers(bkServerConf),
                        ZK_METADATA_ROOT_PATH),
                    registry,
                    rootStatsLogger.scope("sc").scope("manager")))
            // with the inter storage container client manager
            .withRangeStoreFactory(
                new MVCCStoreFactoryImpl(
                    dlNamespaceProvider,
                    () -> new DLCheckpointStore(dlNamespaceProvider.get()),
                    storageConf.getRangeStoreDirs(),
                    storageResources,
                    storageConf.getServeReadOnlyTables(), storageConf))
            // with client manager for proxying grpc requests
            .withStorageServerClientManager(() -> new StorageServerClientManagerImpl(
                proxyClientSettings,
                storageResources.scheduler(),
                StorageServerChannel.factory(proxyClientSettings)
                    // intercept the channel to attach routing header
                    .andThen(channel -> channel.intercept(new RoutingHeaderProxyInterceptor()))
            ));
        //构建存储服务
        StorageService storageService = new StorageService(
            storageConf, storageContainerStoreBuilder, rootStatsLogger.scope("storage"));

        // 创建grpc服务
        // Create gRPC server
        StatsLogger rpcStatsLogger = rootStatsLogger.scope("grpc");
        GrpcServerSpec serverSpec = GrpcServerSpec.builder()
            .storeSupplier(storageService)
            .storeServerConf(serverConf)
            .endpoint(myEndpoint)
            .statsLogger(rpcStatsLogger)
            .build();
        GrpcService grpcService = new GrpcService(
            serverConf, serverSpec, rpcStatsLogger);

        // 创建注册服务提供者
        // Create a registration service provider
        RegistrationServiceProvider regService = new RegistrationServiceProvider(
            bkServerConf,
            dlConf,
            rootStatsLogger.scope("registration").scope("provider"));

        // 仅在服务就绪时创建注册状态服务。
        // Create a registration state service only when service is ready.
        RegistrationStateService regStateService = new RegistrationStateService(
            myEndpoint,
            bkServerConf,
            bkConf,
            regService,
            rootStatsLogger.scope("registration"));

        // 创建集群控制器服务
        // Create a cluster controller service
        ClusterControllerService clusterControllerService = new ClusterControllerService(
            storageConf,
            () -> new ClusterControllerImpl(
                new ZkClusterMetadataStore(
                    curatorProviderService.get(),
                    ZKMetadataDriverBase.resolveZkServers(bkServerConf),
                    ZK_METADATA_ROOT_PATH),
                regService.get(),
                new DefaultStorageContainerController(),
                new ZkClusterControllerLeaderSelector(curatorProviderService.get(), ZK_METADATA_ROOT_PATH),
                storageConf),
            rootStatsLogger.scope("cluster_controller"));

        // 创建所有服务堆栈
        // Create all the service stack
        return serverBuilder
            .addComponent(bkWatchService)           // service that watches bookies
            .addComponent(curatorProviderService)   // service that provides curator client
            .addComponent(dlNamespaceProvider)      // service that provides dl namespace
            .addComponent(storageService)           // range (stream) store
            .addComponent(grpcService)              // range (stream) server (gRPC)
            .addComponent(regService)               // service that provides registration client
            .addComponent(regStateService)          // service that manages server state
            .addComponent(clusterControllerService) // service that run cluster controller service
            .build();
    }

    /**
     * Create the {@link BookieServiceInfo} starting from the published endpoints.
     *
     * @see ComponentInfoPublisher
     * @param componentInfoPublisher the endpoint publisher
     * @return the created bookie service info
     */
    private static BookieServiceInfo buildBookieServiceInfo(ComponentInfoPublisher componentInfoPublisher) {
        List<BookieServiceInfo.Endpoint> endpoints = componentInfoPublisher.getEndpoints().values()
                .stream().map(e -> {
                    return new BookieServiceInfo.Endpoint(
                            e.getId(),
                            e.getPort(),
                            e.getHost(),
                            e.getProtocol(),
                            e.getAuth(),
                            e.getExtensions()
                    );
                }).collect(Collectors.toList());
        return new BookieServiceInfo(componentInfoPublisher.getProperties(), endpoints);
    }
}
