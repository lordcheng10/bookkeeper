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
package org.apache.bookkeeper.client;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.BOOKIES_JOINED;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.BOOKIES_LEFT;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.FAILED_TO_RESOLVE_NETWORK_LOCATION_COUNTER;
import static org.apache.bookkeeper.client.BookKeeperClientStats.CLIENT_SCOPE;
import static org.apache.bookkeeper.client.BookKeeperClientStats.NUM_WRITABLE_BOOKIES_IN_DEFAULT_RACK;
import static org.apache.bookkeeper.client.BookKeeperClientStats.READ_REQUESTS_REORDERED;
import static org.apache.bookkeeper.client.RegionAwareEnsemblePlacementPolicy.UNKNOWN_REGION;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;

import io.netty.util.HashedWheelTimer;

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.bookkeeper.client.BKException.BKNotEnoughBookiesException;
import org.apache.bookkeeper.client.BookieInfoReader.BookieInfo;
import org.apache.bookkeeper.client.WeightedRandomSelection.WeightedObject;
import org.apache.bookkeeper.common.util.ReflectionUtils;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.conf.Configurable;
import org.apache.bookkeeper.feature.FeatureProvider;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.net.BookieNode;
import org.apache.bookkeeper.net.DNSToSwitchMapping;
import org.apache.bookkeeper.net.NetworkTopology;
import org.apache.bookkeeper.net.NetworkTopologyImpl;
import org.apache.bookkeeper.net.Node;
import org.apache.bookkeeper.net.NodeBase;
import org.apache.bookkeeper.net.ScriptBasedMapping;
import org.apache.bookkeeper.net.StabilizeNetworkTopology;
import org.apache.bookkeeper.proto.BookieAddressResolver;
import org.apache.bookkeeper.stats.Counter;
import org.apache.bookkeeper.stats.Gauge;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.stats.annotations.StatsDoc;
import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Simple rackware ensemble placement policy.
 *
 * <p>Make most of the class and methods as protected, so it could be extended to implement other algorithms.
 */
@StatsDoc(
    name = CLIENT_SCOPE,
    help = "BookKeeper client stats"
)
public class RackawareEnsemblePlacementPolicyImpl extends TopologyAwareEnsemblePlacementPolicy {

    static final Logger LOG = LoggerFactory.getLogger(RackawareEnsemblePlacementPolicyImpl.class);
    int maxWeightMultiple;

    protected int minNumRacksPerWriteQuorum;
    protected boolean enforceMinNumRacksPerWriteQuorum;
    protected boolean ignoreLocalNodeInPlacementPolicy;

    public static final String REPP_DNS_RESOLVER_CLASS = "reppDnsResolverClass";
    public static final String REPP_RANDOM_READ_REORDERING = "ensembleRandomReadReordering";

    static final int RACKNAME_DISTANCE_FROM_LEAVES = 1;

    // masks for reordering
    static final int LOCAL_MASK       = 0x01 << 24;
    static final int LOCAL_FAIL_MASK  = 0x02 << 24;
    static final int REMOTE_MASK      = 0x04 << 24;
    static final int REMOTE_FAIL_MASK = 0x08 << 24;
    static final int READ_ONLY_MASK   = 0x10 << 24;
    static final int SLOW_MASK        = 0x20 << 24;
    static final int UNAVAIL_MASK     = 0x40 << 24;
    static final int MASK_BITS        = 0xFFF << 20;

    protected HashedWheelTimer timer;
    // Use a loading cache so slow bookies are expired. Use entryId as values.
    protected Cache<BookieId, Long> slowBookies;
    protected BookieNode localNode;
    protected boolean reorderReadsRandom = false;
    protected boolean enforceDurability = false;
    protected int stabilizePeriodSeconds = 0;
    protected int reorderThresholdPendingRequests = 0;
    // looks like these only assigned in the same thread as constructor, immediately after constructor;
    // no need to make volatile
    protected StatsLogger statsLogger = null;

    @StatsDoc(
            name = READ_REQUESTS_REORDERED,
            help = "The distribution of number of bookies reordered on each read request"
    )
    protected OpStatsLogger readReorderedCounter = null;
    @StatsDoc(
            name = FAILED_TO_RESOLVE_NETWORK_LOCATION_COUNTER,
            help = "Counter for number of times DNSResolverDecorator failed to resolve Network Location"
    )
    protected Counter failedToResolveNetworkLocationCounter = null;
    @StatsDoc(
            name = NUM_WRITABLE_BOOKIES_IN_DEFAULT_RACK,
            help = "Gauge for the number of writable Bookies in default rack"
    )
    protected Gauge<Integer> numWritableBookiesInDefaultRack;

    private String defaultRack = NetworkTopology.DEFAULT_RACK;

    RackawareEnsemblePlacementPolicyImpl() {
        this(false);
    }

    RackawareEnsemblePlacementPolicyImpl(boolean enforceDurability) {
        this.enforceDurability = enforceDurability;
        topology = new NetworkTopologyImpl();
    }

    /**
     * Initialize the policy.
     *
     * @param dnsResolver the object used to resolve addresses to their network address
     * @return initialized ensemble placement policy
     */
    protected RackawareEnsemblePlacementPolicyImpl initialize(DNSToSwitchMapping dnsResolver,
                                                              HashedWheelTimer timer,
                                                              boolean reorderReadsRandom,
                                                              int stabilizePeriodSeconds,
                                                              int reorderThresholdPendingRequests,
                                                              boolean isWeighted,
                                                              int maxWeightMultiple,
                                                              int minNumRacksPerWriteQuorum,
                                                              boolean enforceMinNumRacksPerWriteQuorum,
                                                              boolean ignoreLocalNodeInPlacementPolicy,
                                                              StatsLogger statsLogger,
                                                              BookieAddressResolver bookieAddressResolver) {
        checkNotNull(statsLogger, "statsLogger should not be null, use NullStatsLogger instead.");
        this.statsLogger = statsLogger;
        this.bookieAddressResolver = bookieAddressResolver;
        this.bookiesJoinedCounter = statsLogger.getOpStatsLogger(BOOKIES_JOINED);
        this.bookiesLeftCounter = statsLogger.getOpStatsLogger(BOOKIES_LEFT);
        this.readReorderedCounter = statsLogger.getOpStatsLogger(READ_REQUESTS_REORDERED);
        this.failedToResolveNetworkLocationCounter = statsLogger.getCounter(FAILED_TO_RESOLVE_NETWORK_LOCATION_COUNTER);
        this.numWritableBookiesInDefaultRack = new Gauge<Integer>() {
            @Override
            public Integer getDefaultValue() {
                return 0;
            }

            @Override
            public Integer getSample() {
                rwLock.readLock().lock();
                try {
                    return topology.countNumOfAvailableNodes(getDefaultRack(), Collections.emptySet());
                } finally {
                    rwLock.readLock().unlock();
                }
            }
        };
        this.statsLogger.registerGauge(NUM_WRITABLE_BOOKIES_IN_DEFAULT_RACK, numWritableBookiesInDefaultRack);
        this.reorderReadsRandom = reorderReadsRandom;
        this.stabilizePeriodSeconds = stabilizePeriodSeconds;
        this.reorderThresholdPendingRequests = reorderThresholdPendingRequests;
        this.dnsResolver = new DNSResolverDecorator(dnsResolver, () -> this.getDefaultRack(),
                failedToResolveNetworkLocationCounter);
        this.timer = timer;
        this.minNumRacksPerWriteQuorum = minNumRacksPerWriteQuorum;
        this.enforceMinNumRacksPerWriteQuorum = enforceMinNumRacksPerWriteQuorum;
        this.ignoreLocalNodeInPlacementPolicy = ignoreLocalNodeInPlacementPolicy;

        // create the network topology
        if (stabilizePeriodSeconds > 0) {
            this.topology = new StabilizeNetworkTopology(timer, stabilizePeriodSeconds);
        } else {
            this.topology = new NetworkTopologyImpl();
        }

        BookieNode bn = null;
        if (!ignoreLocalNodeInPlacementPolicy) {
            try {
                bn = createDummyLocalBookieNode(InetAddress.getLocalHost().getHostAddress());
            } catch (IOException e) {
                LOG.error("Failed to get local host address : ", e);
            }
        } else {
            LOG.info("Ignoring LocalNode in Placementpolicy");
        }
        localNode = bn;
        LOG.info("Initialize rackaware ensemble placement policy @ {} @ {} : {}.",
                localNode, null == localNode ? "Unknown" : localNode.getNetworkLocation(),
                dnsResolver.getClass().getName());

        this.isWeighted = isWeighted;
        if (this.isWeighted) {
            this.maxWeightMultiple = maxWeightMultiple;
            this.weightedSelection = new WeightedRandomSelectionImpl<BookieNode>(this.maxWeightMultiple);
            LOG.info("Weight based placement with max multiple of " + this.maxWeightMultiple);
        } else {
            LOG.info("Not weighted");
        }
        return this;
    }

    /*
     * sets default rack for the policy.
     * i.e. region-aware policy may want to have /region/rack while regular
     * rack-aware policy needs /rack only since we cannot mix both styles
     */
    public RackawareEnsemblePlacementPolicyImpl withDefaultRack(String rack) {
        checkNotNull(rack, "Default rack cannot be null");

        this.defaultRack = rack;
        return this;
    }

    public String getDefaultRack() {
        return defaultRack;
    }

    @Override
    public RackawareEnsemblePlacementPolicyImpl initialize(ClientConfiguration conf,
                                                           Optional<DNSToSwitchMapping> optionalDnsResolver,
                                                           HashedWheelTimer timer,
                                                           FeatureProvider featureProvider,
                                                           StatsLogger statsLogger,
                                                           BookieAddressResolver bookieAddressResolver) {
        this.bookieAddressResolver = bookieAddressResolver;
        DNSToSwitchMapping dnsResolver;
        if (optionalDnsResolver.isPresent()) {
            dnsResolver = optionalDnsResolver.get();
        } else {
            String dnsResolverName = conf.getString(REPP_DNS_RESOLVER_CLASS, ScriptBasedMapping.class.getName());
            try {
                dnsResolver = ReflectionUtils.newInstance(dnsResolverName, DNSToSwitchMapping.class);
                dnsResolver.setBookieAddressResolver(bookieAddressResolver);
                if (dnsResolver instanceof Configurable) {
                    ((Configurable) dnsResolver).setConf(conf);
                }

                if (dnsResolver instanceof RackChangeNotifier) {
                    ((RackChangeNotifier) dnsResolver).registerRackChangeListener(this);
                }
            } catch (RuntimeException re) {
                if (!conf.getEnforceMinNumRacksPerWriteQuorum()) {
                    LOG.error("Failed to initialize DNS Resolver {}, used default subnet resolver ",
                            dnsResolverName, re);
                    dnsResolver = new DefaultResolver(this::getDefaultRack);
                    dnsResolver.setBookieAddressResolver(bookieAddressResolver);
                } else {
                    /*
                     * if minNumRacksPerWriteQuorum is enforced, then it
                     * shouldn't continue in the case of failure to create
                     * dnsResolver.
                     */
                    throw re;
                }
            }
        }
        slowBookies = CacheBuilder.newBuilder()
            .expireAfterWrite(conf.getBookieFailureHistoryExpirationMSec(), TimeUnit.MILLISECONDS)
            .build(new CacheLoader<BookieId, Long>() {
                @Override
                public Long load(BookieId key) throws Exception {
                    return -1L;
                }
            });
        return initialize(
                dnsResolver,
                timer,
                conf.getBoolean(REPP_RANDOM_READ_REORDERING, false),
                conf.getNetworkTopologyStabilizePeriodSeconds(),
                conf.getReorderThresholdPendingRequests(),
                conf.getDiskWeightBasedPlacementEnabled(),
                //最大权重，配置是bookieMaxMultipleForWeightBasedPlacement，默认是3
                conf.getBookieMaxWeightMultipleForWeightBasedPlacement(),
                conf.getMinNumRacksPerWriteQuorum(),
                //获取强制每个写入仲裁的最小机架数的标志。
                conf.getEnforceMinNumRacksPerWriteQuorum(),
                conf.getIgnoreLocalNodeInPlacementPolicy(),
                statsLogger,
                bookieAddressResolver);
    }

    @Override
    public void uninitalize() {
        // do nothing
    }

    //excludeBookies: 排除的bookie
    //这个方法应该在 'rwLock' 的 readlock 范围内调用
    /*
     * this method should be called in readlock scope of 'rwLock'
     */
    protected Set<BookieId> addDefaultRackBookiesIfMinNumRacksIsEnforced(
            Set<BookieId> excludeBookies) {
        //综合排除BookiesSet
        Set<BookieId> comprehensiveExclusionBookiesSet;
        //enforceMinNumRacksPerWriteQuorum是否开启,从名字上看，这个变量的意思是：同时一次写的bookie集合是否需要要求跨最少的机架数,默认是false，关闭的，也就是说默认是直接把excludeBookies返回
        if (enforceMinNumRacksPerWriteQuorum) {
            //bookiesInDefaultRack：在默认机架上的bookie
            Set<BookieId> bookiesInDefaultRack = null;
            //获取默认机架下的节点
            Set<Node> defaultRackLeaves = topology.getLeaves(getDefaultRack());
            for (Node node : defaultRackLeaves) {
                //遍历这些节点，如果是BookieNode类型才操作，否则就异常
                if (node instanceof BookieNode) {
                    //bookiesInDefaultRack初始化：把excludeBookies放进去
                    if (bookiesInDefaultRack == null) {
                        bookiesInDefaultRack = new HashSet<BookieId>(excludeBookies);
                    }
                    //把node加入到bookiesInDefaultRack中
                    bookiesInDefaultRack.add(((BookieNode) node).getAddr());
                } else {
                    LOG.error("found non-BookieNode: {} as leaf of defaultrack: {}", node, getDefaultRack());
                }
            }
            //如果后去的defaultRackLeaves为空，那么上面的逻辑就不会走，bookiesInDefaultRack就会为空
            if ((bookiesInDefaultRack == null) || bookiesInDefaultRack.isEmpty()) {
                comprehensiveExclusionBookiesSet = excludeBookies;
            } else {
                //这里会把bookiesInDefaultRack放入到comprehensiveExclusionBookiesSet中
                comprehensiveExclusionBookiesSet = new HashSet<BookieId>(excludeBookies);
                comprehensiveExclusionBookiesSet.addAll(bookiesInDefaultRack);
                LOG.info("enforceMinNumRacksPerWriteQuorum is enabled, so Excluding bookies of defaultRack: {}",
                        bookiesInDefaultRack);
            }
        } else {
            //如果没开启就直接把excludeBookies返回
            comprehensiveExclusionBookiesSet = excludeBookies;
        }
        return comprehensiveExclusionBookiesSet;
    }

    @Override
    public PlacementResult<List<BookieId>> newEnsemble(int ensembleSize, int writeQuorumSize,
            int ackQuorumSize, Map<String, byte[]> customMetadata, Set<BookieId> excludeBookies)
            throws BKNotEnoughBookiesException {
        rwLock.readLock().lock();
        try {
            Set<BookieId> comprehensiveExclusionBookiesSet = addDefaultRackBookiesIfMinNumRacksIsEnforced(
                    excludeBookies);
            PlacementResult<List<BookieId>> newEnsembleResult = newEnsembleInternal(ensembleSize,
                    writeQuorumSize, ackQuorumSize, comprehensiveExclusionBookiesSet, null, null);
            return newEnsembleResult;
        } finally {
            rwLock.readLock().unlock();
        }
    }

    @Override
    public PlacementResult<List<BookieId>> newEnsemble(int ensembleSize,
                                                                  int writeQuorumSize,
                                                                  int ackQuorumSize,
                                                                  Set<BookieId> excludeBookies,
                                                                  Ensemble<BookieNode> parentEnsemble,
                                                                  Predicate<BookieNode> parentPredicate)
            throws BKNotEnoughBookiesException {
        return newEnsembleInternal(
                ensembleSize,
                writeQuorumSize,
                ackQuorumSize,
                excludeBookies,
                parentEnsemble,
                parentPredicate);
    }

    protected PlacementResult<List<BookieId>> newEnsembleInternal(
            int ensembleSize,
            int writeQuorumSize,
            int ackQuorumSize,
            Set<BookieId> excludeBookies,
            Ensemble<BookieNode> parentEnsemble,
            Predicate<BookieNode> parentPredicate) throws BKNotEnoughBookiesException {
        rwLock.readLock().lock();
        try {
            Set<Node> excludeNodes = convertBookiesToNodes(excludeBookies);
            int minNumRacksPerWriteQuorumForThisEnsemble = Math.min(writeQuorumSize, minNumRacksPerWriteQuorum);
            RRTopologyAwareCoverageEnsemble ensemble =
                    new RRTopologyAwareCoverageEnsemble(
                            ensembleSize,
                            writeQuorumSize,
                            ackQuorumSize,
                            RACKNAME_DISTANCE_FROM_LEAVES,
                            parentEnsemble,
                            parentPredicate,
                            minNumRacksPerWriteQuorumForThisEnsemble);
            BookieNode prevNode = null;
            int numRacks = topology.getNumOfRacks();
            // only one rack, use the random algorithm.
            if (numRacks < 2) {
                if (enforceMinNumRacksPerWriteQuorum && (minNumRacksPerWriteQuorumForThisEnsemble > 1)) {
                    LOG.error("Only one rack available and minNumRacksPerWriteQuorum is enforced, so giving up");
                    throw new BKNotEnoughBookiesException();
                }
                List<BookieNode> bns = selectRandom(ensembleSize, excludeNodes, TruePredicate.INSTANCE,
                        ensemble);
                ArrayList<BookieId> addrs = new ArrayList<BookieId>(ensembleSize);
                for (BookieNode bn : bns) {
                    addrs.add(bn.getAddr());
                }
                return PlacementResult.of(addrs, PlacementPolicyAdherence.FAIL);
            }

            for (int i = 0; i < ensembleSize; i++) {
                String curRack;
                if (null == prevNode) {
                    if ((null == localNode) || defaultRack.equals(localNode.getNetworkLocation())) {
                        curRack = NodeBase.ROOT;
                    } else {
                        curRack = localNode.getNetworkLocation();
                    }
                } else {
                    curRack = "~" + prevNode.getNetworkLocation();
                }
                boolean firstBookieInTheEnsemble = (null == prevNode);
                prevNode = selectFromNetworkLocation(curRack, excludeNodes, ensemble, ensemble,
                        !enforceMinNumRacksPerWriteQuorum || firstBookieInTheEnsemble);
            }
            List<BookieId> bookieList = ensemble.toList();
            if (ensembleSize != bookieList.size()) {
                LOG.error("Not enough {} bookies are available to form an ensemble : {}.",
                          ensembleSize, bookieList);
                throw new BKNotEnoughBookiesException();
            }
            return PlacementResult.of(bookieList,
                                      isEnsembleAdheringToPlacementPolicy(
                                              bookieList, writeQuorumSize, ackQuorumSize));
        } finally {
            rwLock.readLock().unlock();
        }
    }

    //ensembleSize：Ledger对应的ensemble个数
    //writeQuorumSize：Ldger一次写的bookie个数
    //ackQuorumSize：ack确认的bookie数
    //customMetadata: 自定义metadata
    //currentEnsemble：当前的ensemble集合
    //bookieToReplace：要替换的bookie
    //excludeBookies：排除的bookie
    @Override
    public PlacementResult<BookieId> replaceBookie(int ensembleSize, int writeQuorumSize, int ackQuorumSize,
            Map<String, byte[]> customMetadata, List<BookieId> currentEnsemble,
            BookieId bookieToReplace, Set<BookieId> excludeBookies)
            throws BKNotEnoughBookiesException {
        //添加读锁
        rwLock.readLock().lock();
        try {
            //如果 MinNumRacks 被强制执行，则添加默认机架的bookie到excludeBookies中，然后返回
            excludeBookies = addDefaultRackBookiesIfMinNumRacksIsEnforced(excludeBookies);
            //当前在ensemble中的bookie，也放入excludeBookies中，防止再次选到ensemble中的bookie
            excludeBookies.addAll(currentEnsemble);
            //获取要替换节点对应的bookie,如果没有获取到，那么就创建
            BookieNode bn = knownBookies.get(bookieToReplace);
            if (null == bn) {
                bn = createBookieNode(bookieToReplace);
            }
            //ensemble的节点转换为Node类型元素
            Set<Node> ensembleNodes = convertBookiesToNodes(currentEnsemble);
            //需要排除的节点,将其转换为Node类型元素
            Set<Node> excludeNodes = convertBookiesToNodes(excludeBookies);
            //加入到可排除的节点中
            excludeNodes.addAll(ensembleNodes);
            //为啥这里还要加要替换的bn节点呢，正常要替换的节点不应该是在ensembleNodes集合中吗 加了ensembleNodes，不就加了要替换的bn节点吗
            excludeNodes.add(bn);
            //从ensembleNodes中移除bn节点
            ensembleNodes.remove(bn);

            //networkLocationsToBeExcluded: 要排除的网络位置
            //这里要排除的网络位置，为啥只排除ensembleNodes,而且ensembleNodes还不包含要替换的bookie，这里看看是否有问题
            //ensemble下的所有bookie节点对应的机架都要排除掉?
            Set<String> networkLocationsToBeExcluded = getNetworkLocations(ensembleNodes);

            if (LOG.isDebugEnabled()) {
                LOG.debug("Try to choose a new bookie to replace {} from ensemble {}, excluding {}.",
                    bookieToReplace, ensembleNodes, excludeNodes);
            }
            //从要替换的机架下，获取候选的bookie
            // pick a candidate from same rack to replace
            BookieNode candidate = selectFromNetworkLocation(
                    //要替换的节点网络位置,应该就是机架位置信息
                    bn.getNetworkLocation(),
                    //排除节点的机架网络位置信息
                    networkLocationsToBeExcluded,
                    //排除的节点
                    excludeNodes,
                    TruePredicate.INSTANCE,
                    EnsembleForReplacementWithNoConstraints.INSTANCE,
                    //enforceMinNumRacksPerWriteQuorum对应配置是enforceMinNumRacksPerWriteQuorum，默认是false
                    !enforceMinNumRacksPerWriteQuorum);
            if (LOG.isDebugEnabled()) {
                LOG.debug("Bookie {} is chosen to replace bookie {}.", candidate, bn);
            }
            // 获取候选bookie的bookieid
            BookieId candidateAddr = candidate.getAddr();
            // 新的ensemble
            List<BookieId> newEnsemble = new ArrayList<BookieId>(currentEnsemble);
            if (currentEnsemble.isEmpty()) {//如果当前ensemble为空，那么久直接把选的bookie放入newEnsemble中
                /*
                 * in testing code there are test cases which would pass empty
                 * currentEnsemble
                 */
                newEnsemble.add(candidateAddr);
            } else {
                //将候选的candidateAddr，放入到newEnsemble对应要替换的bookie节点位置上
                //但在该方法调用前，不是已经从currentEnsemble中移除了bookieToReplace了吗，这里不就相当于放到最后吗 这样是否会有影响
                newEnsemble.set(currentEnsemble.indexOf(bookieToReplace), candidateAddr);
            }
            //放置结果：候选节点、是否严格按照放置策略放置的等级
            return PlacementResult.of(candidateAddr,
                    isEnsembleAdheringToPlacementPolicy(newEnsemble, writeQuorumSize, ackQuorumSize));
        } finally {
            rwLock.readLock().unlock();
        }
    }

    //整个方法和下面方法的区别在：整个方法没有传入excludeRacks
    @Override
    public BookieNode selectFromNetworkLocation(
            String networkLoc,
            Set<Node> excludeBookies,
            Predicate<BookieNode> predicate,
            Ensemble<BookieNode> ensemble,
            boolean fallbackToRandom)
            throws BKNotEnoughBookiesException {
        // select one from local rack
        try {
            //从机架中随机选择
            return selectRandomFromRack(networkLoc, excludeBookies, predicate, ensemble);
        } catch (BKNotEnoughBookiesException e) {
            //如果失败，不随机，那么久直接异常了
            if (!fallbackToRandom) {
                LOG.error(
                        "Failed to choose a bookie from {} : "
                                + "excluded {}, enforceMinNumRacksPerWriteQuorum is enabled so giving up.",
                        networkLoc, excludeBookies);
                throw e;
            }
            LOG.warn("Failed to choose a bookie from {} : "
                     + "excluded {}, fallback to choose bookie randomly from the cluster.",
                     networkLoc, excludeBookies);
            //从整个集群中随机选择一个，忽略提供的谓词。
            // randomly choose one from whole cluster, ignore the provided predicate.
            return selectRandom(1, excludeBookies, predicate, ensemble).get(0);
        }
    }

    //selectFromNetworkLocation:从网络位置选择
    //按照调用传入的逻辑，只有enforceMinNumRacksPerWriteQuorum设置未false，也就是不要钱最小的机架数，fallbackToRandom才会为true，或者说默认fallbackToRandom为true，失败就随机选
    @Override
    public BookieNode selectFromNetworkLocation(String networkLoc,
                                                   Set<String> excludeRacks,
                                                   Set<Node> excludeBookies,
                                                   Predicate<BookieNode> predicate,
                                                   Ensemble<BookieNode> ensemble,
                                                   boolean fallbackToRandom)
            throws BKNotEnoughBookiesException {
        // 首先企图从本地机架去选择
        // first attempt to select one from local rack
        try {
            //从机架中随机选择
            return selectRandomFromRack(networkLoc, excludeBookies, predicate, ensemble);
        } catch (BKNotEnoughBookiesException e) {
            //如果没有足够的bookie，那么从整个集群去选，当然要排除掉 excludeRacks和excludeBookies的节点
            //excludeRacks：排除的机架
            //excludeBookies：排除的bookie
            //predicate：断言规则
            //ensemble：Ledger可选则的bookie集合
            //fallbackToRandom：失败是否随机选择
            /*
             * there is no enough bookie from local rack, select bookies from
             * the whole cluster and exclude the racks specified at
             * <tt>excludeRacks</tt>.
             */
            return selectFromNetworkLocation(excludeRacks, excludeBookies, predicate, ensemble, fallbackToRandom);
        }
    }


    /**
     * 它随机选择一个不在 <i>excludeRacks</i> 集合中的 {@link BookieNode}，排除 <i>excludeBookies</i> 集合中的节点。 如果找不到，它会从整个集群中随机选择一个 {@link BookieNode}。
     * It randomly selects a {@link BookieNode} that is not on the <i>excludeRacks</i> set, excluding the nodes in
     * <i>excludeBookies</i> set. If it fails to find one, it selects a random {@link BookieNode} from the whole
     * cluster.
     */
    @Override
    public BookieNode selectFromNetworkLocation(Set<String> excludeRacks,
                                                   Set<Node> excludeBookies,
                                                   Predicate<BookieNode> predicate,
                                                   Ensemble<BookieNode> ensemble,
                                                   boolean fallbackToRandom)
            throws BKNotEnoughBookiesException {
        // 获取当前可知的bookie节点，应该就是存活的节点
        List<BookieNode> knownNodes = new ArrayList<>(knownBookies.values());
        //构建排除的bookie节点列表
        Set<Node> fullExclusionBookiesList = new HashSet<Node>(excludeBookies);
        //遍历所有存活的节点
        for (BookieNode knownNode : knownNodes) {
            //如果排除的机架下面包含对应节点，那么也放到排除列表中
            if (excludeRacks.contains(knownNode.getNetworkLocation())) {
                fullExclusionBookiesList.add(knownNode);
            }
        }


        try {
            //选择一个bookie节点，从当前知道的bookie节点集合中选择。
            return selectRandomInternal(knownNodes, 1, fullExclusionBookiesList, predicate, ensemble).get(0);
        } catch (BKNotEnoughBookiesException e) {
            //如果挑选失败就随机选择。
            if (!fallbackToRandom) {
                LOG.error(
                        "Failed to choose a bookie excluding Racks: {} "
                                + "Nodes: {}, enforceMinNumRacksPerWriteQuorum is enabled so giving up.",
                        excludeRacks, excludeBookies);
                throw e;
            }
            //从整个集群中进行随机选择
            LOG.warn("Failed to choose a bookie: excluded {}, fallback to choose bookie randomly from the cluster.",
                    excludeBookies);
            // randomly choose one from whole cluster
            return selectRandom(1, excludeBookies, predicate, ensemble).get(0);
        }
    }

    //创建一个基于磁盘容量权重的WeightedRandomSelection对象
    private WeightedRandomSelection<BookieNode> prepareForWeightedSelection(List<Node> leaves) {
        // 为这个机架创建一个 bookieNode->freeDiskSpace 的映射。 假设机架中的节点数量约为 40，因此在账本创建期间每次都构建它应该不会太糟糕
        // create a map of bookieNode->freeDiskSpace for this rack. The assumption is that
        // the number of nodes in a rack is of the order of 40, so it shouldn't be too bad
        // to build it every time during a ledger creation
        Map<BookieNode, WeightedObject> rackMap = new HashMap<BookieNode, WeightedObject>();
        //遍历该机架下的所有node，构建rackMap：bookie->磁盘容量权重
        for (Node n : leaves) {
            if (!(n instanceof BookieNode)) {//如果不是bookieNode就跳过
                continue;
            }
            //获取bookie
            BookieNode bookie = (BookieNode) n;
            if (this.bookieInfoMap.containsKey(bookie)) {
                rackMap.put(bookie, this.bookieInfoMap.get(bookie));
            } else {
                rackMap.put(bookie, new BookieInfo());
            }
        }
        if (rackMap.size() == 0) {
            return null;
        }

        // 构建权重选择
        WeightedRandomSelection<BookieNode> wRSelection = new WeightedRandomSelectionImpl<BookieNode>(
                maxWeightMultiple);
        // 更新rackMap到wRSelection中
        wRSelection.updateMap(rackMap);
        return wRSelection;
    }

    /**
     * Choose random node under a given network path.
     *
     * @param netPath
     *          network path
     * @param excludeBookies
     *          exclude bookies
     * @param predicate
     *          predicate to check whether the target is a good target.
     * @param ensemble
     *          ensemble structure
     * @return chosen bookie.
     */
    protected BookieNode selectRandomFromRack(String netPath, Set<Node> excludeBookies, Predicate<BookieNode> predicate,
            Ensemble<BookieNode> ensemble) throws BKNotEnoughBookiesException {
        WeightedRandomSelection<BookieNode> wRSelection = null;
        //该机架下有哪些node节点
        List<Node> leaves = new ArrayList<Node>(topology.getLeaves(netPath));
        if (!this.isWeighted) {//如果没开启权重，那么就把leaves打散
            Collections.shuffle(leaves);
        } else {
            //否则的话，开启了权重，那么就用leaves-excludeBookies来看，可用的bookie节点数，是否小于1，小于了，那么久异常
            if (CollectionUtils.subtract(leaves, excludeBookies).size() < 1) {
                throw new BKNotEnoughBookiesException();
            }
            //按照磁盘权重来构建一个基于权重的随机选择器
            wRSelection = prepareForWeightedSelection(leaves);
            //如果wRSelection还是为null，也就是说没有磁盘权重信息，相当于没有开启权重功能，那么还是抛异常
            if (wRSelection == null) {
                throw new BKNotEnoughBookiesException();
            }
        }

        //遍历leaves节点
        Iterator<Node> it = leaves.iterator();
        Set<Node> bookiesSeenSoFar = new HashSet<Node>();
        while (true) {
            //这里是选取一个节点n
            Node n;
            if (isWeighted) {//如果开启了基于权重功能
                if (bookiesSeenSoFar.size() == leaves.size()) {//如果已经把leaves遍历完了，那么久退出循环
                    // Don't loop infinitely.
                    break;
                }
                //基于磁盘容量权重去选择
                n = wRSelection.getNextRandom();
                //放入到bookiesSeenSoFar中
                bookiesSeenSoFar.add(n);
            } else {
                //没有开启权重的话，那么就遍历每个节点
                if (it.hasNext()) {
                    n = it.next();
                } else {
                    break;
                }
            }

            //如果该节点包含在excludeBookies中，那么久跳过
            if (excludeBookies.contains(n)) {
                continue;
            }
            //如果n不是BookieNode，或者n不符合断言规则，那么也跳过，断言比如： TruePredicate.INSTANCE
            if (!(n instanceof BookieNode) || !predicate.apply((BookieNode) n, ensemble)) {
                continue;
            }

            //转换成bn节点
            BookieNode bn = (BookieNode) n;
            //将该节点放入ensemble中，如果放入成功，那么就会加入到excludeBookies中
            // got a good candidate
            if (ensemble.addNode(bn)) {
                // add the candidate to exclude set
                excludeBookies.add(bn);
            }
            //直接返回选择的bookie
            return bn;
        }
        //如果在该机架下，没找到，那么久抛异常
        throw new BKNotEnoughBookiesException();
    }

    /**
     * Choose a random node from whole cluster.
     *
     * @param numBookies
     *          number bookies to choose
     * @param excludeBookies
     *          bookies set to exclude.
     * @param ensemble
     *          ensemble to hold the bookie chosen.
     * @return the bookie node chosen.
     * @throws BKNotEnoughBookiesException
     */
    protected List<BookieNode> selectRandom(int numBookies,
                                            Set<Node> excludeBookies,
                                            Predicate<BookieNode> predicate,
                                            Ensemble<BookieNode> ensemble)
            throws BKNotEnoughBookiesException {
        //传入的bookiesToSelectFrom参数为null的话，那么就是从所有活着的bookie节点中去选择
        return selectRandomInternal(null,  numBookies, excludeBookies, predicate, ensemble);
    }

    //这里会挑选出合适的bookie
    //bookiesToSelectFrom: 可供选择的bookie
    //numBookies: 要挑选的bookie数量
    //excludeBookies:排除的bookie节点,在一轮调用中，会把已经选择了的bookie也放入其中，放置下次再选择到，也就是说复用了excludeBookies变量，这样会不会影响到外面调用的逻辑
    //predicate:人为设置不能选的节点
    //ensemble: 选择的bookie会放入其中，表明该leader对应的ensemble节点有哪些
    //BKNotEnoughBookiesException:如果没找到就往外抛BKNotEnoughBookiesException异常
    protected List<BookieNode> selectRandomInternal(List<BookieNode> bookiesToSelectFrom,
                                                    int numBookies,
                                                    Set<Node> excludeBookies,
                                                    Predicate<BookieNode> predicate,
                                                    Ensemble<BookieNode> ensemble)
        throws BKNotEnoughBookiesException {
        //WeightedRandomSelection:加权随机选择
        WeightedRandomSelection<BookieNode> wRSelection = null;
        //如果可供选择的bookie对象为null，也就是没有可供选择的bookie节点，那么从整个 knownBookies 集合中进行选择
        //如果bookiesToSelectFrom未空怎么办?就抛BKNotEnoughBookiesException异常，因为减去excludeBookies后，可用的bookie数量就为0，只要numBookies大于0，那么肯定就抛异常了
        if (bookiesToSelectFrom == null) {
            // 如果列表为空，我们需要从整个 knownBookies 集合中进行选择
            // If the list is null, we need to select from the entire knownBookies set
            //weightedSelection应该是全局可选择的权重bookie列表
            wRSelection = this.weightedSelection;
            //如果是从bookiesToSelectFrom全局去选择的话，会不会选择到本来就在ensemble中的bookie节点
            //这个应该是全局存活的bookie
            bookiesToSelectFrom = new ArrayList<BookieNode>(knownBookies.values());
        }
        //isWeighted这个变量什么意思?
        //是否启用基于磁盘权重的放置策略。对应的配置是diskWeightBasedPlacementEnabled，默认是false，没有开启
        if (isWeighted) {
            //subtract: 减法
            //可选的bookie-关小黑屋的bookie，才是真正可用的bookie,就是从bookiesToSelectFrom中排除在excludeBookies中的节点
            //如果bookiesToSelectFrom未空怎么办?就抛BKNotEnoughBookiesException异常，因为减去excludeBookies后，可用的bookie数量就为0，只要numBookies大于0，那么肯定就抛异常了
            if (CollectionUtils.subtract(bookiesToSelectFrom, excludeBookies).size() < numBookies) {
                throw new BKNotEnoughBookiesException();
            }
            //如果一开始bookiesToSelectFrom!=null，那么走到这里的话，wRSelection就一定为null
            //也就是说如果如果一开始bookiesToSelectFrom一开始为null，那么只会走上面那个减法判断逻辑
            if (wRSelection == null) {
                Map<BookieNode, WeightedObject> rackMap = new HashMap<BookieNode, WeightedObject>();
                //从给定的可选bookie中入手，遍历bookiesToSelectFrom
                for (BookieNode n : bookiesToSelectFrom) {
                    //如果排除的节点中包含该节点，那么就继续选下一个节点
                    if (excludeBookies.contains(n)) {
                        continue;
                    }
                    //如果bookieInfoMap中包含了该节点，那么就放入rackMap，否则，该节点就没有包含磁盘容量信息，那么就放一个空的BookieInfo
                    if (this.bookieInfoMap.containsKey(n)) {
                        rackMap.put(n, this.bookieInfoMap.get(n));
                    } else {
                        rackMap.put(n, new BookieInfo());
                    }
                }
                //构建一个基于权重的随机选择器，然后将生成的rackMap放入其中
                //maxWeightMultiple:最大权重，配置是bookieMaxMultipleForWeightBasedPlacement，默认是3
                wRSelection = new WeightedRandomSelectionImpl<BookieNode>(this.maxWeightMultiple);
                wRSelection.updateMap(rackMap);
            }
        } else {
            //如果没有开启权重的话，这里就直接把bookiesToSelectFrom打散。
            Collections.shuffle(bookiesToSelectFrom);
        }

        //接下来才是选择bookie的主要逻辑
        BookieNode bookie;
        //numBookies是要选择的bookie数，newBookies是新的bookie
        List<BookieNode> newBookies = new ArrayList<BookieNode>(numBookies);
        //遍历bookiesToSelectFrom
        Iterator<BookieNode> it = bookiesToSelectFrom.iterator();
        //到目前为止看到的bookie,这个变量只在开启权重功能的时候用
        Set<BookieNode> bookiesSeenSoFar = new HashSet<BookieNode>();
        //循环选择bookie
        while (numBookies > 0) {

            //从候选列表bookiesToSelectFrom中，选择bookie节点
            if (isWeighted) {
                //如果开启了基于磁盘容量的权重，那么久走到这里
                if (bookiesSeenSoFar.size() == bookiesToSelectFrom.size()) {//如果已经选完bookie了，那么久从break中，终止退出循环
                    // 如果我们已经浏览了整个可用的博彩公司列表，
                    // 仍然无法满足集成请求，请退出。
                    // 我们不想无限循环。
                    // If we have gone through the whole available list of bookies,
                    // and yet haven't been able to satisfy the ensemble request, bail out.
                    // We don't want to loop infinitely.
                    break;
                }
                //用基于权重(也就是磁盘容量)的bookie中选择一个bookie节点
                bookie = wRSelection.getNextRandom();
                //然后放入bookiesSeenSoFar中
                bookiesSeenSoFar.add(bookie);
            } else {
                //如果没开启的话，就遍历bookie节点，直到遍历完
                if (it.hasNext()) {
                    bookie = it.next();
                } else {
                    break;
                }
            }

            //如果选择的这个bookie是在黑名单中，那么直接继续选下一个
            if (excludeBookies.contains(bookie)) {
                continue;
            }

            // 当持久性被强制执行时； 即使在选择随机bookie时，我们也不能违反predicate； 因为耐用性保证不是尽力而为； 它暗示了正确性
            // predicate感觉像是认为设置不能选择的bookie节点,如果在predicate中，那么久继续下一个选择
            // When durability is being enforced; we must not violate the
            // predicate even when selecting a random bookie; as durability
            // guarantee is not best effort; correctness is implied by it
            if (enforceDurability && !predicate.apply(bookie, ensemble)) {
                continue;
            }

            //将选择的bookie节点放入ensemble中
            if (ensemble.addNode(bookie)) {
                //然后排除的节点中加上该bookie，注意这里排除的节点不全是关小黑屋的bookie集合,放入excludeBookies的目的是为了放置本轮循环中，下次还会选择到该bookie节点
                excludeBookies.add(bookie);
                //放入待选的bookie集合newBookies
                newBookies.add(bookie);
                //numBookies减少1
                --numBookies;
            }
        }
        //如果numBookies减少为0，那么就直接返回，选好了
        if (numBookies == 0) {
            return newBookies;
        }

        //否则抛异常，打印warn日志
        LOG.warn("Failed to find {} bookies : excludeBookies {}, allBookies {}.",
            numBookies, excludeBookies, bookiesToSelectFrom);

        throw new BKNotEnoughBookiesException();
    }

    @Override
    public void registerSlowBookie(BookieId bookieSocketAddress, long entryId) {
        if (reorderThresholdPendingRequests <= 0) {
            // only put bookies on slowBookies list if reorderThresholdPendingRequests is *not* set (0);
            // otherwise, rely on reordering of reads based on reorderThresholdPendingRequests
            slowBookies.put(bookieSocketAddress, entryId);
        }
    }

    @Override
    public DistributionSchedule.WriteSet reorderReadSequence(
            List<BookieId> ensemble,
            BookiesHealthInfo bookiesHealthInfo,
            DistributionSchedule.WriteSet writeSet) {
        Map<Integer, String> writeSetWithRegion = new HashMap<>();
        for (int i = 0; i < writeSet.size(); i++) {
            writeSetWithRegion.put(writeSet.get(i), "");
        }
        return reorderReadSequenceWithRegion(
            ensemble, writeSet, writeSetWithRegion, bookiesHealthInfo, false, "", writeSet.size());
    }

    /**
     * This function orders the read sequence with a given region. For region-unaware policies (e.g.
     * RackAware), we pass in false for regionAware and an empty myRegion. When this happens, any
     * remote list will stay empty. The ordering is as follows (the R* at the beginning of each list item
     * is only present for region aware policies).
     *      1. available (local) bookies
     *      2. R* a remote bookie (based on remoteNodeInReorderSequence
     *      3. R* remaining (local) bookies
     *      4. R* remaining remote bookies
     *      5. read only bookies
     *      6. slow bookies
     *      7. unavailable bookies
     *
     * @param ensemble
     *          ensemble of bookies
     * @param writeSet
     *          write set
     * @param writeSetWithRegion
     *          write set with region information
     * @param bookiesHealthInfo
     *          heuristics about health of boookies
     * @param regionAware
     *          whether or not a region-aware policy is used
     * @param myRegion
     *          current region of policy
     * @param remoteNodeInReorderSequence
     *          number of local bookies to try before trying a remote bookie
     * @return ordering of bookies to send read to
     */
    DistributionSchedule.WriteSet reorderReadSequenceWithRegion(
        List<BookieId> ensemble,
        DistributionSchedule.WriteSet writeSet,
        Map<Integer, String> writeSetWithRegion,
        BookiesHealthInfo bookiesHealthInfo,
        boolean regionAware,
        String myRegion,
        int remoteNodeInReorderSequence) {
        boolean useRegionAware = regionAware && (!myRegion.equals(UNKNOWN_REGION));
        int ensembleSize = ensemble.size();

        // For rack aware, If all the bookies in the write set are available, simply return the original write set,
        // to avoid creating more lists
        boolean isAnyBookieUnavailable = false;

        if (useRegionAware || reorderReadsRandom) {
            isAnyBookieUnavailable = true;
        } else {
            for (int i = 0; i < ensemble.size(); i++) {
                BookieId bookieAddr = ensemble.get(i);
                if ((!knownBookies.containsKey(bookieAddr) && !readOnlyBookies.contains(bookieAddr))
                    || slowBookies.getIfPresent(bookieAddr) != null) {
                    // Found at least one bookie not available in the ensemble, or in slowBookies
                    isAnyBookieUnavailable = true;
                    break;
                }
            }
        }

        boolean reordered = false;
        if (reorderThresholdPendingRequests > 0) {
            // if there are no slow or unavailable bookies, capture each bookie's number of
            // pending request to reorder requests based on a threshold of pending requests

            // number of pending requests per bookie (same index as writeSet)
            long[] pendingReqs = new long[writeSet.size()];
            int bestBookieIdx = -1;

            for (int i = 0; i < writeSet.size(); i++) {
                pendingReqs[i] = bookiesHealthInfo.getBookiePendingRequests(ensemble.get(writeSet.get(i)));
                if (bestBookieIdx < 0 || pendingReqs[i] < pendingReqs[bestBookieIdx]) {
                    bestBookieIdx = i;
                }
            }

            // reorder the writeSet if the currently first bookie in our writeSet has at
            // least
            // reorderThresholdPendingRequests more outstanding request than the best bookie
            if (bestBookieIdx > 0 && pendingReqs[0] >= pendingReqs[bestBookieIdx] + reorderThresholdPendingRequests) {
                // We're not reordering the entire write set, but only move the best bookie
                // to the first place. Chances are good that this bookie will be fast enough
                // to not trigger the speculativeReadTimeout. But even if it hits that timeout,
                // things may have changed by then so much that whichever bookie we put second
                // may actually not be the second-best choice any more.
                if (LOG.isDebugEnabled()) {
                    LOG.debug("read set reordered from {} ({} pending) to {} ({} pending)",
                            ensemble.get(writeSet.get(0)), pendingReqs[0], ensemble.get(writeSet.get(bestBookieIdx)),
                            pendingReqs[bestBookieIdx]);
                }
                writeSet.moveAndShift(bestBookieIdx, 0);
                reordered = true;
            }
        }

        if (!isAnyBookieUnavailable) {
            if (reordered) {
                readReorderedCounter.registerSuccessfulValue(1);
            }
            return writeSet;
        }

        for (int i = 0; i < writeSet.size(); i++) {
            int idx = writeSet.get(i);
            BookieId address = ensemble.get(idx);
            String region = writeSetWithRegion.get(idx);
            Long lastFailedEntryOnBookie = bookiesHealthInfo.getBookieFailureHistory(address);
            if (null == knownBookies.get(address)) {
                // there isn't too much differences between readonly bookies
                // from unavailable bookies. since there
                // is no write requests to them, so we shouldn't try reading
                // from readonly bookie prior to writable bookies.
                if ((null == readOnlyBookies)
                    || !readOnlyBookies.contains(address)) {
                    writeSet.set(i, idx | UNAVAIL_MASK);
                } else {
                    if (slowBookies.getIfPresent(address) != null) {
                        long numPendingReqs = bookiesHealthInfo.getBookiePendingRequests(address);
                        // use slow bookies with less pending requests first
                        long slowIdx = numPendingReqs * ensembleSize + idx;
                        writeSet.set(i, (int) (slowIdx & ~MASK_BITS) | SLOW_MASK);
                    } else {
                        writeSet.set(i, idx | READ_ONLY_MASK);
                    }
                }
            } else if (lastFailedEntryOnBookie < 0) {
                if (slowBookies.getIfPresent(address) != null) {
                    long numPendingReqs = bookiesHealthInfo.getBookiePendingRequests(address);
                    long slowIdx = numPendingReqs * ensembleSize + idx;
                    writeSet.set(i, (int) (slowIdx & ~MASK_BITS) | SLOW_MASK);
                } else {
                    if (useRegionAware && !myRegion.equals(region)) {
                        writeSet.set(i, idx | REMOTE_MASK);
                    } else {
                        writeSet.set(i, idx | LOCAL_MASK);
                    }
                }
            } else {
                // use bookies with earlier failed entryIds first
                long failIdx = lastFailedEntryOnBookie * ensembleSize + idx;
                if (useRegionAware && !myRegion.equals(region)) {
                    writeSet.set(i, (int) (failIdx & ~MASK_BITS) | REMOTE_FAIL_MASK);
                } else {
                    writeSet.set(i, (int) (failIdx & ~MASK_BITS) | LOCAL_FAIL_MASK);
                }
            }
        }

        // Add a mask to ensure the sort is stable, sort,
        // and then remove mask. This maintains stability as
        // long as there are fewer than 16 bookies in the write set.
        for (int i = 0; i < writeSet.size(); i++) {
            writeSet.set(i, writeSet.get(i) | ((i & 0xF) << 20));
        }
        writeSet.sort();
        for (int i = 0; i < writeSet.size(); i++) {
            writeSet.set(i, writeSet.get(i) & ~((0xF) << 20));
        }

        if (reorderReadsRandom) {
            shuffleWithMask(writeSet, LOCAL_MASK, MASK_BITS);
            shuffleWithMask(writeSet, REMOTE_MASK, MASK_BITS);
            shuffleWithMask(writeSet, READ_ONLY_MASK, MASK_BITS);
            shuffleWithMask(writeSet, UNAVAIL_MASK, MASK_BITS);
        }

        // nodes within a region are ordered as follows
        // (Random?) list of nodes that have no history of failure
        // Nodes with Failure history are ordered in the reverse
        // order of the most recent entry that generated an error
        // The sort will have put them in correct order,
        // so remove the bits that sort by age.
        for (int i = 0; i < writeSet.size(); i++) {
            int mask = writeSet.get(i) & MASK_BITS;
            int idx = (writeSet.get(i) & ~MASK_BITS) % ensembleSize;
            if (mask == LOCAL_FAIL_MASK) {
                writeSet.set(i, LOCAL_MASK | idx);
            } else if (mask == REMOTE_FAIL_MASK) {
                writeSet.set(i, REMOTE_MASK | idx);
            } else if (mask == SLOW_MASK) {
                writeSet.set(i, SLOW_MASK | idx);
            }
        }

        // Insert a node from the remote region at the specified location so
        // we try more than one region within the max allowed latency
        int firstRemote = -1;
        for (int i = 0; i < writeSet.size(); i++) {
            if ((writeSet.get(i) & MASK_BITS) == REMOTE_MASK) {
                firstRemote = i;
                break;
            }
        }
        if (firstRemote != -1) {
            int i = 0;
            for (; i < remoteNodeInReorderSequence
                && i < writeSet.size(); i++) {
                if ((writeSet.get(i) & MASK_BITS) != LOCAL_MASK) {
                    break;
                }
            }
            writeSet.moveAndShift(firstRemote, i);
        }


        // remove all masks
        for (int i = 0; i < writeSet.size(); i++) {
            writeSet.set(i, writeSet.get(i) & ~MASK_BITS);
        }
        readReorderedCounter.registerSuccessfulValue(1);
        return writeSet;
    }

    // this method should be called in readlock scope of 'rwlock'
    @Override
    public PlacementPolicyAdherence isEnsembleAdheringToPlacementPolicy(List<BookieId> ensembleList,
            int writeQuorumSize, int ackQuorumSize) {
        int ensembleSize = ensembleList.size();
        int minNumRacksPerWriteQuorumForThisEnsemble = Math.min(writeQuorumSize, minNumRacksPerWriteQuorum);
        HashSet<String> racksInQuorum = new HashSet<String>();
        BookieId bookie;
        for (int i = 0; i < ensembleList.size(); i++) {
            racksInQuorum.clear();
            for (int j = 0; j < writeQuorumSize; j++) {
                bookie = ensembleList.get((i + j) % ensembleSize);
                try {
                    racksInQuorum.add(knownBookies.get(bookie).getNetworkLocation());
                } catch (Exception e) {
                    /*
                     * any issue/exception in analyzing whether ensemble is
                     * strictly adhering to placement policy should be
                     * swallowed.
                     */
                    LOG.warn("Received exception while trying to get network location of bookie: {}", bookie, e);
                }
            }
            if ((racksInQuorum.size() < minNumRacksPerWriteQuorumForThisEnsemble)
                    || (enforceMinNumRacksPerWriteQuorum && racksInQuorum.contains(getDefaultRack()))) {
                return PlacementPolicyAdherence.FAIL;
            }
        }
        return PlacementPolicyAdherence.MEETS_STRICT;
    }

    @Override
    public boolean areAckedBookiesAdheringToPlacementPolicy(Set<BookieId> ackedBookies,
                                                            int writeQuorumSize,
                                                            int ackQuorumSize) {
        HashSet<String> rackCounter = new HashSet<>();
        int minWriteQuorumNumRacksPerWriteQuorum = Math.min(writeQuorumSize, minNumRacksPerWriteQuorum);

        ReentrantReadWriteLock.ReadLock readLock = rwLock.readLock();
        readLock.lock();
        try {
            for (BookieId bookie : ackedBookies) {
                rackCounter.add(knownBookies.get(bookie).getNetworkLocation());
            }

            // Check to make sure that ensemble is writing to `minNumberOfRacks`'s number of racks at least.
            if (LOG.isDebugEnabled()) {
                LOG.debug("areAckedBookiesAdheringToPlacementPolicy returning {} because number of racks = {} and "
                          + "minNumRacksPerWriteQuorum = {}",
                          rackCounter.size() >= minNumRacksPerWriteQuorum,
                          rackCounter.size(),
                          minNumRacksPerWriteQuorum);
            }
        } finally {
            readLock.unlock();
        }
        return rackCounter.size() >= minWriteQuorumNumRacksPerWriteQuorum;
    }
}
