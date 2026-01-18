/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.server.kv;

import org.apache.fluss.compression.ArrowCompressionInfo;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.MemorySize;
import org.apache.fluss.config.TableConfig;
import org.apache.fluss.config.cluster.ServerReconfigurable;
import org.apache.fluss.exception.ConfigException;
import org.apache.fluss.exception.KvStorageException;
import org.apache.fluss.fs.FileSystem;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.memory.LazyMemorySegmentPool;
import org.apache.fluss.memory.MemorySegmentPool;
import org.apache.fluss.metadata.KvFormat;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.SchemaGetter;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.server.TabletManagerBase;
import org.apache.fluss.server.kv.rowmerger.RowMerger;
import org.apache.fluss.server.log.LogManager;
import org.apache.fluss.server.log.LogTablet;
import org.apache.fluss.server.metrics.group.TabletServerMetricGroup;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.shaded.arrow.org.apache.arrow.memory.BufferAllocator;
import org.apache.fluss.shaded.arrow.org.apache.arrow.memory.RootAllocator;
import org.apache.fluss.utils.FileUtils;
import org.apache.fluss.utils.FlussPaths;
import org.apache.fluss.utils.MapUtils;
import org.apache.fluss.utils.types.Tuple2;

import org.rocksdb.RateLimiter;
import org.rocksdb.RateLimiterMode;
import org.rocksdb.RocksDB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.fluss.utils.concurrent.LockUtils.inLock;

/**
 * The entry point to the fluss kv management subsystem. The kv manager is responsible for kv tablet
 * creation, retrieval, and cleaning. All read and write operations to kv tablet are delegated to
 * the individual instances.
 */
// KvManager 的核心职责是管理 Tablet Server 上所有 KV 分桶（KV Tablet）的生命周期。其具体作用包括：
// 管理入口：作为 KV 子系统的唯一入口，协调 KV Tablet 的创建、检索、加载和销毁。
// 资源调度：管理所有 KV 实例共享的资源，如内存池（MemorySegmentPool）、Arrow 分配器（BufferAllocator）以及全局写入限流器（RateLimiter）。
// 桥接 Log 与 KV：在 Fluss 中，KV 数据通常由 Log（日志）数据回放生成，KvManager 负责在加载 KV 时关联对应的 LogTablet。
// 存储抽象：屏蔽底层 RocksDB 的复杂性，提供统一的 Tablet 操作接口。

@ThreadSafe
public final class KvManager extends TabletManagerBase implements ServerReconfigurable {

    private static final Logger LOG = LoggerFactory.getLogger(KvManager.class);

    /**
     * Default global rate limiter with unlimited rate (Long.MAX_VALUE bytes per second).
     *
     * <p>This is used by RocksDBResourceContainer when no rate limiter is explicitly provided,
     * ensuring the API is safer and more robust by avoiding null checks throughout the code.
     */
    // 默认全局限流器，初始值为 Long.MAX_VALUE（即不限流），用于确保代码安全性，避免大量的空指针检查。
    private static final RateLimiter DEFAULT_RATE_LIMITER = createDefaultRateLimiter();

    /**
     * Creates a default rate limiter with unlimited rate (Long.MAX_VALUE bytes per second).
     *
     * @return a default rate limiter instance
     */
    private static RateLimiter createDefaultRateLimiter() {
        RocksDB.loadLibrary();
        // Create a rate limiter with unlimited rate (effectively no limit)
        // Using default refill period and fairness values
        return new RateLimiter(Long.MAX_VALUE);
    }

    /**
     * Returns the default global rate limiter with unlimited rate.
     *
     * <p>This method provides access to the default rate limiter for use in
     * RocksDBResourceContainer when no rate limiter is explicitly provided.
     *
     * @return the default rate limiter instance
     */
    public static RateLimiter getDefaultRateLimiter() {
        return DEFAULT_RATE_LIMITER;
    }
    // 引用 LogManager 实例，用于在加载 KV Tablet 时获取其对应的日志组件。
    private final LogManager logManager;
    // 监控指标组，负责收集 KV 层的性能指标（如读写延迟、RocksDB 统计信息）。
    private final TabletServerMetricGroup serverMetricGroup;
    // ZooKeeper 客户端，用于从元数据中心获取表结构（Schema）和配置信息。
    private final ZooKeeperClient zkClient;
    // 维护当前节点上所有活跃的 TableBucket 到 KvTablet 的映射。
    private final Map<TableBucket, KvTablet> currentKvs = MapUtils.newConcurrentHashMap();

    /**
     * For arrow log format. The buffer allocator to allocate memory for arrow write batch of
     * changelog records.
     */
    // Arrow 内存分配器，专门用于处理以 Arrow 格式存储的 Changelog 记录的内存分配。
    private final BufferAllocator arrowBufferAllocator;

    /** The memory segment pool to allocate memorySegment. */
    // 内存段池，管理预分配的内存块，用于优化 I/O 性能和内存复用。
    private final MemorySegmentPool memorySegmentPool;
    // 远程存储路径和文件系统接口，用于处理 KV 状态的远程快照（Snapshot）。
    private final FsPath remoteKvDir;

    private final FileSystem remoteFileSystem;

    /**
     * The shared rate limiter for all RocksDB instances to control flush and compaction write rate.
     */
    // 核心资源控制。一个被所有 RocksDB 实例共享的限流器，用于全局控制 Flush（刷盘）和 Compaction（合并）带来的磁盘写入带宽占用。
    private final RateLimiter sharedRocksDBRateLimiter;

    /** Current shared rate limiter configuration in bytes per second. */
    // 当前共享限流器的速率阈值（字节/秒），支持通过 volatile 关键字保证动态更新的可见性。
    private volatile long currentSharedRateLimitBytesPerSec;
    // 标记位，指示 KV 管理器是否正在关闭，用于防止重配置或新任务启动。
    private volatile boolean isShutdown = false;

    private KvManager(
            File dataDir,
            Configuration conf,
            ZooKeeperClient zkClient,
            int recoveryThreadsPerDataDir,
            LogManager logManager,
            TabletServerMetricGroup tabletServerMetricGroup)
            throws IOException {
        super(TabletType.KV, dataDir, conf, recoveryThreadsPerDataDir);
        this.logManager = logManager;
        this.arrowBufferAllocator = new RootAllocator(Long.MAX_VALUE);
        this.memorySegmentPool = LazyMemorySegmentPool.createServerBufferPool(conf);
        this.zkClient = zkClient;
        this.remoteKvDir = FlussPaths.remoteKvDir(conf);
        this.remoteFileSystem = remoteKvDir.getFileSystem();
        this.serverMetricGroup = tabletServerMetricGroup;
        this.sharedRocksDBRateLimiter = createSharedRateLimiter(conf);
        this.currentSharedRateLimitBytesPerSec =
                conf.get(ConfigOptions.KV_SHARED_RATE_LIMITER_BYTES_PER_SEC).getBytes();
    }
    // 作用是为当前 Tablet Server 上的所有 RocksDB 实例创建一个全局共享的写入限流器。
    // 在分布式存储中，这能有效防止 RocksDB 的后台操作（如 Flush 和 Compaction）占用过高的磁盘 I/O，从而保障前台读写请求的稳定性。
    private static RateLimiter createSharedRateLimiter(Configuration conf) {
        // 从配置对象中获取设定的限流速率。
        long sharedRateLimitBytesPerSecond =
                conf.get(ConfigOptions.KV_SHARED_RATE_LIMITER_BYTES_PER_SEC).getBytes();
        // 确保 RocksDB 的 C++ 本地库（JNI）已加载到 JVM 中。
        // 因为 RateLimiter 是 RocksDB 原生 C++ 对象的 Java 封装，在调用构造函数之前，必须先加载动态链接库，否则会抛出 UnsatisfiedLinkError
        RocksDB.loadLibrary();
        // Always create a shared rate limiter with the configured rate limit.
        // The rate limiter is always enabled with a default value of Long.MAX_VALUE (effectively
        // unlimited).
        // This avoids the overhead of dynamically enabling/disabling the rate limiter.
        // refill_period_us is set to 100ms, fairness is set to 10
        return new RateLimiter(
                sharedRateLimitBytesPerSecond, // 1. 速率阈值
                RateLimiter.DEFAULT_REFILL_PERIOD_MICROS, // 2. 令牌刷新周期
                RateLimiter.DEFAULT_FAIRNESS,  // 3. 公平性系数
                RateLimiterMode.WRITES_ONLY, // 4. 限流模式
                false); // 5. 自动调优开关
    }

    public static KvManager create(
            Configuration conf,
            ZooKeeperClient zkClient,
            LogManager logManager,
            TabletServerMetricGroup tabletServerMetricGroup)
            throws IOException {
        String dataDirString = conf.getString(ConfigOptions.DATA_DIR);
        File dataDir = new File(dataDirString).getAbsoluteFile();
        return new KvManager(
                dataDir,
                conf,
                zkClient,
                conf.getInt(ConfigOptions.NETTY_SERVER_NUM_WORKER_THREADS),
                logManager,
                tabletServerMetricGroup);
    }

    public void startup() {
        // should do nothing now
    }

    public void shutdown() {
        LOG.info("Shutting down KvManager");
        isShutdown = true;
        List<KvTablet> kvs = new ArrayList<>(currentKvs.values());
        for (KvTablet kvTablet : kvs) {
            try {
                kvTablet.close();
            } catch (Exception e) {
                LOG.warn("Exception while closing kv tablet {}.", kvTablet.getTableBucket(), e);
            }
        }
        arrowBufferAllocator.close();
        memorySegmentPool.close();
        if (sharedRocksDBRateLimiter != null) {
            sharedRocksDBRateLimiter.close();
        }
        LOG.info("Shut down KvManager complete.");
    }

    /**
     * If the kv already exists, just return a copy of the existing kv. Otherwise, create a kv for
     * the given table and the given bucket.
     *
     * <p>Note: if the parameter {@code partitionName} is null, the log dir path is:
     * /{database}/{table-name}-{table_id}/kv-{bucket-id}. Otherwise, the log dir path is:
     * /{database}/{table-name}-{partitionName}-{table_id}-p{partition_id}/kv-{bucket-id}
     *
     * @param tablePath the table path of the bucket belongs to
     * @param tableBucket the table bucket
     * @param logTablet the cdc log tablet of the kv tablet
     * @param kvFormat the kv format
     */
    // 负责 KV 分桶（KV Tablet）的按需初始化。该方法通过加锁确保了在并发环境下，同一个分桶不会被重复创建。
    public KvTablet getOrCreateKv(
            PhysicalTablePath tablePath,
            TableBucket tableBucket,
            LogTablet logTablet,
            KvFormat kvFormat,
            SchemaGetter schemaGetter,
            TableConfig tableConfig,
            ArrowCompressionInfo arrowCompressionInfo)
            throws Exception {
        return inLock(
                tabletCreationOrDeletionLock,
                () -> {
                    // 如果该分桶（Bucket）对应的 KvTablet 对象已经存在，说明之前已经创建过了，直接从 Map 中获取并返回。
                    if (currentKvs.containsKey(tableBucket)) {
                        return currentKvs.get(tableBucket);
                    }
                    // 根据表路径和分桶 ID 计算出物理路径。如果目录不存在，该方法会执行创建操作（例如 .../kv-0）。
                    // 这是 RocksDB 存储数据的物理基础。
                    File tabletDir = getOrCreateTabletDir(tablePath, tableBucket);
                    // 在 KV 模型中，当有多条相同 Key 的变更记录（Changelog）进入时，
                    // RowMerger 决定了如何将这些变更合并成最终的一行（例如处理更新、删除逻辑）。它依赖于表的配置（tableConfig）和 Schema 信息。
                    RowMerger merger = RowMerger.create(tableConfig, kvFormat, schemaGetter);
                    KvTablet tablet =
                            KvTablet.create(
                                    tablePath, // 物理表路径
                                    tableBucket, // 分桶元数据
                                    logTablet, // 关联的 Log 组件
                                    tabletDir, // 磁盘路径
                                    conf, // 系统配置
                                    serverMetricGroup, // 指标监控组
                                    arrowBufferAllocator, // Arrow 内存分配器
                                    memorySegmentPool, // 内存段池
                                    kvFormat, // 存储格式
                                    merger, // 上一步创建的合并器
                                    arrowCompressionInfo, // 压缩配置
                                    schemaGetter, // Schema 获取器
                                    tableConfig.getChangelogImage(), // 是否生成镜像
                                    sharedRocksDBRateLimiter); // 共享限流器
                    currentKvs.put(tableBucket, tablet);

                    LOG.info(
                            "Created kv tablet for bucket {} in dir {}.",
                            tableBucket,
                            tabletDir.getAbsolutePath());

                    return tablet;
                });
    }

    /**
     * Create the tablet directory for the given table path and table bucket.
     *
     * <p>When the tablet directory exists, it will first delete it and create a new directory.
     *
     * @param tablePath the table path of the bucket
     * @param tableBucket the table bucket
     * @return the tablet directory
     */
    // 作用是为特定的分桶（Bucket）初始化一个干净的物理存储目录。
    // 法带有“强制重置”的语义：如果目录已经存在，它会先将其清空再重新创建。
    // 这通常用于副本重新分配或需要完全清理旧数据状态的场景。
    public File createTabletDir(PhysicalTablePath tablePath, TableBucket tableBucket) {
        File tabletDir = getTabletDir(tablePath, tableBucket);

        // delete the tablet dir if exists
        FileUtils.deleteDirectoryQuietly(tabletDir);
        createTabletDirectory(tabletDir);
        return tabletDir;
    }

    public Optional<KvTablet> getKv(TableBucket tableBucket) {
        return Optional.ofNullable(currentKvs.get(tableBucket));
    }

    public void dropKv(TableBucket tableBucket) {
        KvTablet dropKvTablet =
                inLock(tabletCreationOrDeletionLock, () -> currentKvs.remove(tableBucket));

        if (dropKvTablet != null) {
            TablePath tablePath = dropKvTablet.getTablePath();
            try {
                dropKvTablet.drop();
                if (dropKvTablet.getPartitionName() == null) {
                    LOG.info(
                            "Deleted kv bucket {} for table {} in file path {}.",
                            tableBucket.getBucket(),
                            tablePath,
                            dropKvTablet.getKvTabletDir().getAbsolutePath());
                } else {
                    LOG.info(
                            "Deleted kv bucket {} for the partition {} of table {} in file path {}.",
                            tableBucket.getBucket(),
                            dropKvTablet.getPartitionName(),
                            tablePath,
                            dropKvTablet.getKvTabletDir().getAbsolutePath());
                }
            } catch (Exception e) {
                throw new KvStorageException(
                        String.format(
                                "Exception while deleting kv for table %s, bucket %s in dir %s.",
                                tablePath,
                                tableBucket.getBucket(),
                                dropKvTablet.getKvTabletDir().getAbsolutePath()),
                        e);
            }
        } else {
            LOG.warn("Fail to delete kv bucket {}.", tableBucket.getBucket());
        }
    }
    // 是 KvManager 在服务器启动或恢复过程中，从物理磁盘加载已有 KV 数据并将其转化为内存中活跃对象的关键方法。
    public KvTablet loadKv(File tabletDir, SchemaGetter schemaGetter) throws Exception {
        // 根据传入的文件夹（tabletDir）反推它属于哪张表、哪个分区以及哪个分桶（Bucket）
        Tuple2<PhysicalTablePath, TableBucket> pathAndBucket = FlussPaths.parseTabletDir(tabletDir);
        PhysicalTablePath physicalTablePath = pathAndBucket.f0;
        TableBucket tableBucket = pathAndBucket.f1;
        // get the log tablet for the kv tablet
        // 从 LogManager 中查找与该 KV 分桶对应的日志组件（Log）
        LogTablet logTablet =
                logManager
                        .getLog(tableBucket)
                        .orElseThrow(
                                () ->
                                        new IllegalStateException(
                                                String.format(
                                                        "Find a kv tablet for %s in dir %s to load, but can't find the log tablet for the bucket."
                                                                + " It is recommended to delete the dir %s to make the loading other kv tablets can success.",
                                                        tableBucket,
                                                        tabletDir.getAbsolutePath(),
                                                        tabletDir.getAbsolutePath())));

        // TODO: we should support recover schema from disk to decouple put and schema.
        TablePath tablePath = physicalTablePath.getTablePath();
        TableInfo tableInfo = getTableInfo(zkClient, tablePath);

        TableConfig tableConfig = tableInfo.getTableConfig();
        // 创建合并器与 KV 实例
        RowMerger rowMerger =
                RowMerger.create(tableConfig, tableConfig.getKvFormat(), schemaGetter);
        KvTablet kvTablet =
                KvTablet.create(
                        physicalTablePath,
                        tableBucket,
                        logTablet,
                        tabletDir,
                        conf,
                        serverMetricGroup,
                        arrowBufferAllocator,
                        memorySegmentPool,
                        tableConfig.getKvFormat(),
                        rowMerger,
                        tableConfig.getArrowCompressionInfo(),
                        schemaGetter,
                        tableConfig.getChangelogImage(),
                        sharedRocksDBRateLimiter);
        if (this.currentKvs.containsKey(tableBucket)) {
            throw new IllegalStateException(
                    String.format(
                            "Duplicate kv tablet directories for bucket %s are found in both %s and %s. "
                                    + "Recover server from this "
                                    + "failure by manually deleting one of the two kv directories for this bucket. "
                                    + "It is recommended to delete the bucket in the kv tablet directory that is "
                                    + "known to have failed recently.",
                            tableBucket,
                            tabletDir.getAbsolutePath(),
                            currentKvs.get(tableBucket).getKvTabletDir().getAbsolutePath()));
        }
        this.currentKvs.put(tableBucket, kvTablet);

        return kvTablet;
    }

    public void deleteRemoteKvSnapshot(
            PhysicalTablePath physicalTablePath, TableBucket tableBucket) {
        FsPath remoteKvTabletDir =
                FlussPaths.remoteKvTabletDir(remoteKvDir, physicalTablePath, tableBucket);
        try {
            if (remoteFileSystem.exists(remoteKvTabletDir)) {
                remoteFileSystem.delete(remoteKvTabletDir, true);
                LOG.info("Delete table's remote bucket snapshot dir of {} success.", tableBucket);
            }
        } catch (Exception e) {
            LOG.error(
                    "Delete table's remote bucket snapshot dir of {} failed.",
                    remoteKvTabletDir,
                    e);
        }
    }

    // ============ ServerReconfigurable Implementation ============

    @Override
    public void validate(Configuration newConfig) throws ConfigException {
        // Config validation is already handled by KvConfigValidator which is registered
        // on both CoordinatorServer and TabletServer. Here we only need to check runtime state.

        // Check if KvManager is in a valid state to accept reconfiguration
        if (isShutdown) {
            throw new ConfigException("Cannot reconfigure KvManager during shutdown");
        }

        // All config value validations are delegated to KvConfigValidator
        LOG.debug("KvManager runtime state validation passed for reconfiguration");
    }

    @Override
    public void reconfigure(Configuration newConfig) throws ConfigException {
        long newSharedRateLimitBytes =
                newConfig.get(ConfigOptions.KV_SHARED_RATE_LIMITER_BYTES_PER_SEC).getBytes();

        // If value hasn't changed, skip
        if (newSharedRateLimitBytes == currentSharedRateLimitBytesPerSec) {
            LOG.debug(
                    "Shared RocksDB rate limiter config unchanged: {} bytes/sec",
                    newSharedRateLimitBytes);
            return;
        }

        long oldValue = currentSharedRateLimitBytesPerSec;

        try {
            // Apply new configuration using RocksDB API (thread-safe)
            // The rate limiter is always enabled, so we can safely reconfigure it
            sharedRocksDBRateLimiter.setBytesPerSecond(newSharedRateLimitBytes);
            currentSharedRateLimitBytesPerSec = newSharedRateLimitBytes;

            LOG.info(
                    "Shared RocksDB rate limiter reconfigured: {} bytes/sec ({}) -> {} bytes/sec ({})",
                    oldValue,
                    new MemorySize(oldValue).toHumanReadableString(),
                    newSharedRateLimitBytes,
                    new MemorySize(newSharedRateLimitBytes).toHumanReadableString());

        } catch (Exception e) {
            // If setting fails, throw ConfigException to trigger rollback
            throw new ConfigException(
                    "Failed to reconfigure shared RocksDB rate limiter: " + e.getMessage(), e);
        }
    }
}
