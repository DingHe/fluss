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

package org.apache.fluss.server.log;

import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.FlussRuntimeException;
import org.apache.fluss.exception.LogStorageException;
import org.apache.fluss.exception.SchemaNotExistException;
import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.server.TabletManagerBase;
import org.apache.fluss.server.log.checkpoint.OffsetCheckpointFile;
import org.apache.fluss.server.metrics.group.TabletServerMetricGroup;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.utils.FileUtils;
import org.apache.fluss.utils.FlussPaths;
import org.apache.fluss.utils.MapUtils;
import org.apache.fluss.utils.clock.Clock;
import org.apache.fluss.utils.concurrent.Scheduler;
import org.apache.fluss.utils.types.Tuple2;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.locks.ReentrantLock;

import static org.apache.fluss.utils.concurrent.LockUtils.inLock;

/* This file is based on source code of Apache Kafka Project (https://kafka.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/**
 * The entry point to the fluss log management subsystem. The log manager is responsible for log
 * creation, retrieval, and cleaning. All read and write operations are delegated to the individual
 * log instances.
 */
// LogManager 是 Apache Fluss 存储子系统的核心组件，其设计深受 Apache Kafka 的影响。
// 它主要负责管理本地磁盘上所有日志分片（LogTablet）的生命周期，包括创建、加载、检索、删除以及故障恢复。
// 日志生命周期管理：负责在磁盘上创建新的日志目录，或删除已不再需要的日志。
// 启动恢复与加载：在服务器启动时，扫描数据目录，并发加载所有日志分片，并根据“干净关机”标识决定是否需要进行耗时的索引重建和日志恢复。
// 一致性保障：通过 Checkpoint 机制记录每个日志的 recovery-point（恢复点），确保在崩溃后能从正确的位置恢复数据。
@ThreadSafe
public final class LogManager extends TabletManagerBase {
    private static final Logger LOG = LoggerFactory.getLogger(LogManager.class);
    // 记录恢复点位偏移量的文件名。
    @VisibleForTesting
    static final String RECOVERY_POINT_CHECKPOINT_FILE = "recovery-point-offset-checkpoint";

    /**
     * Clean shutdown file that indicates the tabletServer was cleanly shutdown in v0.7 and higher.
     * This is used to avoid unnecessary recovery operations after a clean shutdown like recovery
     * writer snapshot by scan all logs during loadLogs.
     *
     * <p>Note: for the previous cluster deploy by v0.6 and lower, there's no this file, we default
     * think its unclean shutdown.
     */
    // 存在此文件表示上次是正常关闭，启动时可跳过某些恢复步骤。
    static final String CLEAN_SHUTDOWN_FILE = ".fluss_cleanshutdown";
    // 用于从 ZooKeeper 获取表的元数据（如 Schema）
    private final ZooKeeperClient zkClient;
    // 调度器，用于执行后台任务（如日志清理、刷新）。
    private final Scheduler scheduler;
    // 时间基准，用于记录日志段的时间戳。
    private final Clock clock;
    // 监控指标组，记录日志相关的性能指标。
    private final TabletServerMetricGroup serverMetricGroup;
    // 独占锁。确保创建或删除日志操作的线程安全。
    private final ReentrantLock logCreationOrDeletionLock = new ReentrantLock();
    // 维护当前节点管理的所有 TableBucket 及其对应的 LogTablet 实例。
    private final Map<TableBucket, LogTablet> currentLogs = MapUtils.newConcurrentHashMap();
    // 负责将内存中的恢复点偏移量持久化到磁盘文件
    private volatile OffsetCheckpointFile recoveryPointCheckpoint;
    // 标识启动时的日志加载是否已全部完成，用于决定关机时是否写干净关机标识。
    private boolean loadLogsCompletedFlag = false;

    private LogManager(
            File dataDir,
            Configuration conf,
            ZooKeeperClient zkClient,
            int recoveryThreadsPerDataDir,
            Scheduler scheduler,
            Clock clock,
            TabletServerMetricGroup serverMetricGroup)
            throws Exception {
        super(TabletType.LOG, dataDir, conf, recoveryThreadsPerDataDir);
        this.zkClient = zkClient;
        this.scheduler = scheduler;
        this.clock = clock;
        this.serverMetricGroup = serverMetricGroup;
        createAndValidateDataDir(dataDir);

        initializeCheckpointMaps();
    }

    public static LogManager create(
            Configuration conf,
            ZooKeeperClient zkClient,
            Scheduler scheduler,
            Clock clock,
            TabletServerMetricGroup serverMetricGroup)
            throws Exception {
        String dataDirString = conf.getString(ConfigOptions.DATA_DIR);
        File dataDir = new File(dataDirString).getAbsoluteFile();
        return new LogManager(
                dataDir,
                conf,
                zkClient,
                conf.getInt(ConfigOptions.NETTY_SERVER_NUM_WORKER_THREADS),
                scheduler,
                clock,
                serverMetricGroup);
    }

    public void startup() {
        loadLogs();

        // TODO add more scheduler, like log-flusher etc.
    }

    public File getDataDir() {
        return dataDir;
    }

    private void initializeCheckpointMaps() throws IOException {
        // 实例化一个位点检查点对象，专门用于管理“恢复点”（Recovery Point）
        // 通过这个对象，LogManager 可以记录每个分桶（Bucket）已经成功刷新（Flush）到磁盘的最大位点。
        recoveryPointCheckpoint =
                new OffsetCheckpointFile(new File(dataDir, RECOVERY_POINT_CHECKPOINT_FILE));
    }

    /** Recover and load all logs in the given data directories. */
    // 负责从磁盘恢复并加载所有分桶（Tablet/Bucket）的日志数据。
    // 它不仅要读取数据，还要根据上次关闭的状态决定是否需要执行复杂的日志修复（Recovery）。

    private void loadLogs() {
        // 当前正在加载的目录路径，并将路径转为绝对路径字符串，方便后续在日志和异常中使用。
        LOG.info("Loading logs from dir {}", dataDir);

        String dataDirAbsolutePath = dataDir.getAbsolutePath();
        try {
            boolean isCleanShutdown = false;
            // 判定“优雅停机”状态（Clean Shutdown）
            File cleanShutdownFile = new File(dataDir, CLEAN_SHUTDOWN_FILE);
            if (cleanShutdownFile.exists()) {
                // Cache the clean shutdown status marker and use that for rest of log loading
                // workflow. Delete the CleanShutdownFile so that if tabletServer crashes while
                // loading the log, it is considered hard shutdown during the next boot up.
                Files.deleteIfExists(cleanShutdownFile.toPath());
                isCleanShutdown = true;
            }

            Map<TableBucket, Long> recoveryPoints = new HashMap<>();
            try {
                recoveryPoints = recoveryPointCheckpoint.read();
            } catch (Exception e) {
                LOG.warn(
                        "Error occurred while reading recovery-point-offset-checkpoint file of directory {}, "
                                + "resetting the recovery checkpoint to 0",
                        dataDirAbsolutePath,
                        e);
            }
            // 扫描待加载的 Tablet（分桶）
            List<File> tabletsToLoad = listTabletsToLoad();
            if (tabletsToLoad.isEmpty()) {
                LOG.info("No logs found to be loaded in {}", dataDirAbsolutePath);
            } else if (isCleanShutdown) {
                LOG.info("Skipping some recovery log process since clean shutdown file was found");
            } else {
                LOG.info("Recovering all local logs since no clean shutdown file was not found");
            }

            final Map<TableBucket, Long> finalRecoveryPoints = recoveryPoints;
            final boolean cleanShutdown = isCleanShutdown;
            // set runnable job.
            // createLogLoadingJobs 将每个分桶的加载逻辑封装成一个 Runnable 任务。
            // 每个任务内部会根据 cleanShutdown 标志决定是直接打开文件，还是扫描 .log 文件来重建索引。
            Runnable[] jobsForDir =
                    createLogLoadingJobs(
                            tabletsToLoad, cleanShutdown, finalRecoveryPoints, conf, clock);

            long startTime = System.currentTimeMillis();
            // 调用 runInThreadPool 并行执行所有加载任务。
            int successLoadCount =
                    runInThreadPool(jobsForDir, "log-recovery-" + dataDirAbsolutePath);

            loadLogsCompletedFlag = true;
            LOG.info(
                    "log loader complete. Total success loaded log count is {}, Take {} ms",
                    successLoadCount,
                    System.currentTimeMillis() - startTime);
        } catch (Throwable e) {
            throw new FlussRuntimeException("Failed to recovery log", e);
        }
    }

    /**
     * Get or create log tablet for a given bucket of a table. If the log already exists, just
     * return a copy of the existing log. Otherwise, create a log for the given table and the given
     * bucket.
     *
     * @param tablePath the table path of the bucket belongs to
     * @param tableBucket the table bucket
     * @param logFormat the log format
     * @param tieredLogLocalSegments the number of segments to retain in local for tiered log
     * @param isChangelog whether the log is a changelog of primary key table
     */
    public LogTablet getOrCreateLog(
            PhysicalTablePath tablePath,
            TableBucket tableBucket,
            LogFormat logFormat,
            int tieredLogLocalSegments,
            boolean isChangelog)
            throws Exception {
        return inLock(
                logCreationOrDeletionLock,
                () -> {
                    if (currentLogs.containsKey(tableBucket)) {
                        return currentLogs.get(tableBucket);
                    }

                    File tabletDir = getOrCreateTabletDir(tablePath, tableBucket);

                    LogTablet logTablet =
                            LogTablet.create(
                                    tablePath,
                                    tabletDir,
                                    conf,
                                    serverMetricGroup,
                                    0L,
                                    scheduler,
                                    logFormat,
                                    tieredLogLocalSegments,
                                    isChangelog,
                                    clock,
                                    true);
                    currentLogs.put(tableBucket, logTablet);

                    LOG.info(
                            "Loaded log for bucket {} in dir {}",
                            tableBucket,
                            tabletDir.getAbsolutePath());

                    return logTablet;
                });
    }

    public Optional<LogTablet> getLog(TableBucket tableBucket) {
        return Optional.ofNullable(currentLogs.get(tableBucket));
    }

    public void dropLog(TableBucket tableBucket) {
        LogTablet dropLogTablet =
                inLock(logCreationOrDeletionLock, () -> currentLogs.remove(tableBucket));

        if (dropLogTablet != null) {
            TablePath tablePath = dropLogTablet.getTablePath();
            try {
                dropLogTablet.drop();
                if (dropLogTablet.getPartitionName() == null) {
                    LOG.info(
                            "Deleted log bucket {} for table {} in file path {}.",
                            tableBucket.getBucket(),
                            tablePath,
                            dropLogTablet.getLogDir().getAbsolutePath());
                } else {
                    LOG.info(
                            "Deleted log bucket {} for the partition {} of table {} in file path {}.",
                            tableBucket.getBucket(),
                            dropLogTablet.getPartitionName(),
                            tablePath,
                            dropLogTablet.getLogDir().getAbsolutePath());
                }
            } catch (Exception e) {
                throw new LogStorageException(
                        String.format(
                                "Error while deleting log for table %s, bucket %s in dir %s: %s",
                                tablePath,
                                tableBucket.getBucket(),
                                dropLogTablet.getLogDir().getAbsolutePath(),
                                e.getMessage()),
                        e);
            }
        } else {
            throw new LogStorageException(
                    String.format(
                            "Failed to delete log bucket %s as it does not exist.",
                            tableBucket.getBucket()));
        }
    }

    /**
     * Truncate the bucket's logs to the specified offsets and checkpoint the recovery point to this
     * offset.
     */
    public void truncateTo(TableBucket tableBucket, long offset) throws LogStorageException {
        LogTablet logTablet = currentLogs.get(tableBucket);
        // If the log tablet does not exist, skip it.
        if (logTablet != null && logTablet.truncateTo(offset)) {
            checkpointRecoveryOffsets();
        }
    }

    public void truncateFullyAndStartAt(TableBucket tableBucket, long newOffset) {
        LogTablet logTablet = currentLogs.get(tableBucket);
        // If the log tablet does not exist, skip it.
        if (logTablet != null) {
            logTablet.truncateFullyAndStartAt(newOffset);
            checkpointRecoveryOffsets();
        }
    }
    // LogManager 中执行单个分桶（Tablet/Bucket）加载的核心业务逻辑。
    // 它完成了从物理路径解析到内存对象构建，再到状态一致性校验的全过程。
    private LogTablet loadLog(
            File tabletDir,
            boolean isCleanShutdown,
            Map<TableBucket, Long> recoveryPoints,
            Configuration conf,
            Clock clock)
            throws Exception {
        // 从文件夹名称中提取逻辑信息。
        Tuple2<PhysicalTablePath, TableBucket> pathAndBucket = FlussPaths.parseTabletDir(tabletDir);
        TableBucket tableBucket = pathAndBucket.f1;
        // 获取该分桶在磁盘上记录的安全位点。
        long logRecoveryPoint = recoveryPoints.getOrDefault(tableBucket, 0L);

        PhysicalTablePath physicalTablePath = pathAndBucket.f0;
        TablePath tablePath = physicalTablePath.getTablePath();
        // 为了正确加载日志，必须知道表的配置（如：是否是主键表、日志格式等）。
        // getTableInfo 会连接元数据中心（ZooKeeper）获取 TableInfo
        TableInfo tableInfo = getTableInfo(zkClient, tablePath);
        LogTablet logTablet =
                LogTablet.create(
                        physicalTablePath,
                        tabletDir,
                        conf,
                        serverMetricGroup,
                        logRecoveryPoint,
                        scheduler,
                        tableInfo.getTableConfig().getLogFormat(),
                        tableInfo.getTableConfig().getTieredLogLocalSegments(),
                        tableInfo.hasPrimaryKey(),
                        clock,
                        isCleanShutdown);

        if (currentLogs.containsKey(tableBucket)) {
            throw new IllegalStateException(
                    String.format(
                            "Duplicate log tablet directories for bucket %s are found in both %s and %s. "
                                    + "It is likely because tablet directory failure happened while server was "
                                    + "replacing current replica with future replica. Recover server from this "
                                    + "failure by manually deleting one of the two log directories for this bucket. "
                                    + "It is recommended to delete the bucket in the log tablet directory that is "
                                    + "known to have failed recently.",
                            tableBucket,
                            tabletDir.getAbsolutePath(),
                            currentLogs.get(tableBucket).getLogDir().getAbsolutePath()));
        }
        currentLogs.put(tableBucket, logTablet);

        return logTablet;
    }
    // 核心作用是初始化并校验存储日志数据的物理目录。
    // 在分布式存储引擎中，确保底层文件系统的健康和持久化是至关重要的。
    private void createAndValidateDataDir(File dataDir) {
        try {
            inLock(
                    logCreationOrDeletionLock,
                    () -> {
                        // 检查目录是否存在
                        if (!dataDir.exists()) {
                            LOG.info(
                                    "Data directory {} not found, creating it.",
                                    dataDir.getAbsolutePath());
                            // 执行创建动作
                            boolean created = dataDir.mkdirs();
                            if (!created) {
                                throw new IOException(
                                        "Failed to create data directory "
                                                + dataDir.getAbsolutePath());
                            }
                            Path parentPath =
                                    dataDir.toPath().toAbsolutePath().normalize().getParent();
                            // 确保目录创建操作在硬件层面落盘。
                            FileUtils.flushDir(parentPath);
                        }
                        if (!dataDir.isDirectory() || !dataDir.canRead()) {
                            throw new IOException(
                                    dataDir.getAbsolutePath()
                                            + " is not a readable data directory.");
                        }
                    });
        } catch (IOException e) {
            throw new FlussRuntimeException(
                    "Failed to create or validate data directory " + dataDir.getAbsolutePath(), e);
        }
    }

    /** Close all the logs. */
    public void shutdown() {
        LOG.info("Shutting down LogManager.");

        String dataDirAbsolutePath = dataDir.getAbsolutePath();
        ExecutorService pool = createThreadPool("log-tablet-closing-" + dataDirAbsolutePath);

        List<LogTablet> logs = new ArrayList<>(currentLogs.values());
        List<Future<?>> jobsForTabletDir = new ArrayList<>();
        for (LogTablet logTablet : logs) {
            Runnable runnable =
                    () -> {
                        try {
                            logTablet.flush(true);
                            logTablet.close();
                        } catch (IOException e) {
                            throw new FlussRuntimeException(e);
                        }
                    };
            jobsForTabletDir.add(pool.submit(runnable));
        }

        try {
            for (Future<?> future : jobsForTabletDir) {
                try {
                    future.get();
                } catch (InterruptedException e) {
                    LOG.warn("Interrupted while shutting down LogManager.");
                } catch (ExecutionException e) {
                    LOG.warn(
                            "There was an error in one of the threads during LogManager shutdown",
                            e);
                }
            }

            // update the last flush point.
            checkpointRecoveryOffsets();

            // mark that the shutdown was clean by creating marker file for log dirs that all logs
            // have been recovered at startup time.
            if (loadLogsCompletedFlag) {
                LOG.debug("Writing clean shutdown marker.");
                try {
                    Files.createFile(new File(dataDir, CLEAN_SHUTDOWN_FILE).toPath());
                } catch (IOException e) {
                    LOG.warn("Failed to write clean shutdown marker.", e);
                }
            }
        } finally {
            pool.shutdown();
        }

        LOG.info("Shut down LogManager complete.");
    }

    /** Create runnable jobs for loading logs from tablet directories. */
    // 主要是为了将待加载的目录转化为可并行执行的任务单元。
    private Runnable[] createLogLoadingJobs(
            List<File> tabletsToLoad, // 待加载的 Tablet（分桶）目录列表
            boolean cleanShutdown, // 是否为正常关机标识
            Map<TableBucket, Long> recoveryPoints, // 从检查点文件读取的恢复点映射
            Configuration conf, // 集群配置信息
            Clock clock) { // 系统时钟，用于记录日志时间
        Runnable[] jobs = new Runnable[tabletsToLoad.size()];
        for (int i = 0; i < tabletsToLoad.size(); i++) {
            final File tabletDir = tabletsToLoad.get(i);
            jobs[i] = createLogLoadingJob(tabletDir, cleanShutdown, recoveryPoints, conf, clock);
        }
        return jobs;
    }

    /** Create a runnable job for loading log from a single tablet directory. */
    // 它为**单个 Tablet（分桶）创建具体的加载任务。
    // 它的特别之处在于：除了正常的加载逻辑，它还包含了对残余数据（已删除表的遗留数据）**的清理逻辑。
    private Runnable createLogLoadingJob(
            File tabletDir,
            boolean cleanShutdown,
            Map<TableBucket, Long> recoveryPoints,
            Configuration conf,
            Clock clock) {
        return new Runnable() {
            @Override
            public void run() {
                LOG.debug("Loading log {}", tabletDir);
                try {
                    loadLog(tabletDir, cleanShutdown, recoveryPoints, conf, clock);
                } catch (Exception e) {
                    LOG.error("Fail to loadLog from {}", tabletDir, e);
                    if (e instanceof SchemaNotExistException) {
                        LOG.error(
                                "schema not exist, table for {} has already been dropped, the residual data will be removed.",
                                tabletDir,
                                e);
                        FileUtils.deleteDirectoryQuietly(tabletDir);

                        // Also delete corresponding KV tablet directory if it exists
                        try {
                            Tuple2<PhysicalTablePath, TableBucket> pathAndBucket =
                                    FlussPaths.parseTabletDir(tabletDir);
                            File kvTabletDir =
                                    FlussPaths.kvTabletDir(
                                            dataDir, pathAndBucket.f0, pathAndBucket.f1);
                            if (kvTabletDir.exists()) {
                                LOG.info(
                                        "Also removing corresponding KV tablet directory: {}",
                                        kvTabletDir);
                                FileUtils.deleteDirectoryQuietly(kvTabletDir);
                            }
                        } catch (Exception kvDeleteException) {
                            LOG.warn(
                                    "Failed to delete corresponding KV tablet directory for log {}: {}",
                                    tabletDir,
                                    kvDeleteException.getMessage());
                        }
                        return;
                    }
                    throw new FlussRuntimeException(e);
                }
            }
        };
    }
    // 核心作用是将内存中所有分桶（LogTablet）当前的“恢复位点”（Recovery Point）持久化到磁盘上的检查点文件中。
    // 这就像是给数据库做“存档”，确保系统如果现在突然断电，重启后知道哪些数据已经安全刷盘了。
    @VisibleForTesting
    void checkpointRecoveryOffsets() {
        // Assuming TableBucket and LogTablet are actual types used in your application
        if (recoveryPointCheckpoint != null) {
            try {
                // 创建临时 Map：新建一个 HashMap 用于存放所有分桶及其对应的位点。
                Map<TableBucket, Long> recoveryOffsets = new HashMap<>();
                for (Map.Entry<TableBucket, LogTablet> entry : currentLogs.entrySet()) {
                    recoveryOffsets.put(entry.getKey(), entry.getValue().getRecoveryPoint());
                }
                recoveryPointCheckpoint.write(recoveryOffsets);
            } catch (Exception e) {
                throw new LogStorageException(
                        "Disk error while writing recovery offsets checkpoint in directory "
                                + dataDir
                                + ": "
                                + e.getMessage(),
                        e);
            }
        }
    }

}
