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

package org.apache.fluss.server;

import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.KvStorageException;
import org.apache.fluss.exception.LogStorageException;
import org.apache.fluss.exception.SchemaNotExistException;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.SchemaInfo;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.server.kv.KvManager;
import org.apache.fluss.server.log.LogManager;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.data.TableRegistration;
import org.apache.fluss.utils.FileUtils;
import org.apache.fluss.utils.FlussPaths;
import org.apache.fluss.utils.concurrent.ExecutorThreadFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

import static org.apache.fluss.utils.FlussPaths.KV_TABLET_DIR_PREFIX;
import static org.apache.fluss.utils.FlussPaths.LOG_TABLET_DIR_PREFIX;
import static org.apache.fluss.utils.FlussPaths.isPartitionDir;

/**
 * A base class for {@link LogManager} {@link KvManager} which provide a common logic for both of
 * them.
 */
// 为日志管理器（LogManager）和键值管理器（KvManager）提供了通用的目录管理、线程池调度以及元数据获取逻辑。
// 在 Fluss 的架构中，数据被划分为多个分片（Tablet）。无论这些分片是存储顺序日志（Log）还是 LSM 树结构（KV），它们在物理磁盘上的组织方式和初始化流程都有很多相似之处，这就是该类的存在意义。
// 统一存储规范：定义了物理磁盘上数据库、表、分区及分片（Tablet）的目录层级扫描逻辑。
// 并发任务调度：提供了一套在多个分片上并行执行任务（如启动时的副本恢复）的通用机制。
// 生命周期锁控：通过共享锁管理 Tablet 的创建与删除，防止并发操作导致文件系统冲突。
// 元数据衔接：充当存储层与服务协调层（ZooKeeper）之间的桥梁，负责加载表结构信息。

public abstract class TabletManagerBase {

    private static final Logger LOG = LoggerFactory.getLogger(TabletManagerBase.class);

    /** The enum for the tablet type. */
    // 定义了两种 Tablet 类型：LOG（日志型）和 KV（键值型）
    public enum TabletType {
        LOG,
        KV
    }
    // 存储的基础根目录，所有数据都存放在此路径下。
    protected final File dataDir;
    // Fluss 的配置对象，用于获取系统级参数。
    protected final Configuration conf;
    // 个重入锁，用于保证在创建或删除 Tablet 物理目录时的线程安全。
    protected final Lock tabletCreationOrDeletionLock = new ReentrantLock();

    // TODO make this parameter configurable.
    // 指定用于并行执行任务（如加载/修复分片数据）的线程数量
    private final int recoveryThreads;
    // 当前管理器的具体类型（LOG 或 KV）
    private final TabletType tabletType;
    // 目录前缀，LOG 默认为 log-，KV 默认为 kv-，用于在文件系统中过滤不同的存储引擎。
    private final String tabletDirPrefix;

    public TabletManagerBase(
            TabletType tabletType, File dataDir, Configuration conf, int recoveryThreads) {
        this.tabletType = tabletType;
        this.tabletDirPrefix = getTabletDirPrefix(tabletType);
        this.dataDir = dataDir;
        this.conf = conf;
        this.recoveryThreads = recoveryThreads;
    }

    /**
     * Return the directories of the tablets to be loaded.
     *
     * <p>See more about the local directory contracts: {@link FlussPaths#logTabletDir(File,
     * PhysicalTablePath, TableBucket)} and {@link FlussPaths#kvTabletDir(File, PhysicalTablePath,
     * TableBucket)}.
     */
    // 核心功能是扫描服务器的本地文件系统，识别并列出所有需要被加载的 Tablet（存储分片）目录。
    // Fluss 的存储目录结构遵循特定的层级：根目录(dataDir) / 数据库(db) / 表(table) / [分区(partition)] / Tablet目录。
    protected List<File> listTabletsToLoad() {
        List<File> tabletsToLoad = new ArrayList<>();
        // Get all database directory.
        // 根目录下的第一层子目录代表不同的数据库 (Database)。
        File[] dbDirs = FileUtils.listDirectories(dataDir);
        for (File dbDir : dbDirs) {
            // Get all table path directory.
            File[] tableDirs = FileUtils.listDirectories(dbDir);
            for (File tableDir : tableDirs) {
                // maybe tablet directories or partition directories
                // 在表目录下扫描子目录。
                // 由于 Fluss 支持分区表，这一层的文件夹可能直接是 Tablet 目录（非分区表），也可能是分区目录（分区表）。
                File[] tabletOrPartitionDirs = FileUtils.listDirectories(tableDir);

                List<File> tabletDirs = new ArrayList<>();
                for (File tabletOrPartitionDir : tabletOrPartitionDirs) {
                    // if not partition dir, consider it as a tablet dir
                    // 如果不是分区目录，则认为它就是一个潜在的 Tablet 目录
                    if (!isPartitionDir(tabletOrPartitionDir.getName())) {
                        tabletDirs.add(tabletOrPartitionDir);
                    } else {
                        // consider all dirs in partition as tablet dirs
                        // 如果是分区目录，则进入该目录内部，将其下的所有子目录视为潜在的 Tablet 目录
                        tabletDirs.addAll(
                                Arrays.asList(FileUtils.listDirectories(tabletOrPartitionDir)));
                    }
                }

                // it may contain the directory for kv tablet and log tablet
                // filter out the directory for specific type tablet
                // actually it identified by the prefix of the directory
                tabletsToLoad.addAll(
                        tabletDirs.stream()
                                .filter(
                                        tabletDir ->
                                                tabletDir.getName().startsWith(tabletDirPrefix))
                                .collect(Collectors.toList()));
            }
        }

        return tabletsToLoad;
    }

    protected ExecutorService createThreadPool(String poolName) {
        return Executors.newFixedThreadPool(recoveryThreads, new ExecutorThreadFactory(poolName));
    }

    /** Running a series of jobs in a thread pool, and return the count of the successful job. */
    // 在 Apache Fluss 中，它通常被用来并发地加载大量 Tablet（分片）、执行日志恢复（Recovery）或进行状态检查，从而显著缩短系统启动或故障恢复的时间。
    protected int runInThreadPool(Runnable[] runnableJobs, String poolName) throws Throwable {
        // 创建一个列表，用于存储每个任务提交后的 Future 对象。
        List<Future<?>> jobsForTabletDir = new ArrayList<>();
        ExecutorService pool = createThreadPool(poolName);
        // 遍历传入的所有任务（runnableJobs），通过 pool.submit() 将它们扔进线程池异步执行。
        for (Runnable runnable : runnableJobs) {
            jobsForTabletDir.add(pool.submit(runnable));
        }
        int successCount = 0;
        try {
            for (Future<?> future : jobsForTabletDir) {
                try {
                    future.get();
                    successCount++;
                } catch (InterruptedException | ExecutionException e) {
                    throw e.getCause();
                }
            }
        } finally {
            pool.shutdown();
        }
        return successCount;
    }

    /**
     * Get the tablet directory with given directory name for the given table path and table bucket.
     *
     * <p>When the parent directory of the tablet directory is missing, it will create the
     * directory.
     *
     * @param tablePath the table path of the bucket
     * @param tableBucket the table bucket
     * @return the tablet directory
     */
    // 确保在文件系统中为特定的分片（Tablet）准备好物理目录。如果目录已经存在则直接使用，不存在则自动创建。
    protected File getOrCreateTabletDir(PhysicalTablePath tablePath, TableBucket tableBucket) {
        // 根据传入的物理表路径（tablePath）和桶信息（tableBucket），通过调用同类中的 getTabletDir 方法，推算出该 Tablet 在磁盘上的绝对路径。
        File tabletDir = getTabletDir(tablePath, tableBucket);
        if (tabletDir.exists()) {
            return tabletDir;
        }
        createTabletDirectory(tabletDir);
        return tabletDir;
    }

    public Path getTabletParentDir(PhysicalTablePath tablePath, TableBucket tableBucket) {
        return getTabletDir(tablePath, tableBucket).toPath().getParent();
    }

    protected File getTabletDir(PhysicalTablePath tablePath, TableBucket tableBucket) {
        switch (tabletType) {
            case LOG:
                return FlussPaths.logTabletDir(dataDir, tablePath, tableBucket);
            case KV:
                return FlussPaths.kvTabletDir(dataDir, tablePath, tableBucket);
            default:
                throw new IllegalArgumentException("Unknown tablet type: " + tabletType);
        }
    }

    // TODO: we should support get table info from local properties file instead of from zk
    // 主要职责是从 ZooKeeper (ZK) 中获取并组装一张表的完整元数据信息。
    // 在分布式系统 Fluss 中，存储层需要知道表的结构（Schema）和配置（Registration）才能正确处理数据。
    public static TableInfo getTableInfo(ZooKeeperClient zkClient, TablePath tablePath)
            throws Exception {
        // 通过 ZK 客户端查询该表路径下当前正在使用的 Schema 版本号
        int schemaId = zkClient.getCurrentSchemaId(tablePath);
        // 根据上面拿到的 schemaId，再次从 ZK 中读取具体的 Schema 详情（例如包含哪些列、数据类型等）
        Optional<SchemaInfo> schemaInfoOpt = zkClient.getSchemaById(tablePath, schemaId);
        SchemaInfo schemaInfo;
        if (!schemaInfoOpt.isPresent()) {
            throw new SchemaNotExistException(
                    String.format(
                            "Failed to load table '%s': Table schema not found in zookeeper metadata.",
                            tablePath));
        } else {
            schemaInfo = schemaInfoOpt.get();
        }
        // 从 ZK 获取表的注册静态信息。
        TableRegistration tableRegistration =
                zkClient.getTable(tablePath)
                        .orElseThrow(
                                () ->
                                        new LogStorageException(
                                                String.format(
                                                        "Failed to load table '%s': table info not found in zookeeper metadata.",
                                                        tablePath)));
        // 对象转换与整合。
        return tableRegistration.toTableInfo(tablePath, schemaInfo);
    }

    /** Create a tablet directory in the given dir. */
    // 创建文件目录
    protected void createTabletDirectory(File tabletDir) {
        try {
            Files.createDirectories(tabletDir.toPath());
        } catch (IOException e) {
            String errorMsg =
                    String.format(
                            "Failed to create directory %s for %s tablet.",
                            tabletDir.toPath(), tabletType);
            LOG.error(errorMsg, e);
            if (tabletType == TabletType.KV) {
                throw new KvStorageException(errorMsg, e);
            } else {
                throw new LogStorageException(errorMsg, e);
            }
        }
    }

    private static String getTabletDirPrefix(TabletType tabletType) {
        switch (tabletType) {
            case LOG:
                return LOG_TABLET_DIR_PREFIX;
            case KV:
                return KV_TABLET_DIR_PREFIX;
            default:
                throw new IllegalArgumentException("Unknown tablet type: " + tabletType);
        }
    }
}
