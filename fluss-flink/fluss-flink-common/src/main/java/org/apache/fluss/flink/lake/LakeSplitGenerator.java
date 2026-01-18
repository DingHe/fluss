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

package org.apache.fluss.flink.lake;

import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.metadata.LakeSnapshot;
import org.apache.fluss.exception.LakeTableSnapshotNotExistException;
import org.apache.fluss.flink.lake.split.LakeSnapshotAndFlussLogSplit;
import org.apache.fluss.flink.lake.split.LakeSnapshotSplit;
import org.apache.fluss.flink.source.enumerator.initializer.OffsetsInitializer;
import org.apache.fluss.flink.source.split.LogSplit;
import org.apache.fluss.flink.source.split.SourceSplitBase;
import org.apache.fluss.lake.source.LakeSource;
import org.apache.fluss.lake.source.LakeSplit;
import org.apache.fluss.metadata.PartitionInfo;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.utils.ExceptionUtils;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.fluss.client.table.scanner.log.LogScanner.EARLIEST_OFFSET;
import static org.apache.fluss.flink.source.split.LogSplit.NO_STOPPING_OFFSET;
import static org.apache.fluss.metadata.ResolvedPartitionSpec.PARTITION_SPEC_SEPARATOR;

/** A generator for lake splits. */
// Apache Fluss 在 Flink 集成层中的一个核心组件。它扮演着“分片编排者”的角色，专门负责处理**湖仓一体（Lakehouse）**场景下的数据分片生成。
// 该类的主要作用是根据“数据湖”和“Fluss 日志”的元数据，计算并生成一套混合分片（Hybrid Splits）。
// 在 Fluss 架构中，旧数据通常归档在对象存储（湖）中，而最新数据存储在 Fluss Log 节点（仓）中。该类的核心逻辑在于：
// 桥接湖与仓：找到湖中最后一个快照（Snapshot）对应的 Fluss Log 偏移量（Offset）。
// 保证数据连续性：确保 Flink Reader 读完湖文件后，能准确地从正确的 Offset 开始接续消费增量日志，不重不漏。
// 适配复杂场景：同时支持主键表（PK Table）和日志表（Log Table），以及分区表和非分区表。

public class LakeSplitGenerator {
    // 存储表的元数据，如表 ID、Schema、是否分区、是否为主键表等。
    private final TableInfo tableInfo;
    // Fluss 管理客户端，用于从服务端获取最新的湖快照（LakeSnapshot）信息。
    private final Admin flussAdmin;
    // 内部工具，用于查询特定 Bucket 在 Fluss Log 中的具体 Offset（如 Earliest, Latest）。
    private final OffsetsInitializer.BucketOffsetsRetriever bucketOffsetsRetriever;
    // 定义读取任务在何处停止。批模式下会有确定的终点，流模式下通常为无终点。
    private final OffsetsInitializer stoppingOffsetInitializer;
    // 当前表的桶（Bucket）总数，用于遍历所有数据分布通道。
    private final int bucketCount;
    // 动态获取当前表的所有分区元数据。
    private final Supplier<Set<PartitionInfo>> listPartitionSupplier;
    // 湖存储的接入源（如对接 Paimon 或 Hudi），负责规划湖文件的物理分片（Plan Splits）。
    private final LakeSource<LakeSplit> lakeSource;

    public LakeSplitGenerator(
            TableInfo tableInfo,
            Admin flussAdmin,
            LakeSource<LakeSplit> lakeSource,
            OffsetsInitializer.BucketOffsetsRetriever bucketOffsetsRetriever,
            OffsetsInitializer stoppingOffsetInitializer,
            int bucketCount,
            Supplier<Set<PartitionInfo>> listPartitionSupplier) {
        this.tableInfo = tableInfo;
        this.flussAdmin = flussAdmin;
        this.lakeSource = lakeSource;
        this.bucketOffsetsRetriever = bucketOffsetsRetriever;
        this.stoppingOffsetInitializer = stoppingOffsetInitializer;
        this.bucketCount = bucketCount;
        this.listPartitionSupplier = listPartitionSupplier;
    }

    /**
     * Return A list of hybrid lake snapshot {@link LakeSnapshotSplit}, {@link
     * LakeSnapshotAndFlussLogSplit} and the corresponding Fluss {@link LogSplit} based on the lake
     * snapshot. Return null if no lake snapshot exists.
     */
    // 主要任务是：找到数据湖中最新的快照，并计算该快照与 Fluss Log 之间的衔接点，从而生成一套完整的分片（Splits）方案。
    @Nullable
    public List<SourceSplitBase> generateHybridLakeFlussSplits() throws Exception {
        LakeSnapshot lakeSnapshotInfo;
        try {
            // 调用 Fluss Admin 客户端，从服务端获取该表当前最新的湖快照（Latest Lake Snapshot）
            // .get() 表示这是一个异步转同步的操作，获取快照的元数据（包含 Snapshot ID 和对应的 Offset）
            lakeSnapshotInfo = flussAdmin.getLatestLakeSnapshot(tableInfo.getTablePath()).get();
        } catch (Exception exception) {
            if (ExceptionUtils.stripExecutionException(exception)
                    instanceof LakeTableSnapshotNotExistException) {
                // 如果湖里根本没数据（没有快照），则返回 null，调用方会降级到纯 Log 读取模式
                return null;
            }
            throw exception;
        }
        // 判断是否为日志表（Log Table）：如果没有定义主键，则视为日志表；否则为主键表
        boolean isLogTable = !tableInfo.hasPrimaryKey();
        // 判断是否为分区表
        boolean isPartitioned = tableInfo.isPartitioned();
        // 这段代码负责扫描数据湖存储（如 S3/OSS 上的 Parquet 文件）
        Map<String, Map<Integer, List<LakeSplit>>> lakeSplits =
                groupLakeSplits( // 将平铺的分片按 [分区 -> 桶] 的层级进行分组
                        lakeSource
                                .createPlanner(
                                        (LakeSource.PlannerContext) lakeSnapshotInfo::getSnapshotId) // 传入快照 ID
                                .plan()); // 执行规划，返回湖中物理文件的分片列表

        // 从快照元数据中提取 TableBucket 到 Long Offset 的映射
        // 这代表了：当湖快照生成的那一刻，每个 Bucket 在 Fluss Log 中对应的消费位置
        Map<TableBucket, Long> tableBucketsOffset = lakeSnapshotInfo.getTableBucketsOffset();
        if (isPartitioned) {
            // 1. 调用 Supplier 获取当前 Fluss 中所有的分区信息
            Set<PartitionInfo> partitionInfos = listPartitionSupplier.get();
            // 2. 将分区信息转换为 ID -> Name 的映射表，方便后续按名称匹配
            Map<Long, String> partitionNameById =
                    partitionInfos.stream()
                            .collect(
                                    Collectors.toMap(
                                            PartitionInfo::getPartitionId,
                                            PartitionInfo::getPartitionName));
            // 3. 调用专门的方法生成分区表的混合分片
            return generatePartitionTableSplit(
                    lakeSplits, isLogTable, tableBucketsOffset, partitionNameById);
        } else {
            //场景 B：非分区表
            // 1. 对于非分区表，lakeSplits Map 中只会有一个 entry
            // 取出该 entry 的值（即 [Bucket ID -> LakeSplit 列表] 的映射）
            Map<Integer, List<LakeSplit>> nonPartitionLakeSplits =
                    lakeSplits.values().iterator().next();
            // non-partitioned table
            // 2. 调用专门的方法生成非分区表的混合分片
            return generateNoPartitionedTableSplit(
                    nonPartitionLakeSplits, isLogTable, tableBucketsOffset);
        }
    }
    // 核心作用是将“平铺”的湖存储分片列表（List<LakeSplit>），
    // 按照**分区（Partition）和桶（Bucket）**两个层级进行重新组织，形成一个嵌套的映射结构（Map）。
    private Map<String, Map<Integer, List<LakeSplit>>> groupLakeSplits(List<LakeSplit> lakeSplits) {
        // 初始化结果容器。
        // 第一层 Key (String) 是分区名称；
        // 第二层 Key (Integer) 是桶 ID；
        // Value 是属于该分区该桶的所有 LakeSplit 列表。
        Map<String, Map<Integer, List<LakeSplit>>> result = new HashMap<>();
        // 生成的所有物理分片进行循环处理。每一个 LakeSplit 通常代表湖存储（如 S3 或 HDFS）中的一个或多个 Parquet/Orc 文件。
        for (LakeSplit split : lakeSplits) {
            String partition = String.join(PARTITION_SPEC_SEPARATOR, split.partition());
            int bucket = split.bucket();
            // Get or create the partition group
            Map<Integer, List<LakeSplit>> bucketMap =
                    result.computeIfAbsent(partition, k -> new HashMap<>());
            List<LakeSplit> splitList = bucketMap.computeIfAbsent(bucket, k -> new ArrayList<>());
            splitList.add(split);
        }
        return result;
    }

    // 核心目标是：对比“数据湖”中的分区和“Fluss 日志服务”中的分区，确保无论是两边都有的分区，还是只存在于某一边的分区，都能被正确生成 Flink 分片。
    private List<SourceSplitBase> generatePartitionTableSplit(
            Map<String, Map<Integer, List<LakeSplit>>> lakeSplits,
            boolean isLogTable,
            Map<TableBucket, Long> tableBucketSnapshotLogOffset,
            Map<Long, String> partitionNameById) {
        List<SourceSplitBase> splits = new ArrayList<>();
        // 1. 将传入的 [ID -> Name] 映射反转为 [Name -> ID] 映射
        // 这样做是为了后面能通过分区名称（String）快速找到 Fluss 内部的 Partition ID
        Map<String, Long> flussPartitionIdByName =
                partitionNameById.entrySet().stream()
                        .collect(
                                Collectors.toMap(
                                        Map.Entry::getValue,
                                        Map.Entry::getKey,
                                        (existing, replacement) -> existing,
                                        LinkedHashMap::new));
        // 2. 初始化一个虚拟 ID。对于只存在于湖中、 Fluss 里已不存在的分区，
        // 我们给它分配一个从 -1 开始递减的 ID，以确保分片的唯一性。
        long lakeSplitPartitionId = -1L;

        // iterate lake splits
        // 这一步处理所有在“数据湖”中存有历史文件的分区。
        for (Map.Entry<String, Map<Integer, List<LakeSplit>>> lakeSplitEntry :
                lakeSplits.entrySet()) {
            String partitionName = lakeSplitEntry.getKey();
            Map<Integer, List<LakeSplit>> lakeSplitsOfPartition = lakeSplitEntry.getValue();
            Long partitionId = flussPartitionIdByName.remove(partitionName);
            if (partitionId != null) {
                // --- 场景 A：湖里有数据，Fluss 里也存在该分区 ---
                // 1. 获取该分区下所有桶在 Fluss 中的“终点 Offset”（批模式有终点，流模式通常是持续读取）
                // mean the partition also exist in fluss partition
                Map<Integer, Long> bucketEndOffset =
                        stoppingOffsetInitializer.getBucketOffsets(
                                partitionName,
                                IntStream.range(0, bucketCount)
                                        .boxed()
                                        .collect(Collectors.toList()),
                                bucketOffsetsRetriever);
                // 2. 生成“混合分片”：包含湖里的文件和接续的 Fluss 日志
                splits.addAll(
                        generateSplit(
                                lakeSplitsOfPartition,
                                partitionId,
                                partitionName,
                                isLogTable,
                                tableBucketSnapshotLogOffset,
                                bucketEndOffset));

            } else {
                // only lake data
                // --- 场景 B：湖里有数据，但 Fluss 里该分区已被删除（TTL过期或手动删除） ---
                // 仅生成湖分片，不再尝试读取日志。使用自减的虚拟 ID 保证并发分发时不会冲突。
                splits.addAll(
                        toLakeSnapshotSplits(
                                lakeSplitsOfPartition,
                                partitionName,
                                // now, we can't get partition id for the partition only
                                // in lake, set them to a arbitrary partition id, but
                                // make sure different partition have different partition id
                                // to enable different partition can be distributed to different
                                // tasks
                                lakeSplitPartitionId--));
            }
        }

        // iterate remain fluss splits
        // 在第一阶段中，我们使用了 remove(partitionName)。剩下的就是**“湖里还没生成快照，但 Fluss 里已经产生了新数据”**的分区。
        for (Map.Entry<String, Long> partitionIdByNameEntry : flussPartitionIdByName.entrySet()) {
            String partitionName = partitionIdByNameEntry.getKey();
            Long partitionId = partitionIdByNameEntry.getValue();
            // 1. 同样获取这些新分区的终点位置
            Map<Integer, Long> bucketEndOffset =
                    stoppingOffsetInitializer.getBucketOffsets(
                            partitionName,
                            IntStream.range(0, bucketCount).boxed().collect(Collectors.toList()),
                            bucketOffsetsRetriever);
            // 2. 生成“纯日志分片”：因为湖里没有这些分区的文件
            // 第一个参数传 null，表示没有 LakeSplits
            splits.addAll(
                    generateSplit(
                            null,
                            partitionId,
                            partitionName,
                            isLogTable,
                            // pass empty map since we won't read lake splits
                            Collections.emptyMap(),
                            bucketEndOffset));
        }
        return splits;
    }

    private List<SourceSplitBase> generateSplit(
            @Nullable Map<Integer, List<LakeSplit>> lakeSplits,
            @Nullable Long partitionId,
            @Nullable String partitionName,
            boolean isLogTable,
            Map<TableBucket, Long> tableBucketSnapshotLogOffset,
            Map<Integer, Long> bucketEndOffset) {
        List<SourceSplitBase> splits = new ArrayList<>();
        if (isLogTable) {
            if (lakeSplits != null) {
                splits.addAll(toLakeSnapshotSplits(lakeSplits, partitionName, partitionId));
            }
            for (int bucket = 0; bucket < bucketCount; bucket++) {
                TableBucket tableBucket =
                        new TableBucket(tableInfo.getTableId(), partitionId, bucket);
                Long snapshotLogOffset = tableBucketSnapshotLogOffset.get(tableBucket);
                Long stoppingOffset = bucketEndOffset.get(bucket);
                if (snapshotLogOffset == null) {
                    // no any data commit to this bucket, scan from fluss log
                    splits.add(
                            new LogSplit(
                                    tableBucket, partitionName, EARLIEST_OFFSET, stoppingOffset));
                } else {
                    // need to read remain fluss log
                    if (stoppingOffset == NO_STOPPING_OFFSET
                            || snapshotLogOffset < stoppingOffset) {
                        splits.add(
                                new LogSplit(
                                        tableBucket,
                                        partitionName,
                                        snapshotLogOffset,
                                        stoppingOffset));
                    }
                }
            }
        } else {
            // it's primary key table
            for (int bucket = 0; bucket < bucketCount; bucket++) {
                TableBucket tableBucket =
                        new TableBucket(tableInfo.getTableId(), partitionId, bucket);
                Long snapshotLogOffset = tableBucketSnapshotLogOffset.get(tableBucket);
                Long stoppingOffset = bucketEndOffset.get(bucket);
                splits.add(
                        generateSplitForPrimaryKeyTableBucket(
                                lakeSplits != null ? lakeSplits.get(bucket) : null,
                                tableBucket,
                                partitionName,
                                snapshotLogOffset,
                                stoppingOffset));
            }
        }

        return splits;
    }

    private List<SourceSplitBase> toLakeSnapshotSplits(
            Map<Integer, List<LakeSplit>> lakeSplits,
            @Nullable String partitionName,
            @Nullable Long partitionId) {
        List<SourceSplitBase> splits = new ArrayList<>();
        // we may have multiple table buckets; so we need to
        // introduce an index to make split unique
        int index = 0;
        for (LakeSplit lakeSplit :
                lakeSplits.values().stream().flatMap(List::stream).collect(Collectors.toList())) {
            TableBucket tableBucket =
                    new TableBucket(tableInfo.getTableId(), partitionId, lakeSplit.bucket());
            splits.add(new LakeSnapshotSplit(tableBucket, partitionName, lakeSplit, index++));
        }
        return splits;
    }

    private SourceSplitBase generateSplitForPrimaryKeyTableBucket(
            @Nullable List<LakeSplit> lakeSplits,
            TableBucket tableBucket,
            @Nullable String partitionName,
            @Nullable Long snapshotLogOffset,
            long stoppingOffset) {
        // no snapshot data for this bucket or no a corresponding log offset in this bucket,
        // can only scan from change log
        if (snapshotLogOffset == null || snapshotLogOffset < 0) {
            return new LakeSnapshotAndFlussLogSplit(
                    tableBucket, partitionName, null, EARLIEST_OFFSET, stoppingOffset);
        }

        return new LakeSnapshotAndFlussLogSplit(
                tableBucket, partitionName, lakeSplits, snapshotLogOffset, stoppingOffset);
    }

    private List<SourceSplitBase> generateNoPartitionedTableSplit(
            Map<Integer, List<LakeSplit>> lakeSplits,
            boolean isLogTable,
            Map<TableBucket, Long> tableBucketSnapshotLogOffset) {
        // iterate all bucket
        // assume bucket is from 0 to bucket count
        Map<Integer, Long> bucketEndOffset =
                stoppingOffsetInitializer.getBucketOffsets(
                        null,
                        IntStream.range(0, bucketCount).boxed().collect(Collectors.toList()),
                        bucketOffsetsRetriever);
        return generateSplit(
                lakeSplits, null, null, isLogTable, tableBucketSnapshotLogOffset, bucketEndOffset);
    }
}
