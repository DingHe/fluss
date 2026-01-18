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

package org.apache.fluss.flink.source.enumerator.initializer;

import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.admin.OffsetSpec;
import org.apache.fluss.flink.source.split.SourceSplitBase;
import org.apache.fluss.metadata.TablePath;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Collection;
import java.util.Map;

/** An interface for users to specify the starting offset of a {@link SourceSplitBase}. */
// 针对 Flink Source 开发的一个核心接口。
// 它借鉴了 Flink Kafka Connector 的设计思想，为用户提供了一种声明式的方式，来指定作业启动时（或结束时）应该从 Fluss 表的哪个位置（Offset）开始读取数据。
// 该接口的主要作用是定义数据消费的起点或终点。
// 在分布式流处理中，每个分片（Split/Bucket）的进度各不相同。
// OffsetsInitializer 封装了查询元数据的逻辑，将抽象的用户需求（如“我要从最早开始读”或“我要从昨天开始读”）转化为具体的、每个 Bucket 对应的物理 Long 类型偏移量（Offset）。
// 全量消费：从最早的数据或最新的表快照开始。
// 实时消费：跳过历史数据，只读任务启动后的新数据。
// 时间点消费：定位到特定的时间戳位置。
public interface OffsetsInitializer extends Serializable {

    /**
     * Get the initial offsets for the given fluss buckets. These offsets will be used as starting
     * offsets of the fluss buckets.
     *
     * @param partitionName the partition name of the buckets if they are partitioned. Otherwise,
     *     null.
     * @param buckets the fluss buckets to get the starting offsets.
     * @param bucketOffsetsRetriever a helper to retrieve information of the fluss buckets.
     * @return A mapping from fluss bucket to their offsets to start scanning from.
     */
    // 根据具体的初始化策略，计算并返回一组 Bucket 对应的起始偏移量。
    Map<Integer, Long> getBucketOffsets(
            @Nullable String partitionName,
            Collection<Integer> buckets,
            BucketOffsetsRetriever bucketOffsetsRetriever); // 这是一个回调工具（见下文），用来实际向 Fluss 集群发送请求获取物理位点。

    /**
     * An interface that provides necessary information to the {@link OffsetsInitializer} to get the
     * initial offsets of the fluss buckets.
     */
    // 作用是将逻辑上的位点请求转化为 RPC 通信。
    interface BucketOffsetsRetriever {
        // 获取这些桶当前最新的位点（Log End Offset）
        Map<Integer, Long> latestOffsets(
                @Nullable String partitionName, Collection<Integer> buckets);
        // 获取这些桶当前最早可用的位点。
        Map<Integer, Long> earliestOffsets(
                @Nullable String partitionName, Collection<Integer> buckets);
        // 根据时间戳寻找位点。
        Map<Integer, Long> offsetsFromTimestamp(
                @Nullable String partitionName, Collection<Integer> buckets, long timestamp);
    }

    // --------------- factory methods ---------------

    /**
     * Get an {@link OffsetsInitializer} which initializes the offsets to the earliest available
     * offsets of each bucket.
     *
     * @return an {@link OffsetsInitializer} which initializes the offsets to the earliest available
     *     offsets.
     */
    // 从头开始。读取 Fluss Log 中当前保存的最早的一条记录。适用于全量历史数据追溯。
    static OffsetsInitializer earliest() {
        return new EarliestOffsetsInitializer();
    }

    /**
     * Get an {@link OffsetsInitializer} which initializes the offsets to the latest offsets of each
     * bucket.
     *
     * @return an {@link OffsetsInitializer} which initializes the offsets to the latest offsets.
     */
    // 从末尾开始。任务启动时跳过所有存量数据，只读取任务启动后新写入的数据。通常用于纯实时监控场景。
    static OffsetsInitializer latest() {
        return new LatestOffsetsInitializer();
    }

    /**
     * Get an {@link OffsetsInitializer} which performs a full snapshot on the table upon first
     * startup, and continue to read log with the offset to the snapshot.
     *
     * <p>If the table to read is a log table, the full snapshot means reading from the earliest log
     * offset which means "full" OffsetsInitializer equal to the {@link #earliest()}
     * OffsetsInitializer. If the table to read is a primary key table, the full snapshot means
     * reading the latest snapshot which materializes all changes on the table.
     *
     * @return an {@link OffsetsInitializer} which initializes the offsets to snapshot offsets.
     */
    // 全量+增量模式。这是 Fluss 湖仓场景最常用的方法：
    // 1. 如果是日志表，等同于 earliest()。
    //2. 如果是主键表，它会定位到最新的快照位置，确保你能读到当前表的完整状态。
    static OffsetsInitializer full() {
        return new SnapshotOffsetsInitializer();
    }

    /**
     * Get an {@link OffsetsInitializer} which initializes the offsets in each bucket so that the
     * initialized offset is the offset of the first record batch whose commit timestamp is greater
     * than or equals the given timestamp (milliseconds).
     *
     * @param timestamp the timestamp (milliseconds) to start the scan.
     * @return an {@link OffsetsInitializer} which initializes the offsets based on the given
     *     timestamp.
     * @see Admin#listOffsets(TablePath, Collection, OffsetSpec)
     * @see Admin#listOffsets(TablePath, String, Collection, OffsetSpec)
     */
    // 按时间戳定位。找到第一个“提交时间 >= 给定时间戳”的数据批次
    static OffsetsInitializer timestamp(long timestamp) {
        return new TimestampOffsetsInitializer(timestamp);
    }
}
