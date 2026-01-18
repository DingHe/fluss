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

package org.apache.fluss.flink.source.state;

import org.apache.fluss.flink.source.split.SourceSplitBase;
import org.apache.fluss.metadata.TableBucket;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/** A checkpoint of the current state of the containing the buckets that is already assigned. */
// 主要负责记录 SourceEnumerator（源枚举器）在进行检查点（Checkpoint）时的快照状态。
// 在 Flink 的新版 Source 架构中，SourceEnumerator 负责发现数据分片（Splits）并将其分配给下游的 SourceReader。
// SourceEnumeratorState 的核心作用是故障恢复（Fault Tolerance）：
// 持久化进度：当 Flink 进行 Checkpoint 时，会将 SourceEnumerator 当前的工作进度（即哪些 Bucket 已分配，哪些 Partition 已扫描）序列化并存储在 SourceEnumeratorState 中。
// 状态重建：如果作业发生故障重启，Flink 会从最近一次成功的 Checkpoint 中恢复此状态对象，使枚举器能够准确知道哪些数据已经处理过，从而避免重复分配或遗漏分配。
public class SourceEnumeratorState {

    /** buckets that have been assigned to readers. */
    // 记录已经分配给下游 Reader 的 Bucket（桶）集合
    // Fluss 的表数据是按 Bucket 组织的。为了保证流式读取的有序性和唯一性，Enumerator 需要知道哪些 Bucket 已经处于被读取状态。
    private final Set<TableBucket> assignedBuckets;

    // partitions that have been assigned to readers.
    // mapping from partition id to partition name
    // 记录已经分配的 Partition（分区）映射关系。
    // Key 是 Partition ID（长整型），Value 是 Partition 的名称（字符串）
    private final Map<Long, String> assignedPartitions;

    // the unassigned lake splits of a lake snapshot and the fluss splits base on the
    // lake snapshot
    // 记录剩余未分配的混合读取分片。
    // 当系统进行快照时，如果有一些从“湖”快照中生成的 Split 还没有分发给 Reader，它们会被保存在这个列表中。重启后，枚举器会从这里重新加载这些分片并继续分发，确保数据不丢失。
    @Nullable private final List<SourceSplitBase> remainingHybridLakeFlussSplits;

    public SourceEnumeratorState(
            Set<TableBucket> assignedBuckets,
            Map<Long, String> assignedPartitions,
            @Nullable List<SourceSplitBase> remainingHybridLakeFlussSplits) {
        this.assignedBuckets = assignedBuckets;
        this.assignedPartitions = assignedPartitions;
        this.remainingHybridLakeFlussSplits = remainingHybridLakeFlussSplits;
    }

    public Set<TableBucket> getAssignedBuckets() {
        return assignedBuckets;
    }

    public Map<Long, String> getAssignedPartitions() {
        return assignedPartitions;
    }

    @Nullable
    public List<SourceSplitBase> getRemainingHybridLakeFlussSplits() {
        return remainingHybridLakeFlussSplits;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SourceEnumeratorState that = (SourceEnumeratorState) o;
        return Objects.equals(assignedBuckets, that.assignedBuckets)
                && Objects.equals(assignedPartitions, that.assignedPartitions)
                && Objects.equals(
                        remainingHybridLakeFlussSplits, that.remainingHybridLakeFlussSplits);
    }

    @Override
    public int hashCode() {
        return Objects.hash(assignedBuckets, assignedPartitions, remainingHybridLakeFlussSplits);
    }

    @Override
    public String toString() {
        return "SourceEnumeratorState{"
                + "assignedBuckets="
                + assignedBuckets
                + ", assignedPartitions="
                + assignedPartitions
                + ", remainingHybridLakeFlussSplits="
                + remainingHybridLakeFlussSplits
                + '}';
    }
}
