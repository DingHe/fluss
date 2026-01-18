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

package org.apache.fluss.flink.lake.split;

import org.apache.fluss.flink.source.split.SourceSplitBase;
import org.apache.fluss.lake.source.LakeSplit;
import org.apache.fluss.metadata.TableBucket;

import javax.annotation.Nullable;

/** A split for reading a snapshot of lake. */
// 用于处理数据湖（Data Lake）快照读取的具体分片实现类,连接 Flink 读取器与湖底层存储（如 Parquet/ORC 文件）的关键媒介。
// LakeSnapshotSplit 的主要作用是定义如何读取数据湖中某一特定时刻的全量（快照）数据。
// 在一个“湖仓一体”的任务中，通常需要先读取存储在数据湖中的历史存量数据，再衔接 Fluss 中的增量日志。该类的角色包括：
// 桥接器：它将通用的 LakeSplit（定义了湖中文件的位置）封装为 Flink 可识别的 SourceSplit。
// 断点续传器：通过记录跳过的记录数（recordsToSkip），确保在读取湖文件过程中发生故障重启时，能够从精确的行数恢复，实现 Exactly-Once 语义。
// 并行度管理：通过 splitIndex 区分同一个桶下的多个湖文件分片，支持并行读取。
public class LakeSnapshotSplit extends SourceSplitBase {
    // 分片类型标识符。
    public static final byte LAKE_SNAPSHOT_SPLIT_KIND = -1;
    // 底层湖分片的详细元数据。
    // 包含了实际存储层的信息（如文件路径、偏移量、分区路径等）。它是真正执行 I/O 操作所需的“说明书”。
    private final LakeSplit lakeSplit;
    // 跳过的记录行数。
    // 主要用于故障恢复。如果 Reader 在读取文件到一半时崩溃，重启后通过此值定位到上次处理的位置。
    private final long recordsToSkip;
    // 分片索引。
    // 当一个大的逻辑快照被拆分为多个物理分片时，此索引用于唯一标识该分片。
    private final int splitIndex;

    public LakeSnapshotSplit(
            TableBucket tableBucket,
            @Nullable String partitionName,
            LakeSplit lakeSplit,
            int splitIndex) {
        this(tableBucket, partitionName, lakeSplit, splitIndex, 0);
    }

    public LakeSnapshotSplit(
            TableBucket tableBucket,
            @Nullable String partitionName,
            LakeSplit lakeSplit,
            int splitIndex,
            long recordsToSkip) {
        super(tableBucket, partitionName);
        this.lakeSplit = lakeSplit;
        this.splitIndex = splitIndex;
        this.recordsToSkip = recordsToSkip;
    }

    public LakeSplit getLakeSplit() {
        return lakeSplit;
    }

    public long getRecordsToSkip() {
        return recordsToSkip;
    }

    public int getSplitIndex() {
        return splitIndex;
    }

    @Override
    public String splitId() {
        return toSplitId(
                        "lake-snapshot-",
                        new TableBucket(
                                tableBucket.getTableId(),
                                tableBucket.getPartitionId(),
                                lakeSplit.bucket()))
                + "-"
                + splitIndex;
    }

    @Override
    public boolean isLakeSplit() {
        return true;
    }

    @Override
    public byte splitKind() {
        return LAKE_SNAPSHOT_SPLIT_KIND;
    }

    @Override
    public String toString() {
        return "LakeSnapshotSplit{"
                + "lakeSplit="
                + lakeSplit
                + ", recordsToSkip="
                + recordsToSkip
                + ", splitIndex="
                + splitIndex
                + ", tableBucket="
                + tableBucket
                + ", partitionName='"
                + partitionName
                + '\''
                + '}';
    }
}
