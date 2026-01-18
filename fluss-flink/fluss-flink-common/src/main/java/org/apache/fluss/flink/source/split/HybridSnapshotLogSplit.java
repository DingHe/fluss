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

package org.apache.fluss.flink.source.split;

import org.apache.fluss.metadata.TableBucket;

import javax.annotation.Nullable;

import java.util.Objects;

/**
 * The hybrid split for first reading the snapshot files and then switch to read the cdc log from a
 * specified offset.
 *
 * <p>Only used for primary key table which will be of snapshot phase and incremental phase of
 * reading.
 */
// Apache Fluss 实现“流批一体”和“CDC（数据变更捕获）全量增量一体化”读取的核心类。
// 它继承自 SnapshotSplit，专门用于主键表（Primary Key Table）的数据消费。
// HybridSnapshotLogSplit 的主要作用是管理从“存量快照”到“增量日志”的平滑切换过程。
// 快照阶段（Snapshot Phase）：首先读取该分桶（Bucket）在某一时刻的全部存量数据。
// 增量阶段（Incremental Phase）：快照读完后，自动切换到该时刻之后的 CDC 日志，继续消费增量变更。
public class HybridSnapshotLogSplit extends SnapshotSplit {

    private static final String HYBRID_SPLIT_PREFIX = "hybrid-snapshot-log-";
    // 记录当前分片是否已经完成了快照阶段的读取
    private final boolean isSnapshotFinished;
    // 指定快照结束后，CDC 日志应该从哪一个逻辑偏移量（Offset）开始消费
    private final long logStartingOffset;

    public HybridSnapshotLogSplit(
            TableBucket tableBucket,
            @Nullable String partitionName,
            long snapshotId,
            long logStartingOffset) {
        this(tableBucket, partitionName, snapshotId, 0, false, logStartingOffset);
    }

    public HybridSnapshotLogSplit(
            TableBucket tableBucket,
            @Nullable String partitionName,
            long snapshotId,
            long recordsToSkip,
            boolean isSnapshotFinished,
            long logStartingOffset) {
        super(tableBucket, partitionName, snapshotId, recordsToSkip);
        this.isSnapshotFinished = isSnapshotFinished;
        this.logStartingOffset = logStartingOffset;
    }

    public long getLogStartingOffset() {
        return logStartingOffset;
    }

    public boolean isSnapshotFinished() {
        return isSnapshotFinished;
    }

    @Override
    public String splitId() {
        return toSplitId(HYBRID_SPLIT_PREFIX, tableBucket);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof HybridSnapshotLogSplit)) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        HybridSnapshotLogSplit that = (HybridSnapshotLogSplit) o;
        return isSnapshotFinished == that.isSnapshotFinished
                && logStartingOffset == that.logStartingOffset;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), isSnapshotFinished, logStartingOffset);
    }

    @Override
    public String toString() {
        return "HybridSnapshotLogSplit{"
                + "tableBucket="
                + tableBucket
                + ", partitionName='"
                + partitionName
                + "', snapshotId="
                + snapshotId
                + ", isSnapshotFinished="
                + isSnapshotFinished
                + ", logStartingOffset="
                + logStartingOffset
                + ", recordsToSkip="
                + recordsToSkip
                + '}';
    }
}
