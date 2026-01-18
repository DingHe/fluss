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
import java.util.Optional;

/** The split for log. It's used to describe the log data of a table bucket. */
// 专门用于描述和追踪 Fluss 表中某个分桶（Bucket）的**增量日志数据（Log Data）**读取进度。
// LogSplit 的核心作用是作为 流式读取的进度指针。
// 在 Fluss 的流式架构中，数据以 Log 的形式存储，类似于 Kafka 的 Topic Partition。LogSplit 告诉 Flink 的 SourceReader：
// 从哪里开始读：通过起始偏移量（Offset）确定。
// 读到哪里为止：通过停止偏移量确定（可选）。
// 对应哪个物理节点：通过继承自基类的 TableBucket 确定位置。
public class LogSplit extends SourceSplitBase {
    // 表示“无停止位置”。在普通的流式任务中，由于需要持续监听新数据，因此没有结束时间点，此时会使用这个常量。
    public static final long NO_STOPPING_OFFSET = Long.MIN_VALUE;
    // 分片 ID 的前缀。用于在 Flink 内部生成唯一标识符，区分于快照分片（Snapshot Split）。
    private static final String LOG_SPLIT_PREFIX = "log-";
    // 起始偏移量
    // 指示 Reader 应该从该 Bucket 的哪一条记录开始读取数据。它是保证“断点续传”的核心。
    private final long startingOffset;
    // 停止偏移量
    // 如果设置了大于或等于 0 的值，Reader 读到该位置就会停止并关闭此分片。如果为 NO_STOPPING_OFFSET，则代表无限流读取。
    private final long stoppingOffset;

    public LogSplit(TableBucket tableBucket, @Nullable String partitionName, long startingOffset) {
        this(tableBucket, partitionName, startingOffset, NO_STOPPING_OFFSET);
    }

    public LogSplit(
            TableBucket tableBucket,
            @Nullable String partitionName,
            long startingOffset,
            long stoppingOffset) {
        super(tableBucket, partitionName);
        this.startingOffset = startingOffset;
        this.stoppingOffset = stoppingOffset;
    }

    public long getStartingOffset() {
        return startingOffset;
    }

    public Optional<Long> getStoppingOffset() {
        return stoppingOffset >= 0 ? Optional.of(stoppingOffset) : Optional.empty();
    }

    @Override
    protected byte splitKind() {
        return LOG_SPLIT_FLAG;
    }

    @Override
    public String splitId() {
        return toSplitId(LOG_SPLIT_PREFIX, tableBucket);
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (!(object instanceof LogSplit)) {
            return false;
        }
        if (!super.equals(object)) {
            return false;
        }
        LogSplit logSplit = (LogSplit) object;
        return startingOffset == logSplit.startingOffset
                && stoppingOffset == logSplit.stoppingOffset;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), startingOffset, stoppingOffset);
    }

    @Override
    public String toString() {
        return "LogSplit{"
                + "tableBucket="
                + tableBucket
                + ", partitionName='"
                + partitionName
                + '\''
                + ", startingOffset="
                + startingOffset
                + ", stoppingOffset="
                + stoppingOffset
                + '}';
    }
}
