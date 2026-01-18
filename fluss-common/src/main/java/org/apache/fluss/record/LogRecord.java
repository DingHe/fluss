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

package org.apache.fluss.record;

import org.apache.fluss.annotation.PublicEvolving;
import org.apache.fluss.memory.MemorySegment;
import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.compacted.CompactedRow;
import org.apache.fluss.row.compacted.CompactedRowDeserializer;
import org.apache.fluss.row.indexed.IndexedRow;
import org.apache.fluss.types.DataType;

/**
 * A log record is a tuple consisting of a unique offset in the log, a changeType and a row.
 *
 * @since 0.1
 */
// LogRecord 是底层存储和流式处理的基础数据单元。它代表了存储在 Log（日志）中的一条原子数据记录。
// LogRecord 接口定义了 Fluss 日志中一条记录的逻辑视图。
// Fluss 是一个类似于 Kafka 但针对流式数仓优化过的系统，它的日志不仅包含原始数据，还包含元数据（如 Offset）和变更信息（ChangeType）。LogRecord 的主要作用包括：
@PublicEvolving
public interface LogRecord {

    /**
     * The offset of this record in the log.
     *
     * @return the offset
     */
    // 获取该记录在日志分区中的唯一偏移量（Offset）。
    long logOffset();

    /**
     * The commit timestamp of this record in the log.
     *
     * @return the timestamp
     */
    // 获取记录的提交时间戳（Commit Timestamp）
    long timestamp();

    /**
     * Get the log record's {@link ChangeType}.
     *
     * @return the record's {@link ChangeType}.
     */
    // 获取该记录的操作类型。
    ChangeType getChangeType();

    /**
     * Get the log record's row.
     *
     * @return the log record's row
     */
    // 获取记录中实际保存的数据行。
    InternalRow getRow();

    /** Deserialize the row in the log record according to given log format. */
    // 这是 Fluss 实现高性能读取的关键。它根据不同的**日志格式（LogFormat）**将二进制字节流转换为对应的行对象。
    static InternalRow deserializeInternalRow(
            int length,
            MemorySegment segment,
            int position,
            DataType[] fieldTypes,
            LogFormat logFormat) {
        if (logFormat == LogFormat.INDEXED) {
            IndexedRow indexedRow = new IndexedRow(fieldTypes);
            indexedRow.pointTo(segment, position, length);
            return indexedRow;
        } else if (logFormat == LogFormat.COMPACTED) {
            CompactedRow compactedRow =
                    new CompactedRow(fieldTypes.length, new CompactedRowDeserializer(fieldTypes));
            compactedRow.pointTo(segment, position, length);
            return compactedRow;
        } else {
            throw new IllegalArgumentException(
                    "No such internal row deserializer for: " + logFormat);
        }
    }
}
