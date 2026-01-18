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
import org.apache.fluss.row.InternalRow;

/** A generic implementation of {@link LogRecord} which is backed by generic Java objects. */
// 实现了 LogRecord 接口，提供了一种通用的方式来表示从日志中读取到的单条记录。
// GenericRecord 的作用是作为日志单条记录的通用 Java 对象表示。
// 在 Fluss 的存储架构中，数据在磁盘或内存（LogRecordBatch）中通常是以紧凑的二进制格式存在的。当系统需要处理这些数据（例如在 Flink 中进行计算）时，需要将二进制数据反序列化为 Java 对象。
// 数据承载：它将一条记录的物理元数据（位点、时间戳）与实际的内容数据（行数据）组合在一起。
// 通用性：它不依赖于特定的序列化框架（如 Arrow），而是基于 Fluss 内部的 InternalRow 接口，可以表示任何 Schema 的数据。
// 只读性：该类设计为不可变（Immutable），属性均由 final 修饰，确保了数据在被多个算子或线程处理时的安全性。

@PublicEvolving
public class GenericRecord implements LogRecord {
    // 日志位点。代表该条记录在所属桶（Bucket）日志流中的唯一物理偏移量。
    // 它是数据的唯一标识，常用于 Checkpoint 和断点续传。
    private final long logOffset;
    // 时间戳。记录该数据产生或存入系统的时间。
    // 在流处理中，这通常对应于事件时间（Event Time）或摄入时间（Ingestion Time）。
    private final long timestamp;
    // 变更类型。标记该条记录的操作类型。
    // 由于 Fluss 支持 CDC（变更数据捕获），该值可能是：+I (INSERT)、-U (UPDATE_BEFORE)、+U (UPDATE_AFTER) 或 -D (DELETE)。
    private final ChangeType changeType;
    // 行数据实体。这是最核心的业务数据。InternalRow 是 Fluss 的内部行接口，支持按位置或名称访问具体的列字段值。
    private final InternalRow row;

    public GenericRecord(long logOffset, long timestamp, ChangeType changeType, InternalRow row) {
        this.logOffset = logOffset;
        this.timestamp = timestamp;
        this.changeType = changeType;
        this.row = row;
    }

    @Override
    public long logOffset() {
        return logOffset;
    }

    @Override
    public long timestamp() {
        return timestamp;
    }

    @Override
    public ChangeType getChangeType() {
        return changeType;
    }

    @Override
    public InternalRow getRow() {
        return row;
    }
}
