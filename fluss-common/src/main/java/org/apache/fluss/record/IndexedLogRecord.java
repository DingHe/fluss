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
import org.apache.fluss.memory.OutputView;
import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.row.BinaryRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.indexed.IndexedRow;
import org.apache.fluss.row.indexed.IndexedRowWriter;
import org.apache.fluss.types.DataType;
import org.apache.fluss.utils.MurmurHashUtils;

import java.io.IOException;

import static org.apache.fluss.record.LogRecordBatchFormat.LENGTH_LENGTH;

/* This file is based on source code of Apache Kafka Project (https://kafka.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/**
 * This class is an immutable log record and can be directly persisted. The schema is as follows:
 *
 * <ul>
 *   <li>Length => int32
 *   <li>Attributes => Int8
 *   <li>Value => {@link InternalRow}
 * </ul>
 *
 * <p>The current record attributes are depicted below:
 *
 * <p>----------- | ChangeType (0-3) | Unused (4-7) |---------------
 *
 * <p>The offset compute the difference relative to the base offset and of the batch that this
 * record is contained in.
 *
 * @since 0.1
 */
// Apache Fluss 中用于处理**索引格式（Indexed Format）**日志记录的核心类。
// 它实现了 LogRecord 接口，主要用于以一种内存友好且可以直接持久化的二进制格式来表示单条记录。
// IndexedLogRecord 并不持有解压后的 Java 对象，而是直接指向内存段（MemorySegment）中的二进制数据，从而实现极高的读写效率
// IndexedLogRecord 的作用可以概括为：高性能的二进制日志记录访问器。
// 紧凑存储：定义了日志记录在磁盘/内存中的物理布局：
// Length (4字节)：记录的总长度
// Attributes (1字节)：包含记录的元数据，如 ChangeType（变更类型）。
// Value (变长)：实际的行数据（IndexedRow）。

@PublicEvolving
public class IndexedLogRecord implements LogRecord {
    // 固定为 1 字节，用于存储属性信息。
    private static final int ATTRIBUTES_LENGTH = 1;
    // 记录该行在日志流中的绝对位点。
    private final long logOffset;
    // 记录该行的时间戳。
    private final long timestamp;
    // 该行的字段类型定义数组（DataType[]），用于在反序列化 getRow() 时告知系统如何解析二进制数据。
    private final DataType[] fieldTypes;
    // 指向包含实际二进制数据的 MemorySegment（Fluss 封装的内存缓冲区）
    private MemorySegment segment;
    // 该条记录在 segment 中的起始偏移位置
    private int offset;
    // 整条记录的物理大小（包含长度字段本身）
    private int sizeInBytes;

    IndexedLogRecord(long logOffset, long timestamp, DataType[] fieldTypes) {
        this.logOffset = logOffset;
        this.fieldTypes = fieldTypes;
        this.timestamp = timestamp;
    }

    void pointTo(MemorySegment segment, int offset, int sizeInBytes) {
        this.segment = segment;
        this.offset = offset;
        this.sizeInBytes = sizeInBytes;
    }

    public int getSizeInBytes() {
        return sizeInBytes;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        IndexedLogRecord that = (IndexedLogRecord) o;
        return sizeInBytes == that.sizeInBytes
                && segment.equalTo(that.segment, offset, that.offset, sizeInBytes);
    }

    @Override
    public int hashCode() {
        return MurmurHashUtils.hashBytes(segment, offset, sizeInBytes);
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
        byte attributes = segment.get(offset + LENGTH_LENGTH);
        return ChangeType.fromByteValue(attributes);
    }

    @Override
    public InternalRow getRow() {
        int rowOffset = LENGTH_LENGTH + ATTRIBUTES_LENGTH;
        return LogRecord.deserializeInternalRow(
                sizeInBytes - rowOffset,
                segment,
                offset + rowOffset,
                fieldTypes,
                LogFormat.INDEXED);
    }

    /** Write the record to input `target` and return its size. */
    // 将一条 IndexedRow 写入输出流。它先写长度，再写属性字节，最后序列化行内容。
    public static int writeTo(OutputView outputView, ChangeType changeType, IndexedRow row)
            throws IOException {
        int sizeInBytes = calculateSizeInBytes(row);

        // TODO using varint instead int to reduce storage size.
        // write record total bytes size.
        outputView.writeInt(sizeInBytes);

        // write attributes.
        outputView.writeByte(changeType.toByteValue());

        // write internal row.
        serializeInternalRow(outputView, row);

        return sizeInBytes + LENGTH_LENGTH;
    }
    // 从内存段的指定位置读取并构造一个 IndexedLogRecord 对象。
    public static IndexedLogRecord readFrom(
            MemorySegment segment,
            int position,
            long logOffset,
            long logTimestamp,
            DataType[] colTypes) {
        int sizeInBytes = segment.getInt(position);
        IndexedLogRecord logRecord = new IndexedLogRecord(logOffset, logTimestamp, colTypes);
        logRecord.pointTo(segment, position, sizeInBytes + LENGTH_LENGTH);
        return logRecord;
    }
    // 预估将一行数据转换为 IndexedLogRecord 格式后所需的总字节数。
    public static int sizeOf(BinaryRow row) {
        int sizeInBytes = calculateSizeInBytes(row);
        return sizeInBytes + LENGTH_LENGTH;
    }

    private static int calculateSizeInBytes(BinaryRow row) {
        int size = 1; // always one byte for attributes
        size += row.getSizeInBytes();
        return size;
    }

    private static void serializeInternalRow(OutputView outputView, IndexedRow row)
            throws IOException {
        IndexedRowWriter.serializeIndexedRow(row, outputView);
    }
}
