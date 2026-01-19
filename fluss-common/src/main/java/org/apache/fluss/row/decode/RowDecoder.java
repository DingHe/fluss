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

package org.apache.fluss.row.decode;

import org.apache.fluss.annotation.PublicEvolving;
import org.apache.fluss.memory.MemorySegment;
import org.apache.fluss.metadata.KvFormat;
import org.apache.fluss.record.ValueRecord;
import org.apache.fluss.row.BinaryRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.types.DataType;

/**
 * A decoder to read {@link BinaryRow binary format InternalRow} from a byte array or memory segment
 * of a value record {@link ValueRecord}.
 *
 * @since 0.2
 */
// 主要用于将底层存储的二进制字节数据还原（解码）为内存中可操作的行对象（InternalRow/BinaryRow）
// 在分布式存储系统中，为了提高性能，数据通常以紧凑的二进制格式存储。当需要读取数据进行计算或返回给客户端时，就需要 RowDecoder 来完成这种“反序列化”过程。
// 解耦存储格式：由于 Fluss 支持不同的键值格式（如 COMPACTED 压缩格式和 INDEXED 索引格式），该接口通过工厂模式隐藏了具体的解码细节。
@PublicEvolving
public interface RowDecoder {

    /** Create a {@link RowDecoder} for to decode {@link InternalRow} from a byte array. */
    // 根据指定的存储格式和字段类型，创建对应的解码器实例。
    // kvFormat: 存储格式枚举。决定了数据是如何在二进制层面组织的。
    // fieldDataTypes: 一个 DataType 数组，描述了每一列的数据类型（如 INT, STRING, LONG 等）。解码器需要根据这些信息来定位和解析字段。
    static RowDecoder create(KvFormat kvFormat, DataType[] fieldDataTypes) {
        // 如果格式是 COMPACTED（压缩格式），则返回 CompactedRowDecoder。这种格式通常不存储索引偏移量，存储空间更小。
        if (kvFormat == KvFormat.COMPACTED) {
            return new CompactedRowDecoder(fieldDataTypes);
        // 如果格式是 INDEXED（索引格式），则返回 IndexedRowDecoder。这种格式通常带有偏移量索引，随机访问字段的速度更快。
        } else if (kvFormat == KvFormat.INDEXED) {
            return new IndexedRowDecoder(fieldDataTypes);
        } else {
            throw new IllegalArgumentException("Unsupported kv format: " + kvFormat);
        }
    }

    /** Decode the byte array to {@link BinaryRow}. */
    // 将整个字节数组解码为一个 BinaryRow
    BinaryRow decode(byte[] values);

    /**
     * Decode the bytes in the memory segment to {@link BinaryRow}.
     *
     * @param segment the memory segment to read.
     * @param offset the offset in the memory segment to read from.
     * @param sizeInBytes the total size in bytes to read.
     */
    // 这是更底层的解码方法，
    // 直接从 Fluss 的内存管理单元 MemorySegment 中读取特定片段的数据。
    BinaryRow decode(MemorySegment segment, int offset, int sizeInBytes);
}
