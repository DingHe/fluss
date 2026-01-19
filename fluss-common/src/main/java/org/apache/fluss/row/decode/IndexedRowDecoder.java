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

import org.apache.fluss.memory.MemorySegment;
import org.apache.fluss.row.indexed.IndexedRow;
import org.apache.fluss.types.DataType;

/** A decoder to decode {@link IndexedRow} from a byte array or memory segment. */
// 专门用于处理 Indexed 格式二进制数据的解码器。它是 RowDecoder 接口的具体实现类。
// 将已经存在的二进制字节序列（存储在 byte[] 或 MemorySegment 中）包装成一个可操作的 IndexedRow 对象。
// 由于 IndexedRow 采用了“逻辑与物理分离”的设计（即行对象本身不持有数据，而是像指针一样指向内存），这个解码器的逻辑非常轻量化。
// 它不涉及复杂的数据解析或拷贝，其本质是初始化一个 IndexedRow 实例并将其“锚定”到指定的内存位置，从而让上层应用能够以结构化的方式读取其中的字段。
public class IndexedRowDecoder implements RowDecoder {
    // 存储该行数据对应的字段类型定义（Schema）
    private final DataType[] fieldDataTypes;

    public IndexedRowDecoder(DataType[] fieldDataTypes) {
        this.fieldDataTypes = fieldDataTypes;
    }
    // 将字节数组解码为 IndexedRow。
    @Override
    public IndexedRow decode(byte[] values) {
        return IndexedRow.from(fieldDataTypes, values);
    }
    // 从内存段（MemorySegment）的特定位置解码出 IndexedRow
    @Override
    public IndexedRow decode(MemorySegment segment, int offset, int sizeInBytes) {
        IndexedRow indexedRow = new IndexedRow(fieldDataTypes);
        indexedRow.pointTo(segment, offset, sizeInBytes);
        return indexedRow;
    }
}
