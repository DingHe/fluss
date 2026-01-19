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

package org.apache.fluss.row.encode;

import org.apache.fluss.memory.MemorySegment;
import org.apache.fluss.metadata.KvFormat;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.SchemaGetter;
import org.apache.fluss.record.BinaryValue;
import org.apache.fluss.row.BinaryRow;
import org.apache.fluss.row.decode.RowDecoder;
import org.apache.fluss.types.DataType;

import java.util.Map;

import static org.apache.fluss.row.encode.ValueEncoder.SCHEMA_ID_LENGTH;
import static org.apache.fluss.utils.MapUtils.newConcurrentHashMap;

/**
 * A decoder to decode a schema id and {@link BinaryRow} from a byte array value which is encoded by
 * {@link ValueEncoder#encodeValue(short, BinaryRow)}.
 */
// ValueDecoder 是 Apache Fluss 框架中负责将底层物理存储的 Value 字节还原为结构化行对象的高层解码器。它是 ValueEncoder 的逆向操作实现。
// 在 Fluss 的 KV 存储中，为了支持 Schema 的版本演进（Schema Evolution），每一条记录的 Value 并不是纯粹的数据，而是采用 [Schema ID (2字节)] + [Row Binary Data] 的格式进行打包存储。
// 解析 Schema ID：从字节流的前两个字节中提取 Schema 版本号。
// 动态获取元数据：根据 Schema ID 找到对应的字段类型定义。
// 还原数据行：利用匹配的 RowDecoder 将剩余的二进制数据转换为可读的 BinaryRow。
// 封装结果：将解析出的 Schema ID 和 Row 封装进 BinaryValue 对象返回。

public class ValueDecoder {
    // 作为 RowDecoder 的线程安全缓存。
    // 由于系统中可能存在多个版本的 Schema，每个版本都需要一个专门的 RowDecoder。该属性使用 ConcurrentHashMap 缓存已经创建过的解码器，避免每次解码都去重新解析 Schema 或创建对象，从而大幅提升解码吞吐量。
    private final Map<Short, RowDecoder> rowDecoders;
    // 元数据查询器接口
    private final SchemaGetter schemaGetter;
    // 定义存储格式（如 INDEXED 或 COMPACTED）
    private final KvFormat kvFormat;

    public ValueDecoder(SchemaGetter schemaGetter, KvFormat kvFormat) {
        this.rowDecoders = newConcurrentHashMap();
        this.schemaGetter = schemaGetter;
        this.kvFormat = kvFormat;
    }

    /** Decode the value bytes and return the schema id and the row encoded in the value bytes. */
    public BinaryValue decodeValue(byte[] valueBytes) {
        MemorySegment memorySegment = MemorySegment.wrap(valueBytes);
        short schemaId = memorySegment.getShort(0);

        RowDecoder rowDecoder =
                rowDecoders.computeIfAbsent(
                        schemaId,
                        (id) -> {
                            Schema schema = schemaGetter.getSchema(schemaId);
                            return RowDecoder.create(
                                    kvFormat,
                                    schema.getRowType().getChildren().toArray(new DataType[0]));
                        });

        BinaryRow row =
                rowDecoder.decode(
                        memorySegment, SCHEMA_ID_LENGTH, valueBytes.length - SCHEMA_ID_LENGTH);
        return new BinaryValue(schemaId, row);
    }
}
