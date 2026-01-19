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

import org.apache.fluss.metadata.KvFormat;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.SchemaGetter;
import org.apache.fluss.row.decode.RowDecoder;
import org.apache.fluss.types.DataType;

import java.util.HashMap;
import java.util.Map;

/** A default implementation of {@link KvRecordBatch.ReadContext} . */
// 在 Fluss 这种支持 Schema 演进（Schema Evolution） 的系统中，同一个数据流的不同批次（Batch）可能使用不同版本的 Schema（由 schemaId 标识）。
// 连接元数据与数据：它持有 SchemaGetter，能够根据数据批次中的 schemaId 查找到对应的字段定义。
// 管理解码逻辑：它负责根据存储格式（kvFormat）和具体的 Schema 创建出正确的 RowDecoder。
// 性能优化（缓存）：通过内部缓存已创建的 RowDecoder，避免在读取大量数据批次时重复进行耗时的解码器初始化操作（如反射、类型数组构建等）。


public class KvRecordReadContext implements KvRecordBatch.ReadContext {
    // 存储当前读取任务所采用的键值存储格式（如 COMPACTED 或 INDEXED）
    private final KvFormat kvFormat;
    // 一个元数据查询器接口。
    private final SchemaGetter schemaGetter;
    // RowDecoder 的实例缓存池。
    private final Map<Integer, RowDecoder> rowDecoderCache;

    // 初始化格式类型、元数据查询器，并创建一个空的 HashMap 用于缓存。
    private KvRecordReadContext(KvFormat kvFormat, SchemaGetter schemaGetter) {
        this.kvFormat = kvFormat;
        this.schemaGetter = schemaGetter;
        this.rowDecoderCache = new HashMap<>();
    }

    public static KvRecordReadContext createReadContext(
            KvFormat kvFormat, SchemaGetter schemaGetter) {
        return new KvRecordReadContext(kvFormat, schemaGetter);
    }

    @Override
    public RowDecoder getRowDecoder(int schemaId) {
        return rowDecoderCache.computeIfAbsent(
                schemaId,
                (id) -> {
                    Schema schema = schemaGetter.getSchema((short) schemaId);
                    return RowDecoder.create(
                            kvFormat, schema.getRowType().getChildren().toArray(new DataType[0]));
                });
    }
}
