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
import org.apache.fluss.row.decode.RowDecoder;

/**
 * A kv record batch is a container for {@link KvRecord KvRecords}.
 *
 * @since 0.1
 */
// 代表了以 KV（Key-Value）格式 存储的一组记录的集合。
// 在 Fluss 这种流式存储系统中，为了提高 I/O 效率，数据通常不会一条一条地处理或写入磁盘，而是将多条记录聚合在一起，形成一个 Batch（批次）。
// 数据容器：作为 KvRecord（键值记录）的物理载体，负责管理一组记录的二进制布局。
// 保证数据完整性：通过校验和（Checksum）机制确保数据在传输或存储过程中没有损坏。
// 支持幂等性写入：记录了 Writer ID 和序列号，用于在重试场景下防止数据重复。
// 元数据管理：记录了版本号（Magic）、Schema ID、记录总数等关键信息，以便正确解码。

@PublicEvolving
public interface KvRecordBatch {

    /** The "magic" values. */
    // 定义了 KV 记录格式的第 0 个版本号。
    // Magic Byte 是二进制协议中常用的手段，用于标识数据格式的版本。
    byte KV_MAGIC_VALUE_V0 = 0;

    /** The current "magic" value. */
    // 指向当前系统正在使用的 Magic 版本（目前为 V0）。
    byte CURRENT_KV_MAGIC_VALUE = KV_MAGIC_VALUE_V0;

    /**
     * Check whether the checksum of this batch is correct.
     *
     * @return true If so, false otherwise
     */
    // 检查该 Batch 的校验和（Checksum）是否正确。
    boolean isValid();

    /** Raise an exception if the checksum is not valid. */
    // 强校验。如果 isValid() 返回 false，该方法会直接抛出异常。
    void ensureValid();

    /**
     * Get the checksum of this record batch, which covers the batch header as well as all of the
     * records.
     *
     * @return The 4-byte unsigned checksum represented as a long
     */
    // 获取存储在 Batch Header 中的 4 字节无符号校验和。
    long checksum();

    /**
     * Get the schema id of this record batch.
     *
     * @return The schema id
     */
    // 获取该 Batch 所使用的 Schema 标识符。
    short schemaId();

    /**
     * Get the record format version of this record batch (i.e its magic value).
     *
     * @return the magic byte
     */
    // 获取格式版本号。
    byte magic();

    /**
     * Get writer id for this log record batch.
     *
     * @return writer id
     */
    // 获取写入者的唯一标识。
    long writerId();

    /**
     * Get batch base sequence for this log record batch. the base sequence is the first sequence
     * number of this batch, it's used to protect the idempotence of the those batches write by same
     * writer.
     *
     * @return batch base sequence
     */
    // 获取该批次的起始序列号。
    // 结合 writerId，系统可以实现幂等性写入。如果服务器收到了一个已经存在的序列号，则会忽略该重复写入。
    int batchSequence();

    /**
     * Get the size in bytes of this batch, including the size of the record and the batch overhead.
     *
     * @return The size in bytes of this batch
     */
    // 获取整个 Batch 的总字节数（包括 Header 和所有 Records 的开销）
    int sizeInBytes();

    /**
     * Get the count.
     *
     * @return The number of records in the batch.
     */
    // 获取当前批次中包含的记录条数。
    int getRecordCount();

    /**
     * Get the iterable of {@link KvRecord} in this batch.
     *
     * @param readContext The context to read records from the record batch
     * @return The iterable of {@link KvRecord} in this batch
     */
    // 获取一个可迭代对象，用于遍历 Batch 中的每一条记录。
    Iterable<KvRecord> records(ReadContext readContext);

    /** The read context of a {@link KvRecordBatch} to read records. */
    interface ReadContext {

        /**
         * Gets the row decoder for the given schema to decode bytes read from {@link
         * KvRecordBatch}.
         *
         * @param schemaId the schema of the kv records
         */
        // 根据传入的 schemaId 获取对应的行解码器。
        RowDecoder getRowDecoder(int schemaId);
    }
}
