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
import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.row.ProjectedRow;
import org.apache.fluss.shaded.arrow.org.apache.arrow.memory.BufferAllocator;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.CloseableIterator;

import javax.annotation.Nullable;

import java.util.Iterator;

import static org.apache.fluss.record.LogRecordBatchFormat.LOG_MAGIC_VALUE_V0;
import static org.apache.fluss.record.LogRecordBatchFormat.NO_WRITER_ID;

/**
 * A record batch is a container for {@link LogRecord LogRecords}.
 *
 * @since 0.1
 */
// 在 Fluss 中，为了提高 I/O 效率、压缩率和保证写入的原子性，数据不是单条存储的，而是被打包成一个个“批次（Batch）”
// LogRecordBatch 是日志数据的物理和逻辑管理单位。它的作用主要体现在：
// 数据封装：作为 LogRecord（具体行数据）的容器，包含了一组记录以及描述这组记录的元数据头（Header）。
// 一致性校验：通过校验和（Checksum）确保磁盘或网络传输的数据没有损坏。
// 性能优化：支持批量处理。同时支持多种格式（如索引友好的格式或高效查询的 Arrow 列式格式）。
// 幂等性支持：记录了 Writer ID 和序列号，用于实现分布式系统中的“精确一次（Exactly-once）”写入。
// 版本兼容：通过 magic 值管理不同版本的日志格式。
@PublicEvolving
public interface LogRecordBatch {
    /**
     * The current "magic" value. Even though we already support LOG_MAGIC_VALUE_V1, for
     * compatibility reasons — specifically, a higher-version Fluss Client (which supports
     * LOG_MAGIC_VALUE_V1) cannot write to a lower-version Fluss Server (which only supports
     * LOG_MAGIC_VALUE_V0) — we are unable to guarantee compatibility at this time. Therefore, we
     * will keep the current log magic value set to LOG_MAGIC_VALUE_V0 for now, and only upgrade it
     * to LOG_MAGIC_VALUE_V1 once the compatibility issue is resolved.
     */
    // 定义当前系统使用的日志协议版本（当前为 V0）
    byte CURRENT_LOG_MAGIC_VALUE = LOG_MAGIC_VALUE_V0;

    /**
     * Check whether the checksum of this batch is correct.
     *
     * @return true If so, false otherwise
     */
    // 检查该批次的 Checksum 是否正确
    boolean isValid();

    /** Raise an exception if the checksum is not valid. */
    // 强制校验，如果数据损坏则直接抛出异常。
    void ensureValid();

    /**
     * Get the checksum of this record batch, which covers the batch header as well as all of the
     * records.
     *
     * @return The 4-byte unsigned checksum represented as a long
     */
    // 返回该批次（包括 Header 和所有记录）的 CRC 校验和。
    long checksum();

    /**
     * Get the schema id of this record batch.
     *
     * @return The schema id
     */
    // 返回该批次数据所使用的 Schema 版本 ID。Fluss 支持 Schema 演进，不同批次可能对应不同版本的 Schema。
    short schemaId();

    /**
     * Get the base log offset contained in this record batch.
     *
     * @return The base offset of this record batch (which may or may not be the offset of the first
     *     record as described above).
     */
    // 获取该批次中第一条记录的起始物理偏移量。
    long baseLogOffset();

    /**
     * Get the last log offset in this record batch (inclusive). Just like {@link #baseLogOffset()},
     * the last offset always reflects the offset of the last record in the original batch.
     *
     * @return The offset of the last record in this batch
     */
    // 获取该批次中最后一条记录的物理偏移量。
    long lastLogOffset();

    /**
     * Get the log offset following this record batch (i.e. the last offset contained in this batch
     * plus one).
     *
     * @return the next consecutive offset following this batch
     */
    // 获取紧随该批次后的下一个预期的偏移量（即 lastLogOffset + 1）。
    long nextLogOffset();

    /**
     * Get the record format version of this record batch (i.e its magic value).
     *
     * @return the magic byte
     */
    // 返回日志格式的版本号（Magic Byte）
    byte magic();

    /**
     * Get commit timestamp of this record batch. Commit timestamp means the timestamp when the
     * batch is appended to the log segment in server.
     *
     * @return the commit timestamp
     */
    // 返回该批次在服务端被追加到日志时的“提交时间戳”。
    long commitTimestamp();

    /**
     * Get writer id for this log record batch.
     *
     * @return writer id
     */
    // 获取产生该批次的写入者唯一 ID。用于去重。
    long writerId();

    /** Does the batch have a valid writer id set. */
    // 判断该批次是否设置了有效的 Writer ID。
    default boolean hasWriterId() {
        return writerId() != NO_WRITER_ID;
    }

    /**
     * Get batch sequence number for this log record batch. it's used to protect the idempotence of
     * the written batches write by same writer.
     *
     * @return batch base sequence
     */
    // 获取该批次的序列号。对于同一个 Writer ID，序列号必须是连续递增的，系统据此实现幂等性。
    int batchSequence();

    /**
     * Get leader epoch of this bucket for this log record batch.
     *
     * @return leader epoch
     */
    // 返回写入该批次时当前桶（Bucket）的 Leader 版本号。用于在副本切换或故障恢复时校验数据一致性。
    int leaderEpoch();

    /**
     * Get the size in bytes of this batch, including the size of the record and the batch overhead.
     *
     * @return The size in bytes of this batch
     */
    // 返回该批次的物理总大小（字节），包含 Header 和所有记录体。
    int sizeInBytes();

    /**
     * Get the count.
     *
     * @return The number of records in the batch.
     */
    // 返回该批次内包含的记录条数。
    int getRecordCount();

    /**
     * Returns a closeable iterator of records for this batch which basically delays deserialization
     * of the record stream until the records are actually asked for using {@link Iterator#next()}.
     * Callers should ensure that the iterator is closed.
     *
     * @param context The context to read records from the record batch.
     * @return The closeable iterator of records in this batch
     * @see ReadContext
     */
    // 返回一个可关闭的迭代器 CloseableIterator<LogRecord>
    CloseableIterator<LogRecord> records(ReadContext context);

    /** The read context of a {@link LogRecordBatch} to read records. */
    // 由于日志批次可能是压缩的、列式的（Arrow）或者投影过的，读取时需要背景信息。
    interface ReadContext {

        /** Gets the log format of the record batch. */
        // 确定日志是普通的行格式还是 Arrow 格式。
        LogFormat getLogFormat();

        /**
         * Get the row type of the schema id. The returned row type is projected if the record batch
         * is a projected {@link LogRecordBatch}.
         *
         * @param schemaId The schema id of the record batch.
         * @return The (maybe projected) row type of the record batch.
         */
        // 根据 Schema ID 获取对应的逻辑行类型（RowType）
        RowType getRowType(int schemaId);

        /**
         * Gets the Arrow {@link VectorSchemaRoot} for the given schema id. The returned schema root
         * is projected if the record batch is a projected {@link LogRecordBatch}.
         *
         * <p>The schema root is used to read the Arrow records in the batch, if this is a {@link
         * LogFormat#ARROW} record batch.
         *
         * <p>Note: DO NOT close the vector schema root because it is shared across multiple
         * batches. Use {@link VectorSchemaRoot#slice(int)} to cache the root and close it after
         * use.
         *
         * @param schemaId The schema id of the record batch.
         * @return The (maybe projected) schema root of the record batch.
         */
        // 专门针对 Arrow 格式，获取内存中的向量根对象。
        VectorSchemaRoot getVectorSchemaRoot(int schemaId);

        /** Gets the buffer allocator. */
        // 提供 Arrow 读取所需的内存分配器。
        BufferAllocator getBufferAllocator();

        /**
         * If the read context defines an output projection (for example, log records may add new
         * columns or reorder columns, but reader need a static schema for the output rows), return
         * a {@link ProjectedRow} that describes the projected output row for the given schemaId.
         * The returned object is used by readers to transform or materialize rows according to the
         * output projection. Returns {@code null} if no output projection is configured.
         *
         * @param schemaId the current row schema id
         * @return a {@link ProjectedRow} describing the output projection, or {@code null} if none
         */
        // 如果读取时需要进行列投影（只读某些列），该方法返回描述投影关系的 ProjectedRow。
        @Nullable
        ProjectedRow getOutputProjectedRow(int schemaId);
    }
}
