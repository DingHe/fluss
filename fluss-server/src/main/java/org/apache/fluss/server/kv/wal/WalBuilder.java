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

package org.apache.fluss.server.kv.wal;

import org.apache.fluss.record.ChangeType;
import org.apache.fluss.record.MemoryLogRecords;
import org.apache.fluss.row.InternalRow;

/** The interface to build write-ahead-log batch ({@link MemoryLogRecords}) for kv store. */
// WalBuilder（Write-Ahead-Log Builder）是一个核心接口，专门用于将 KV 存储的变更操作转化为持久化的日志记录。
// WalBuilder 的主要作用是 构建预写日志（WAL）的数据批次。
// 在 Fluss 的主键表（Primary Key Table）中，数据不仅要写入 KV 存储（RocksDB），还要生成对应的变更日志（CDC Log）。WalBuilder 充当了一个“收集器”和“格式化器”的角色：
// 统一格式：它将不同类型的物理变更（插入、更新、删除）统一封装进一个日志批次（MemoryLogRecords）。
// 解耦存储与日志：无论底层的日志存储格式是 Arrow 还是其他格式，KvTablet 都通过这个接口进行写入。
// 支持幂等性：它负责携带写入者的状态信息，确保在网络重试等异常情况下数据不会重复。
public interface WalBuilder {
    // 向当前正在构建的日志批次中添加一条变更记录。
    void append(ChangeType changeType, InternalRow row) throws Exception;
    // 完成构建过程，并将缓冲区中的数据封装成可传输、可落盘的 MemoryLogRecords 对象。
    MemoryLogRecords build() throws Exception;
    // 设置当前写入批次的元数据状态，用于实现 幂等性（Idempotency）
    void setWriterState(long writerId, int batchSequence);
    // 释放构建过程中占用的系统资源。
    // Fluss 为了高性能大量使用了 堆外内存（Off-heap Memory） 和 Arrow 内存缓冲区。
    void deallocate();
}
