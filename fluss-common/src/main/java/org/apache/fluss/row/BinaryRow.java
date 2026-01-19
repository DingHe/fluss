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

package org.apache.fluss.row;

import org.apache.fluss.memory.MemorySegment;
import org.apache.fluss.row.aligned.AlignedRow;
import org.apache.fluss.row.compacted.CompactedRow;
import org.apache.fluss.row.indexed.IndexedRow;

/**
 * A binary format {@link InternalRow} that is backed on {@link MemorySegment} and supports all
 * interfaces provided by {@link MemoryAwareGetters}.
 */
// 实现“逻辑-物理”绑定：它既能像普通行一样通过 getInt 等方法读取字段，又能暴露其底层在 MemorySegment 中的物理位置。
// 支持高性能零拷贝：它是“可指向”的。通过 pointTo 方法，一个 BinaryRow 对象可以像指针一样瞬间切换到另一块内存区域，从而避免了将大量数据从内存拷贝到 Java 对象的开销。
// 统一不同二进制布局：Fluss 存在多种二进制布局（紧凑型、对齐型、索引型），BinaryRow 为这些不同的底层实现提供了统一的操作界面。

public interface BinaryRow extends InternalRow, MemoryAwareGetters {

    /**
     * Copies the bytes of the row to the destination memory, beginning at the given offset.
     *
     * @param dst The memory into which the bytes will be copied.
     * @param dstOffset The copying offset in the destination memory.
     */
    // 将当前行对象所代表的二进制字节数据拷贝到一个目标字节数组中。
    // dst: 目标字节数组。
    // dstOffset: 在目标数组中的起始写入位置。
    // 应用场景：通常用于将内存中的行数据序列化到磁盘缓冲区，或者准备发送到网络。
    void copyTo(byte[] dst, int dstOffset);

    /**
     * Copy the bytes of the row to the destination memory, beginning at the given offset.
     *
     * @return The copied row.
     */
    // 创建当前行的一个深度拷贝（Deep Copy）
    BinaryRow copy();

    /**
     * Point to the bytes of the row.
     *
     * @param segment The memory segment.
     * @param offset The offset in the memory segment.
     * @param sizeInBytes The size of the row.
     */
    // 将当前的 BinaryRow 对象“指向”一个单段内存区域。
    void pointTo(MemorySegment segment, int offset, int sizeInBytes);

    /**
     * Point to the bytes of the row.
     *
     * @param segments The memory segments.
     * @param offset The offset in the memory segments.
     * @param sizeInBytes The size of the row.
     */
    // 将行对象“指向”一组可能跨多个段的内存区域。
    void pointTo(MemorySegment[] segments, int offset, int sizeInBytes);

    /**
     * The binary row format types, it indicates the generated {@link BinaryRow} type by the {@link
     * BinaryWriter}.
     */
    enum BinaryRowFormat {

        /** Compacted binary row format, see {@link CompactedRow}. */
        // 极致紧凑。
        // 不存储额外偏移量，空间占用最小，适合冷数据存储，但随机字段访问开销略大。
        COMPACTED,

        /** Aligned binary row format, see {@link AlignedRow}. */
        // 内存对齐。字段按 4/8 字节对齐，利用 CPU 缓存特性提高读取速度。
        ALIGNED,

        /** Indexed binary row format, see {@link IndexedRow}. */
        // 带索引。在行头部存储字段偏移量索引，支持 $O(1)$ 时间复杂度的随机字段访问，适合频繁读取。
        INDEXED
    }
}
