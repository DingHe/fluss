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

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.memory.MemorySegment;

/** Provides memory ({@link MemorySegment}) related getters. */
// 定义了如何从一个**基于内存段（Memory Segment）**构建的对象中提取其物理存储信息。
// 在 Fluss 这种追求极致性能的系统中，数据往往不是以传统的 Java 对象形式存在，而是直接存储在连续的内存块中。
// 该接口的作用就是“解开”业务对象（如 BinaryRow）的包装，暴露出它在内存中的具体位置和大小。
// 零拷贝（Zero-copy）支持：当系统需要将一行数据写入磁盘、发送到网络，或者在不同的内存区域移动时，如果能直接获取它所在的内存段和偏移量，就可以通过 memcpy 或 FileChannel.write(ByteBuffer) 直接操作物理内存，而不需要先将其反序列化为 Java 对象。
// 内存管理桥梁：它将逻辑上的“数据对象”（如一行记录）与物理上的“内存管理单元”（MemorySegment）联系起来。
@Internal
public interface MemoryAwareGetters {

    /** Gets the underlying {@link MemorySegment}s this binary format spans. */
    // TODO: maybe we only need a single MemorySegment.
    // 获取该数据对象所跨越的底层 MemorySegment 数组。
    MemorySegment[] getSegments();

    /** Gets the start offset of this binary data in the {@link MemorySegment}s. */
    // 获取该数据对象在 MemorySegment 中的起始偏移量（Start Offset）。
    int getOffset();

    /** Gets the size in bytes of this binary data. */
    // 获取该数据对象占据的总字节大小。
    int getSizeInBytes();
}
