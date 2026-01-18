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

/**
 * Interface for accessing the records contained in a log. The log itself is represented as a
 * sequence of record batches (see {@link LogRecordBatch}).
 *
 * @since 0.1
 */
// 定义了以**日志（Log）**格式存储数据的基本访问规范。在 Fluss 这种流式存储系统中，日志数据并不是一条条独立存储的，而是以“批次”为单位组织的。
// 流式日志记录的抽象容器
@PublicEvolving
public interface LogRecords {
    /**
     * The size of these records in bytes.
     *
     * @return The size in bytes of the records
     */
    // 获取这组日志记录所占用的物理字节大小
    int sizeInBytes();

    /**
     * Get the record batches. Note that the signature allows subclasses to return a more specific
     * batch type.
     *
     * @return An iterator over the record batches of the log
     */
    // 获取日志记录中所有**记录批次（Record Batches）**的迭代器。
    Iterable<LogRecordBatch> batches();
}
