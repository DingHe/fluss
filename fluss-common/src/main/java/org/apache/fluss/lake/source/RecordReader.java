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

package org.apache.fluss.lake.source;

import org.apache.fluss.annotation.PublicEvolving;
import org.apache.fluss.record.LogRecord;
import org.apache.fluss.utils.CloseableIterator;

import java.io.IOException;

/**
 * An interface for reading records from {@link LakeSplit}.
 *
 * <p>Implementations of this interface provide an iterator-style access to records, allowing
 * efficient sequential reading of potentially large datasets without loading all data into memory
 * at once. The reading should consider the pushed-down optimizations (project, filters, limits,
 * etc.) from {@link LakeSource}.
 *
 * @since 0.8
 */
// 在 Apache Fluss 的湖仓一体架构中，RecordReader 接口是数据读取路径上的最末端执行者。它负责执行物理读取操作，将存储在外部湖存储（如 S3、HDFS）中的原始文件内容转化为 Fluss 能够理解的内存记录对象。
// 数据格式的转换器与流式提取器
@PublicEvolving
public interface RecordReader {

    /**
     * Read a {@link LakeSplit} into a closeable iterator.
     *
     * @return the closeable iterator of records
     * @throws IOException if an I/O error occurs
     */
    CloseableIterator<LogRecord> read() throws IOException;
}
