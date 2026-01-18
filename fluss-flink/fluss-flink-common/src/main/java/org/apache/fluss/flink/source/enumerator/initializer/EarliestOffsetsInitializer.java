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

package org.apache.fluss.flink.source.enumerator.initializer;

import org.apache.fluss.flink.source.enumerator.FlinkSourceEnumerator;
import org.apache.fluss.flink.source.reader.FlinkSourceSplitReader;

import javax.annotation.Nullable;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static org.apache.fluss.client.table.scanner.log.LogScanner.EARLIEST_OFFSET;

/**
 * A initializer that initialize the buckets to the earliest offsets. The offsets initialization are
 * taken care of by the {@link FlinkSourceSplitReader} instead of by the {@link
 * FlinkSourceEnumerator}.
 *
 * <p>Package private and should be instantiated via {@link OffsetsInitializer}.
 */
// 主要作用是指示 Flink 从 Fluss 桶（Bucket）中最早可用的数据开始消费。
// 全量追溯：它告诉 Flink 读取该表日志文件中现存的所有历史数据。
class EarliestOffsetsInitializer implements OffsetsInitializer {
    private static final long serialVersionUID = 172938052008787981L;

    @Override
    public Map<Integer, Long> getBucketOffsets(
            @Nullable String partitionName,
            Collection<Integer> buckets,
            BucketOffsetsRetriever bucketOffsetsRetriever) {
        Map<Integer, Long> initialOffsets = new HashMap<>();
        for (Integer tb : buckets) {
            // EARLIEST_OFFSET 通常定义为 -2L（或类似标识值）。这并不是一个真实的物理文件偏移量，而是一个占位符。
            // 在 Earliest 模式下，这个类选择不查。它直接返回占位符。当 SourceReader 真正开始读取数据发现 Offset 是 EARLIEST_OFFSET 时，
            // 它会自行在 TaskManager 端去查询该桶当前物理上最早的位点（因为日志可能因为 TTL 被清理，最早位点会随时间变化）。
            initialOffsets.put(tb, EARLIEST_OFFSET);
        }
        return initialOffsets;
    }
}
