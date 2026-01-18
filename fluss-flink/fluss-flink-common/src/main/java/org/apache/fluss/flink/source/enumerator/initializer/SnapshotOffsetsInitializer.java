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

import javax.annotation.Nullable;

import java.util.Collection;
import java.util.Map;

/**
 * An implementation of {@link OffsetsInitializer} which initializes the offsets.
 *
 * <p>If the user don't specify the offsets for a bucket, the initial offset of the bucket will be
 * the earliest offset.
 *
 * <p>Package private and should be instantiated via {@link OffsetsInitializer}.
 */
// 主要作用是为“全量读取”模式定义回退的起始位点。
// 在 Fluss 中，当你调用 OffsetsInitializer.full() 时，系统默认希望先读取“湖”中的快照（Snapshot），再读取“仓”中的增量日志。然而，会存在以下特殊情况：
// 快照不存在：如果这张表是新创建的，或者湖同步（Lake Sync）任务还没有完成，此时没有可用的 KV 快照或湖快照。
// 日志表：对于没有主键的日志表，其“全量”定义就是从日志的最早位置开始。
// 逻辑核心是：如果无法基于快照启动，则降级为从最早可用的日志位点（Earliest Offset）开始消费。
public class SnapshotOffsetsInitializer implements OffsetsInitializer {
    private static final long serialVersionUID = 1649702397250402877L;

    /**
     * For table with primary key. This method will be invoked only when the kv snapshot not exists.
     */
    @Override
    public Map<Integer, Long> getBucketOffsets(
            @Nullable String partitionName,
            Collection<Integer> buckets,
            BucketOffsetsRetriever bucketOffsetsRetriever) {
        return bucketOffsetsRetriever.earliestOffsets(partitionName, buckets);
    }
}
