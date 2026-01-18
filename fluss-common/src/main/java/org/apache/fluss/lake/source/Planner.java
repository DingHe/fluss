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

import java.io.IOException;
import java.util.List;

/**
 * A planner interface for generating readable splits for lake data sources.
 *
 * <p>Implementations of this interface are responsible for determining how to divide the data into
 * manageable splits that can be read in parallel. The planning should consider the pushed-down
 * optimizations (filters, limits, etc.) from {@link LakeSource}.
 *
 * @param <Split> the type of data split this planner generates, must extend {@link LakeSplit}
 * @since 0.8
 */
// Planner 接口定义了如何将存储在数据湖中的庞大数据集“切分”成可并行处理任务的标准。
// 数据读取任务的拆解者
// 在分布式计算中，面对数据湖（如存储在 S3 或 HDFS 上的 Parquet/Avro 文件）中数以万计的文件，单个节点无法高效处理。Planner 的职责就是：
// 扫描元数据：根据 LakeSource 提供的快照 ID（Snapshot ID），扫描数据湖中对应的文件列表。
// 任务切分：将海量文件逻辑上划分为多个 Split（分片）。每个 Split 通常代表一个文件的一部分或多个小文件的组合。
//
@PublicEvolving
public interface Planner<Split extends LakeSplit> {

    /**
     * Plans and generates a list of readable data splits in parallel.
     *
     * @return the list of readable data splits
     * @throws IOException if an I/O error occurs
     */
    // 执行实际的切分规划逻辑，生成一组可读取的数据分片。
    List<Split> plan() throws IOException;
}
