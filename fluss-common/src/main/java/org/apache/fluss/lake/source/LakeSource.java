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
import org.apache.fluss.lake.serializer.SimpleVersionedSerializer;
import org.apache.fluss.predicate.Predicate;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;

/**
 * A generic interface for lake data sources that defines how to plan splits and read data. Any data
 * lake format supporting reading from data tiered in lake as Fluss records should implement this
 * interface.
 *
 * <p>This interface provides methods for projection, filtering, limiting to enable query engine to
 * push to lake source. Implementations must ensure that split planning and record reading
 * operations properly account for these pushed-down operations during execution.
 *
 * @param <Split> The type of data split, which must extend {@link LakeSplit}
 * @since 0.8
 */
// LakeSource 接口是 Fluss 与“湖仓一体”架构（如 Apache Paimon, Iceberg 等数据湖格式）对接的核心枢纽。
// LakeSource 是一个泛型接口，定义了如何从数据湖（Data Lake）中读取分层存储（Tiered Storage）数据的标准规范。
// 在 Fluss 的架构中，历史数据或非热数据会被“归档”或“分层”到对象存储（如 S3/OSS）的数据湖格式中。LakeSource 的作用就是：
// 作为适配器：让 Fluss 能够对接不同的湖格式（如 Paimon, Iceberg）。
// 支持算子下推：提供投影（Projection）、过滤（Filtering）和限制（Limiting）接口，允许查询引擎将这些操作直接下推到湖源码头，减少网络 IO 和计算开销。
// 规划读取逻辑：通过 Planner 将湖中的文件切分成多个 Split（分片），并通过 RecordReader 实现分布式并行读取。


@PublicEvolving
public interface LakeSource<Split extends LakeSplit> extends Serializable {

    /**
     * Applies column projection to the data source. it provides the field index paths that should
     * be used for a projection. The indices are 0-based and support fields within (possibly nested)
     * structures.
     *
     * <p>For nested, given the following SQL, CREATE TABLE t (i INT, r ROW < d DOUBLE, b BOOLEAN>,
     * s STRING); SELECT s, r.d FROM t; the project will be [[2], [1, 0]]
     */
    // 应用列投影。
    // 告诉数据源只需要读取哪些列。
    void withProject(int[][] project);

    /** Applies a row limit to the lake source. */
    // 应用行限制。
    // 告诉数据源最多读取多少行。如果湖格式支持索引或元数据统计，可以在达到阈值后提前停止读取。
    void withLimit(int limit);

    /** Applies filters to the lake source. */
    // 应用谓词过滤（Filter Pushdown）
    FilterPushDownResult withFilters(List<Predicate> predicates);

    /**
     * Creates a planner for plan splits to be read.
     *
     * @param context The planning context providing necessary planning information
     * @return A planner instance for this lake source
     * @throws IOException if an error occurs during planner creation
     */
    // 创建切片规划器
    Planner<Split> createPlanner(PlannerContext context) throws IOException;

    /**
     * Creates a record reader for reading data from the lake source for the specified split.
     *
     * @param context The reader context containing the split to be read
     * @return A record reader instance for the given split
     * @throws IOException if an error occurs during reader creation
     */
    // 创建数据读取器。
    // 这是真正执行读取任务的组件。它接收一个具体的 Split，打开文件，并将其转换为 Fluss 内部的记录格式。
    RecordReader createRecordReader(ReaderContext<Split> context) throws IOException;

    /**
     * Returns the serializer for the data split, used to transfer split information in distributed
     * environment.
     *
     * @return The serializer for the split
     */
    // 获取 Split 序列化器。
    // 在分布式系统中，Planner 生成的 Split 信息需要通过网络传输给不同的执行节点（Worker/TaskExecutor）。
    // 该序列化器确保这些元数据能正确地跨进程传输。
    SimpleVersionedSerializer<Split> getSplitSerializer();

    /**
     * Context interface for planners, providing the snapshot id of the table in data-lake to plan
     * splits.
     */
    interface PlannerContext extends Serializable {
        // 方法 long snapshotId()：提供数据湖表的快照 ID。
        // 确保读取的是数据在某一特定时刻的状态，保证数据一致性（快照读）。
        long snapshotId();
    }

    /**
     * Context interface for record readers, providing access to the lake split being read.
     *
     * @param <Split> The type of lake split
     */
    interface ReaderContext<Split extends LakeSplit> extends Serializable {
        // 方法 Split lakeSplit()：为 RecordReader 提供当前需要读取的具体分片信息（如文件路径、偏移量等）。
        Split lakeSplit();
    }

    /**
     * Represents the result of a filter push down operation to lake source, indicating which
     * predicates were accepted by the source and which remain to be evaluated.
     *
     * @since 0.8
     */
    @PublicEvolving
    final class FilterPushDownResult {
        // 返回湖数据源已经承接并会内部执行的过滤条件。
        private final List<Predicate> acceptedPredicates;
        // 返回湖数据源无法处理，需要计算引擎在读取数据后再过滤一遍的剩余条件。
        private final List<Predicate> remainingPredicates;

        private FilterPushDownResult(
                List<Predicate> acceptedPredicates, List<Predicate> remainingPredicates) {
            this.acceptedPredicates = acceptedPredicates;
            this.remainingPredicates = remainingPredicates;
        }

        /**
         * Creates a new FilterPushDownResult instance.
         *
         * @param acceptedPredicates The accepted predicates
         * @param remainingPredicates The remaining predicates
         * @return A new FilterPushDownResult instance
         */
        public static FilterPushDownResult of(
                List<Predicate> acceptedPredicates, List<Predicate> remainingPredicates) {
            return new FilterPushDownResult(acceptedPredicates, remainingPredicates);
        }

        /**
         * Returns the predicates that were accepted by the source.
         *
         * @return The list of accepted predicates
         */
        public List<Predicate> acceptedPredicates() {
            return acceptedPredicates;
        }

        /**
         * Returns the predicates that remain to be evaluated.
         *
         * @return The list of remaining predicates
         */
        public List<Predicate> remainingPredicates() {
            return remainingPredicates;
        }
    }
}
