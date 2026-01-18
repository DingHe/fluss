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

package org.apache.fluss.flink.source;

import org.apache.fluss.config.Configuration;
import org.apache.fluss.flink.source.deserializer.DeserializerInitContextImpl;
import org.apache.fluss.flink.source.deserializer.FlussDeserializationSchema;
import org.apache.fluss.flink.source.emitter.FlinkRecordEmitter;
import org.apache.fluss.flink.source.enumerator.FlinkSourceEnumerator;
import org.apache.fluss.flink.source.enumerator.initializer.OffsetsInitializer;
import org.apache.fluss.flink.source.metrics.FlinkSourceReaderMetrics;
import org.apache.fluss.flink.source.reader.FlinkSourceReader;
import org.apache.fluss.flink.source.reader.RecordAndPos;
import org.apache.fluss.flink.source.split.SourceSplitBase;
import org.apache.fluss.flink.source.split.SourceSplitSerializer;
import org.apache.fluss.flink.source.state.FlussSourceEnumeratorStateSerializer;
import org.apache.fluss.flink.source.state.SourceEnumeratorState;
import org.apache.fluss.lake.source.LakeSource;
import org.apache.fluss.lake.source.LakeSplit;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.predicate.Predicate;
import org.apache.fluss.types.RowType;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import javax.annotation.Nullable;

import static org.apache.fluss.config.ConfigOptions.CLIENT_SCANNER_IO_TMP_DIR;
import static org.apache.fluss.flink.utils.FlinkConnectorOptionsUtils.getClientScannerIoTmpDir;

/** Flink source for Fluss. */
// 主要作用是连接 Flink 运行时环境与 Fluss 存储集群
// 配置持有者：保存读取 Fluss 表所需的所有元数据（如表路径、字段投影、分区过滤等）。
// 组件工厂：负责在 Flink 任务启动时创建 SplitEnumerator（用于发现和分配分片）和 SourceReader（用于实际读取数据）。
// 行为定义：定义数据源是流式的还是批次的，并提供序列化工具以支持 Flink 的状态快照（Checkpoint）和恢复。
// 类型映射：将 Fluss 的数据类型（RowType）转换为 Flink 的类型系统（TypeInformation）。
public class FlinkSource<OUT>
        implements Source<OUT, SourceSplitBase, SourceEnumeratorState>, ResultTypeQueryable {
    private static final long serialVersionUID = 1L;
    // 存储 Fluss 客户端的相关配置（如服务器地址、超时时间等）。
    private final Configuration flussConf;
    // 标识要读取的 Fluss 表的完整路径（数据库名.表名）。
    private final TablePath tablePath;
    // 标识目标表是否有主键。这决定了是否需要使用 HybridSnapshotLogSplit。
    private final boolean hasPrimaryKey;
    // 标识目标表是否为分区表
    private final boolean isPartitioned;
    // Fluss 层的原始输出行类型，定义了数据的 Schema。
    private final RowType sourceOutputType;
    // 字段投影索引。如果只查询部分列，这里记录了需要读取的列索引。
    @Nullable private final int[] projectedFields;
    // 定义起始消费位置（例如：从最早位置、最新位置或特定时间戳开始）。
    protected final OffsetsInitializer offsetsInitializer;
    // 自动发现新分区的间隔时间。
    protected final long scanPartitionDiscoveryIntervalMs;
    // 标识是流模式（持续读取）还是批模式（读完快照即结束）。
    private final boolean streaming;
    // 反序列化器，负责将 Fluss 的二进制记录转换为 Flink 的输出对象类型 OUT。
    private final FlussDeserializationSchema<OUT> deserializationSchema;
    // 分区过滤谓词，用于在源头进行分区裁剪，减少不必要的数据扫描。
    @Nullable private final Predicate partitionFilters;
    // 湖仓一体支持。如果涉及读取湖底层的存量数据，该对象提供湖分片的获取逻辑。
    @Nullable private final LakeSource<LakeSplit> lakeSource;

    public FlinkSource(
            Configuration flussConf,
            TablePath tablePath,
            boolean hasPrimaryKey,
            boolean isPartitioned,
            RowType sourceOutputType,
            @Nullable int[] projectedFields,
            OffsetsInitializer offsetsInitializer,
            long scanPartitionDiscoveryIntervalMs,
            FlussDeserializationSchema<OUT> deserializationSchema,
            boolean streaming,
            @Nullable Predicate partitionFilters) {
        this(
                flussConf,
                tablePath,
                hasPrimaryKey,
                isPartitioned,
                sourceOutputType,
                projectedFields,
                offsetsInitializer,
                scanPartitionDiscoveryIntervalMs,
                deserializationSchema,
                streaming,
                partitionFilters,
                null);
    }

    public FlinkSource(
            Configuration flussConf,
            TablePath tablePath,
            boolean hasPrimaryKey,
            boolean isPartitioned,
            RowType sourceOutputType,
            @Nullable int[] projectedFields,
            OffsetsInitializer offsetsInitializer,
            long scanPartitionDiscoveryIntervalMs,
            FlussDeserializationSchema<OUT> deserializationSchema,
            boolean streaming,
            @Nullable Predicate partitionFilters,
            @Nullable LakeSource<LakeSplit> lakeSource) {
        this.flussConf = flussConf;
        this.tablePath = tablePath;
        this.hasPrimaryKey = hasPrimaryKey;
        this.isPartitioned = isPartitioned;
        this.sourceOutputType = sourceOutputType;
        this.projectedFields = projectedFields;
        this.offsetsInitializer = offsetsInitializer;
        this.scanPartitionDiscoveryIntervalMs = scanPartitionDiscoveryIntervalMs;
        this.deserializationSchema = deserializationSchema;
        this.streaming = streaming;
        this.partitionFilters = partitionFilters;
        this.lakeSource = lakeSource;
    }

    @Override
    public Boundedness getBoundedness() {
        return streaming ? Boundedness.CONTINUOUS_UNBOUNDED : Boundedness.BOUNDED;
    }

    @Override
    public SplitEnumerator<SourceSplitBase, SourceEnumeratorState> createEnumerator(
            SplitEnumeratorContext<SourceSplitBase> splitEnumeratorContext) {
        return new FlinkSourceEnumerator(
                tablePath,
                flussConf,
                hasPrimaryKey,
                isPartitioned,
                splitEnumeratorContext,
                offsetsInitializer,
                scanPartitionDiscoveryIntervalMs,
                streaming,
                partitionFilters,
                lakeSource);
    }

    @Override
    public SplitEnumerator<SourceSplitBase, SourceEnumeratorState> restoreEnumerator(
            SplitEnumeratorContext<SourceSplitBase> splitEnumeratorContext,
            SourceEnumeratorState sourceEnumeratorState) {
        return new FlinkSourceEnumerator(
                tablePath,
                flussConf,
                hasPrimaryKey,
                isPartitioned,
                splitEnumeratorContext,
                sourceEnumeratorState.getAssignedBuckets(),
                sourceEnumeratorState.getAssignedPartitions(),
                sourceEnumeratorState.getRemainingHybridLakeFlussSplits(),
                offsetsInitializer,
                scanPartitionDiscoveryIntervalMs,
                streaming,
                partitionFilters,
                lakeSource);
    }

    @Override
    public SimpleVersionedSerializer<SourceSplitBase> getSplitSerializer() {
        return new SourceSplitSerializer(lakeSource);
    }

    @Override
    public SimpleVersionedSerializer<SourceEnumeratorState> getEnumeratorCheckpointSerializer() {
        return new FlussSourceEnumeratorStateSerializer(lakeSource);
    }

    @Override
    public SourceReader<OUT, SourceSplitBase> createReader(SourceReaderContext context)
            throws Exception {
        FutureCompletingBlockingQueue<RecordsWithSplitIds<RecordAndPos>> elementsQueue =
                new FutureCompletingBlockingQueue<>();
        FlinkSourceReaderMetrics flinkSourceReaderMetrics =
                new FlinkSourceReaderMetrics(context.metricGroup());

        flussConf.set(
                CLIENT_SCANNER_IO_TMP_DIR,
                getClientScannerIoTmpDir(flussConf, context.getConfiguration()));
        deserializationSchema.open(
                new DeserializerInitContextImpl(
                        context.metricGroup().addGroup("deserializer"),
                        context.getUserCodeClassLoader(),
                        sourceOutputType));
        FlinkRecordEmitter<OUT> recordEmitter = new FlinkRecordEmitter<>(deserializationSchema);

        return new FlinkSourceReader<>(
                elementsQueue,
                flussConf,
                tablePath,
                sourceOutputType,
                context,
                projectedFields,
                flinkSourceReaderMetrics,
                recordEmitter,
                lakeSource);
    }

    @Override
    public TypeInformation<OUT> getProducedType() {
        return deserializationSchema.getProducedType(sourceOutputType);
    }
}
