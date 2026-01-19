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

package org.apache.fluss.server.kv.rowmerger;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.TableConfig;
import org.apache.fluss.metadata.DeleteBehavior;
import org.apache.fluss.metadata.KvFormat;
import org.apache.fluss.metadata.MergeEngineType;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.SchemaGetter;
import org.apache.fluss.record.BinaryValue;

import javax.annotation.Nullable;

import java.util.Optional;

/** A merging interface defines how to merge a new row with existing row. */
// 定义了在 主键表（Primary Key Table） 场景下，当新数据写入时，如何处理主键冲突（即新旧行合并）的核心逻辑。
// 在分布式流存储中，主键表允许用户根据主键更新数据。当一条新的记录进入存储系统时，如果该主键已经存在，系统不能简单地覆盖，因为用户可能配置了不同的合并引擎（Merge Engine）
// RowMerger 的作用就是抽象合并策略。它屏蔽了底层存储细节，让系统能够根据配置灵活地决定：
// 是直接覆盖（Default）？
// 还是保留第一行（First Row）？
// 还是基于版本字段比较（Versioned）？
// 还是进行增量聚合（Aggregation）？
public interface RowMerger {

    /**
     * Merge the old value with the new value.
     *
     * @param oldValue the old value
     * @param newValue the new row
     * @return the merged value, if the returned row is the same to the old row, then nothing
     *     happens to the row (no update, no delete).
     */
    // 定义两个值（旧行和新行）的合并逻辑
    // oldValue 是目前存储在系统中的行，newValue 是当前尝试写入的行。
    // 应用场景：处理 UPSERT（更新或插入）操作。
    BinaryValue merge(BinaryValue oldValue, BinaryValue newValue);

    /**
     * Merge the old row with a delete row.
     *
     * <p>This method will be invoked only when {@link #deleteBehavior()} returns {@link
     * DeleteBehavior#ALLOW}.
     *
     * @param oldRow the old row.
     * @return the merged row, or null if the row is deleted.
     */
    // 作用：定义当接收到删除指令时，如何处理已有的旧行。
    // 返回 null 表示物理删除该行。
    // 返回一个 BinaryValue 则可能表示“逻辑删除”或部分状态更新。
    @Nullable
    BinaryValue delete(BinaryValue oldRow);

    /**
     * The behavior of delete operations on primary key tables.
     *
     * @return {@link DeleteBehavior}
     */
    // 定义该合并引擎是否支持删除操作。
    DeleteBehavior deleteBehavior();

    /**
     * Dynamically configure the target columns to merge and return the effective merger.
     *
     * @param targetColumns the partial update target column positions, null means full update
     * @param latestShemaId the schema id used to generate new rows
     * @param latestSchema the schema used to generate new rows
     */
    // 动态配置部分列更新。
    // targetColumns: 指定要更新的列索引数组。如果为 null，表示整行全量更新。
    // 核心意义：支持高效的 Partial Update（部分列更新），合并器会根据这些索引只修改特定字段。
    RowMerger configureTargetColumns(
            @Nullable int[] targetColumns, short latestShemaId, Schema latestSchema);

    /**
     * Create a row merger based on the given configuration.
     *
     * @param tableConf the table configuration
     * @param kvFormat the kv format
     * @param schemaGetter the schema getter for retrieving schemas by schema id (required for
     *     schema evolution support)
     * @return the created row merger
     */
    // 根据 TableConfig 决定实例化哪个具体的合并实现：
    static RowMerger create(TableConfig tableConf, KvFormat kvFormat, SchemaGetter schemaGetter) {
        Optional<MergeEngineType> mergeEngineType = tableConf.getMergeEngineType();
        @Nullable DeleteBehavior deleteBehavior = tableConf.getDeleteBehavior().orElse(null);

        if (mergeEngineType.isPresent()) {
            switch (mergeEngineType.get()) {
                // 保留首行。如果旧行已存在，则忽略新行，永不更新。
                case FIRST_ROW:
                    return new FirstRowRowMerger(deleteBehavior);
                // 版本比较。通过指定的版本列（Version Column）比较，只有新行版本号更大时才更新。
                case VERSIONED:
                    Optional<String> versionColumn = tableConf.getMergeEngineVersionColumn();
                    if (!versionColumn.isPresent()) {
                        throw new IllegalArgumentException(
                                String.format(
                                        "'%s' must be set for versioned merge engine.",
                                        ConfigOptions.TABLE_MERGE_ENGINE_VERSION_COLUMN.key()));
                    }
                    return new VersionedRowMerger(versionColumn.get(), deleteBehavior);
                // 增量聚合。对字段进行求和（Sum）、最大值（Max）等数学运算。
                case AGGREGATION:
                    return new AggregateRowMerger(tableConf, kvFormat, schemaGetter);
                default:
                    throw new IllegalArgumentException(
                            "Unsupported merge engine type: " + mergeEngineType.get());
            }
        } else {
            // 整行覆盖。新行直接替换旧行。
            return new DefaultRowMerger(kvFormat, deleteBehavior);
        }
    }
}
