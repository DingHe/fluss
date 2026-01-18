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

package org.apache.fluss.metadata;

import org.apache.fluss.annotation.PublicEvolving;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Objects;

/**
 * A class to identify a table bucket, containing:
 *
 * <ul>
 *   <li>the table id
 *   <li>the bucket num
 *   <li>the partition id of the table bucket. if the table bucket doesn't belong to a partition,
 *       this field will be null
 * </ul>
 *
 * @since 0.1
 */
// 分布式流式存储系统中，它扮演着“逻辑寻址标签”的角色。
// TableBucket 的主要作用是唯一标识 Fluss 集群中的一个数据分桶（Bucket）。
// Table（表） 是数据的逻辑集合。
// Partition（分区） 是可选的逻辑层（例如按天分区）。
// Bucket（分桶） 是物理存储和并行读写的最小单位。
// TableBucket 整合了表 ID、分区 ID 和桶索引，成为了 Fluss 内部在进行数据分发（Produce）、任务调度（Assignment）以及元数据管理时的通用键（Key）。
@PublicEvolving
public class TableBucket implements Serializable {

    private static final long serialVersionUID = 1L;
    // 所属表的唯一标识符（ID）
    private final long tableId;
    // 桶的索引编号。
    private final int bucket;

    // will be null if the bucket doesn't belong to a partition
    // 所属分区的唯一标识符（ID）
    private final @Nullable Long partitionId;

    // Cache hashCode as it is called in performance sensitive parts of the code (e.g.
    // RecordAccumulator.ready)
    private Integer hash;

    public TableBucket(long tableId, int bucket) {
        this(tableId, null, bucket);
    }

    public TableBucket(long tableId, @Nullable Long partitionId, int bucket) {
        this.tableId = tableId;
        this.partitionId = partitionId;
        this.bucket = bucket;
    }

    public int getBucket() {
        return bucket;
    }

    public long getTableId() {
        return tableId;
    }

    @Nullable
    public Long getPartitionId() {
        return partitionId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TableBucket that = (TableBucket) o;
        return tableId == that.tableId
                && bucket == that.bucket
                && Objects.equals(partitionId, that.partitionId);
    }

    @Override
    public int hashCode() {
        Integer h = this.hash;
        if (h == null) {
            int result = Objects.hash(tableId, bucket, partitionId);
            this.hash = result;
            return result;
        } else {
            return h;
        }
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder("TableBucket{tableId=");
        builder.append(tableId);
        if (partitionId == null) {
            builder.append(", bucket=").append(bucket);
        } else {
            builder.append(", partitionId=").append(partitionId).append(", bucket=").append(bucket);
        }
        builder.append('}');
        return builder.toString();
    }
}
