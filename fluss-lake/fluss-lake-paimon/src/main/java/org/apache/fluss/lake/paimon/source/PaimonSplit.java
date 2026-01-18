/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.lake.paimon.source;

import org.apache.fluss.lake.source.LakeSplit;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.table.source.DataSplit;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Split for paimon table. */
// PaimonSplit 是 Fluss 与 Apache Paimon 数据湖格式对接的具体实现类。它实现了 LakeSplit 接口，充当了 Fluss 与 Paimon 之间的“翻译官”。
public class PaimonSplit implements LakeSplit {
    // 持有 Paimon 原生的数据切片对象
    private final DataSplit dataSplit;
    // 标记当前表是否为“非桶感知（Bucket-unaware）”模式。
    // Paimon 支持“有桶”和“无桶”模式。如果是无桶模式（通常用于 Append-only 表），数据不根据 Key 分布到特定的 Bucket，此时读取策略会有所不同。
    private final boolean isBucketUnAware;

    public PaimonSplit(DataSplit dataSplit, boolean isBucketUnAware) {
        this.dataSplit = dataSplit;
        this.isBucketUnAware = isBucketUnAware;
    }

    @Override
    public int bucket() {
        if (isBucketUnAware) {
            // bucket-unaware table returns -1
            return -1;
        }
        return dataSplit.bucket();
    }

    @Override
    public List<String> partition() {
        BinaryRow partition = dataSplit.partition();
        if (partition.getFieldCount() == 0) {
            return Collections.emptyList();
        }

        List<String> partitions = new ArrayList<>();
        for (int i = 0; i < partition.getFieldCount(); i++) {
            // Todo Currently, partition column must be String datatype, so we can always use
            // consider it as string. Revisit here when
            // #489 is finished.
            partitions.add(partition.getString(i).toString());
        }
        return partitions;
    }

    public DataSplit dataSplit() {
        return dataSplit;
    }

    public boolean isBucketUnAware() {
        return isBucketUnAware;
    }

    @Override
    public String toString() {
        return "PaimonSplit{"
                + "dataSplit="
                + dataSplit
                + ", isBucketUnAware="
                + isBucketUnAware
                + '}';
    }
}
