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

package org.apache.fluss.server.log.checkpoint;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.exception.LogStorageException;
import org.apache.fluss.metadata.TableBucket;

import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;

/* This file is based on source code of Apache Kafka Project (https://kafka.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/**
 * this class persists a map of (Bucket => Offsets) to a file (for a certain replica).
 *
 * <pre>
 * The format in the offset checkpoint file is like this:
 * ------ checkpoint file begin -----
 * 0              <- OffsetCheckpointFile.currentVersion
 * 2              <- following entries size
 * 150001 [20241121] 0 10    <- (TableBucket.tableId, [TableBucket.partitionId], TableBucket.bucket, Offset)
 * 150001 [20241121] 1 5
 * ----- checkpoint file end  ------
 * </pre>
 */
// 借鉴了 Apache Kafka 的设计，主要负责将内存中各个分桶（Bucket）的**位点（Offsets）**信息持久化到物理磁盘文件中。
// 在分布式存储系统中，位点信息（如 High Watermark, Recovery Point 等）至关重要。该类的核心作用是：
// 持久化存储：将 TableBucket -> Offset 的映射关系以文本形式保存到文件中。
// 故障恢复：当 Tablet Server 重启时，系统可以通过读取该文件，快速恢复各个分桶的消费或同步进度，而不需要重新扫描所有日志。
// 版本管理：支持文件格式的版本化（当前为版本 0），便于未来升级扩展。
@Internal
public final class OffsetCheckpointFile {
    // 正规表达式 \s+，用于在解析文件内容时识别一个或多个空格，以此拆分每一行的字段。
    private static final Pattern WHITE_SPACES_PATTERN = Pattern.compile("\\s+");
    // 设为 0，标识当前检查点文件的格式版本。
    static final int CURRENT_VERSION = 0;
    // 封装了具体的文件 I/O 操作（如原子写入、临时文件重命名等），以保证文件写入的安全性。
    // 先写入临时文件，再重命名
    private final CheckpointFile<Pair<TableBucket, Long>> checkpoint;

    public OffsetCheckpointFile(File file) throws IOException {
        this.checkpoint =
                new CheckpointFile<>(
                        file,
                        OffsetCheckpointFile.CURRENT_VERSION,
                        new OffsetCheckpointFile.Formatter());
    }
    // 将内存中的位点 Map 批量写入文件。
    public void write(Map<TableBucket, Long> offsets) {
        List<Pair<TableBucket, Long>> list = new ArrayList<>(offsets.size());
        for (Map.Entry<TableBucket, Long> entry : offsets.entrySet()) {
            list.add(new MutablePair<>(entry.getKey(), entry.getValue()));
        }
        try {
            checkpoint.write(list);
        } catch (IOException e) {
            String msg = "Error while writing to checkpoint file " + checkpoint.getAbsolutePath();
            throw new LogStorageException(msg, e);
        }
    }
    // 从磁盘读取并解析所有位点信息。
    public Map<TableBucket, Long> read() {
        List<Pair<TableBucket, Long>> list;
        try {
            list = checkpoint.read();
        } catch (IOException e) {
            String msg = "Error while reading checkpoint file " + checkpoint.getAbsolutePath();
            throw new LogStorageException(msg, e);
        }

        Map<TableBucket, Long> result = new HashMap<>();
        for (Pair<TableBucket, Long> pair : list) {
            result.put(pair.getKey(), pair.getValue());
        }
        return result;
    }

    /** Formatter for offset checkpoint file. */
    public static class Formatter
            implements CheckpointFile.EntryFormatter<Pair<TableBucket, Long>> {

        @Override
        public String toString(Pair<TableBucket, Long> entry) {
            TableBucket tableBucket = entry.getLeft();
            long offset = entry.getRight();
            if (tableBucket.getPartitionId() == null) {
                return tableBucket.getTableId() + " " + tableBucket.getBucket() + " " + offset;
            } else {
                return tableBucket.getTableId()
                        + " "
                        + tableBucket.getPartitionId()
                        + " "
                        + tableBucket.getBucket()
                        + " "
                        + offset;
            }
        }

        @Override
        public Optional<Pair<TableBucket, Long>> fromString(String line) {
            String[] parts = WHITE_SPACES_PATTERN.split(line);
            if (parts.length == 3) {
                int tableId = Integer.parseInt(parts[0]);
                int bucketId = Integer.parseInt(parts[1]);
                long offset = Long.parseLong(parts[2]);
                return Optional.of(new MutablePair<>(new TableBucket(tableId, bucketId), offset));
            } else if (parts.length == 4) {
                int tableId = Integer.parseInt(parts[0]);
                long partitionId = Long.parseLong(parts[1]);
                int bucketId = Integer.parseInt(parts[2]);
                long offset = Long.parseLong(parts[3]);
                return Optional.of(
                        new MutablePair<>(new TableBucket(tableId, partitionId, bucketId), offset));
            } else {
                return Optional.empty();
            }
        }
    }

    /** Loads checkpoint file on demand and caches the offsets for reuse. */
    public static class LazyOffsetCheckpoints {
        private final OffsetCheckpointFile checkpoint;
        private Map<TableBucket, Long> offsets;

        public LazyOffsetCheckpoints(OffsetCheckpointFile checkpoint) {
            this.checkpoint = checkpoint;
            this.offsets = null;
        }

        private Map<TableBucket, Long> getOffsets() {
            if (offsets == null) {
                offsets = checkpoint.read();
            }
            return offsets;
        }

        public Optional<Long> fetch(TableBucket tableBucket) {
            return Optional.ofNullable(getOffsets().get(tableBucket));
        }
    }
}
