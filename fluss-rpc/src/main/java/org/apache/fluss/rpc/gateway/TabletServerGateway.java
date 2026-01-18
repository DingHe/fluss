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

package org.apache.fluss.rpc.gateway;

import org.apache.fluss.rpc.RpcGateway;
import org.apache.fluss.rpc.messages.FetchLogRequest;
import org.apache.fluss.rpc.messages.FetchLogResponse;
import org.apache.fluss.rpc.messages.InitWriterRequest;
import org.apache.fluss.rpc.messages.InitWriterResponse;
import org.apache.fluss.rpc.messages.LimitScanRequest;
import org.apache.fluss.rpc.messages.LimitScanResponse;
import org.apache.fluss.rpc.messages.ListOffsetsRequest;
import org.apache.fluss.rpc.messages.ListOffsetsResponse;
import org.apache.fluss.rpc.messages.LookupRequest;
import org.apache.fluss.rpc.messages.LookupResponse;
import org.apache.fluss.rpc.messages.NotifyKvSnapshotOffsetRequest;
import org.apache.fluss.rpc.messages.NotifyKvSnapshotOffsetResponse;
import org.apache.fluss.rpc.messages.NotifyLakeTableOffsetRequest;
import org.apache.fluss.rpc.messages.NotifyLakeTableOffsetResponse;
import org.apache.fluss.rpc.messages.NotifyLeaderAndIsrRequest;
import org.apache.fluss.rpc.messages.NotifyLeaderAndIsrResponse;
import org.apache.fluss.rpc.messages.NotifyRemoteLogOffsetsRequest;
import org.apache.fluss.rpc.messages.NotifyRemoteLogOffsetsResponse;
import org.apache.fluss.rpc.messages.PrefixLookupRequest;
import org.apache.fluss.rpc.messages.PrefixLookupResponse;
import org.apache.fluss.rpc.messages.ProduceLogRequest;
import org.apache.fluss.rpc.messages.ProduceLogResponse;
import org.apache.fluss.rpc.messages.PutKvRequest;
import org.apache.fluss.rpc.messages.PutKvResponse;
import org.apache.fluss.rpc.messages.StopReplicaRequest;
import org.apache.fluss.rpc.messages.StopReplicaResponse;
import org.apache.fluss.rpc.messages.UpdateMetadataRequest;
import org.apache.fluss.rpc.messages.UpdateMetadataResponse;
import org.apache.fluss.rpc.protocol.ApiKeys;
import org.apache.fluss.rpc.protocol.RPC;

import java.util.concurrent.CompletableFuture;

/** The entry point of RPC gateway interface for tablet server. */
// 定义了客户端（如 Flink）以及协调节点（Coordinator）与 TabletServer（实际存储数据的节点）进行交互的所有核心协议。
// TabletServerGateway 是 TabletServer 的访问入口点。它继承了 AdminReadOnlyGateway（只读元数据访问），并扩展了大量关于数据读写和副本管理的操作。
// 数据读写（Data Plane）：处理 Log（日志）和 KV（键值）数据的写入、拉取、查找。
// 控制指令（Control Plane）：接收来自 Coordinator 的指令，如角色切换（Leader/Isr 变更）、停止副本、更新元数据缓存。
// 异步通知（Notification）：接收关于远程存储（Remote Log）、快照（KV Snapshot）以及湖同步（Lake Sync）进度的通知。
public interface TabletServerGateway extends RpcGateway, AdminReadOnlyGateway {

    /**
     * Notify the bucket leader and isr.
     *
     * @return the response for bucket leader and isr notification
     */
    // notifyLeaderAndIsr: 通知 TabletServer 某个 Bucket 的 Leader 或 ISR（保持同步的副本集合）发生了变更。
    // 这是实现高可用的核心，决定了谁负责写，谁负责备份。
    @RPC(api = ApiKeys.NOTIFY_LEADER_AND_ISR)
    CompletableFuture<NotifyLeaderAndIsrResponse> notifyLeaderAndIsr(
            NotifyLeaderAndIsrRequest notifyLeaderAndIsrRequest);

    /**
     * request send to tablet server to update the metadata cache for every tablet server node,
     * asynchronously.
     *
     * @return the update metadata response
     */
    // 异步更新 TabletServer 节点上的元数据缓存（如节点列表、表信息），确保节点路由信息的准确性。
    @RPC(api = ApiKeys.UPDATE_METADATA)
    CompletableFuture<UpdateMetadataResponse> updateMetadata(UpdateMetadataRequest request);

    /**
     * Stop replica.
     *
     * @return the response for stop replica
     */
    // 指令 TabletServer 停止并清理某个桶的副本（常用于删除表或迁移分片）
    @RPC(api = ApiKeys.STOP_REPLICA)
    CompletableFuture<StopReplicaResponse> stopReplica(StopReplicaRequest stopBucketReplicaRequest);

    /**
     * Produce log data to the specified table bucket.
     *
     * @return the produce response.
     */
    // 写入日志。客户端将数据流写入指定的 Table Bucket。
    @RPC(api = ApiKeys.PRODUCE_LOG)
    CompletableFuture<ProduceLogResponse> produceLog(ProduceLogRequest request);

    /**
     * Fetch log data from the specified table bucket. The request can send by the client scanner or
     * other tablet server.
     *
     * @return the fetch response.
     */
    // 拉取日志。由 Flink 消费任务或副本同步任务调用，用于读取流式数据。
    @RPC(api = ApiKeys.FETCH_LOG)
    CompletableFuture<FetchLogResponse> fetchLog(FetchLogRequest request);

    /**
     * Put kv data to the specified table bucket.
     *
     * @return the produce response.
     */
    // 写入/更新 KV 数据。
    @RPC(api = ApiKeys.PUT_KV)
    CompletableFuture<PutKvResponse> putKv(PutKvRequest request);

    /**
     * Lookup value from the specified table bucket by key.
     *
     * @return the fetch response.
     */
    // 点查询（Point Lookup）。根据特定的 Key 获取对应的 Value。
    @RPC(api = ApiKeys.LOOKUP)
    CompletableFuture<LookupResponse> lookup(LookupRequest request);

    /**
     * Prefix lookup to get value by prefix key.
     *
     * @return Prefix lookup response.
     */
    // 前缀查询。根据 Key 的前缀范围获取匹配的一组数据。
    @RPC(api = ApiKeys.PREFIX_LOOKUP)
    CompletableFuture<PrefixLookupResponse> prefixLookup(PrefixLookupRequest request);

    /**
     * Get limit number of values from the specified table bucket.
     *
     * @param request the limit scan request
     * @return the limit scan response
     */
    // 限制条数的扫描。获取桶中指定数量的数据，常用于快速预览。
    @RPC(api = ApiKeys.LIMIT_SCAN)
    CompletableFuture<LimitScanResponse> limitScan(LimitScanRequest request);

    /**
     * List offsets for the specified table bucket.
     *
     * @return the fetch response.
     */
    // 查询位点。获取指定桶的 Earliest（最早）、Latest（最新）或按时间戳定位的 Offset。
    @RPC(api = ApiKeys.LIST_OFFSETS)
    CompletableFuture<ListOffsetsResponse> listOffsets(ListOffsetsRequest request);

    /**
     * Init writer.
     *
     * @return the init writer response.
     */
    // 初始化写入器。用于处理幂等性写入或事务性写入的初始化工作（分配 Producer ID）。
    @RPC(api = ApiKeys.INIT_WRITER)
    CompletableFuture<InitWriterResponse> initWriter(InitWriterRequest request);

    /**
     * Notify remote log offsets.
     *
     * @return notify remote log offsets response.
     */
    // 通知 TabletServer 有哪些日志已经成功上传到了远程存储（如 S3/OSS），以便进行冷热数据切换。
    @RPC(api = ApiKeys.NOTIFY_REMOTE_LOG_OFFSETS)
    CompletableFuture<NotifyRemoteLogOffsetsResponse> notifyRemoteLogOffsets(
            NotifyRemoteLogOffsetsRequest request);

    /**
     * Notify log offset of a kv snapshot.
     *
     * @return notify snapshot offset response.
     */
    // 通知 KV 快照的完成情况及其对应的 Log Offset。
    @RPC(api = ApiKeys.NOTIFY_KV_SNAPSHOT_OFFSET)
    CompletableFuture<NotifyKvSnapshotOffsetResponse> notifyKvSnapshotOffset(
            NotifyKvSnapshotOffsetRequest request);

    /**
     * Notify log offset of a lakehouse table.
     *
     * @return notify lakehouse data response
     */
    // 湖仓一体核心通知。通知数据湖（Lakehouse）已经同步到了哪个位点，确保湖仓数据读取的一致性。
    @RPC(api = ApiKeys.NOTIFY_LAKE_TABLE_OFFSET)
    CompletableFuture<NotifyLakeTableOffsetResponse> notifyLakeTableOffset(
            NotifyLakeTableOffsetRequest request);
}
