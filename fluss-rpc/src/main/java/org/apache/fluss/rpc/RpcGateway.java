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

package org.apache.fluss.rpc;

import org.apache.fluss.rpc.messages.ApiVersionsRequest;
import org.apache.fluss.rpc.messages.ApiVersionsResponse;
import org.apache.fluss.rpc.messages.AuthenticateRequest;
import org.apache.fluss.rpc.messages.AuthenticateResponse;
import org.apache.fluss.rpc.protocol.ApiKeys;
import org.apache.fluss.rpc.protocol.RPC;
import org.apache.fluss.shaded.netty4.io.netty.channel.ChannelHandlerContext;

import java.util.concurrent.CompletableFuture;

/** Rpc gateway interface which has to be implemented by Rpc gateways. */
// RpcGateway 的主要作用是定义远程服务的访问入口点。
// 在分布式系统里，Client（如 Flink 任务）需要向 Server（如 TabletServer 或 CoordinatorServer）发送请求。RpcGateway 及其子接口扮演了以下角色：
// 统一契约：定义了服务端能够理解的所有 API 及其版本。
// 解耦通信实现：通过这个接口，上层代码只需要关注“发送什么请求”和“获得什么响应”，而不必关心底层的 Netty 网络细节或序列化过程。
// 代码生成的依据：Fluss 的 RPC 框架会扫描这些接口中的 @RPC 注解，动态生成客户端代理类（Stub）和服务器端的请求分发逻辑。
public interface RpcGateway {

    /** Returns the APIs and Versions of the RPC Gateway supported. */
    // 当客户端第一次连接到服务器时，会先调用此方法。
    // 服务器会返回它所支持的所有 API 键（ApiKeys）及其对应的最小和最大版本号。
    @RPC(api = ApiKeys.API_VERSIONS)
    CompletableFuture<ApiVersionsResponse> apiVersions(ApiVersionsRequest request);

    /**
     * This method just to registers the AUTHENTICATE API in the API manager for client-side
     * request/response object generation.
     *
     * <p>This method does not handle the authentication logic itself. Instead, the {@link
     * AuthenticateRequest} is processed preemptively by {@link
     * org.apache.fluss.rpc.netty.server.NettyServerHandler} during the initial connection
     * handshake. The client uses this method definition to generate corresponding request/response
     * objects for API version compatibility.
     *
     * @param request The authenticate request (not used in this method's implementation).
     * @return Always returns {@code null} since the actual authentication handling occurs in {@link
     *     org.apache.fluss.rpc.netty.server.NettyServerHandler}.
     * @see org.apache.fluss.rpc.netty.server.NettyServerHandler#channelRead(ChannelHandlerContext,
     *     Object) For authentication processing implementation.
     */
    // 注册身份验证 API 规范。
    @RPC(api = ApiKeys.AUTHENTICATE)
    default CompletableFuture<AuthenticateResponse> authenticate(AuthenticateRequest request) {
        throw new UnsupportedOperationException("This method should not be called directly.");
    }
}
