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

package org.apache.fluss.server;

import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.FlussException;
import org.apache.fluss.fs.FileSystem;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.plugin.PluginManager;
import org.apache.fluss.plugin.PluginUtils;
import org.apache.fluss.server.authorizer.Authorizer;
import org.apache.fluss.server.coordinator.CoordinatorServer;
import org.apache.fluss.server.exception.FlussParseException;
import org.apache.fluss.server.tablet.TabletServer;
import org.apache.fluss.server.utils.ConfigurationParserUtils;
import org.apache.fluss.server.utils.FatalErrorHandler;
import org.apache.fluss.server.utils.ShutdownHookUtil;
import org.apache.fluss.server.utils.SignalHandler;
import org.apache.fluss.utils.AutoCloseableAsync;
import org.apache.fluss.utils.ExceptionUtils;
import org.apache.fluss.utils.concurrent.FutureUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.lang.reflect.UndeclaredThrowableException;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.apache.fluss.server.utils.LogShutdownUtil.shutdownLogIfPossible;

/** An abstract base server class for {@link CoordinatorServer} & {@link TabletServer}. */
// ServerBase 是 Apache Fluss 服务端架构中的抽象基类，它为 Fluss 的两大核心组件：CoordinatorServer（协调节点）和 TabletServer（数据节点）提供了通用的生命周期管理、配置加载、故障处理和资源初始化逻辑。
// 资源抽象：为子类统一初始化了远程文件系统（Remote FileSystem），用于存取数据湖或远程快照。

public abstract class ServerBase implements AutoCloseableAsync, FatalErrorHandler {

    private static final Logger LOG = LoggerFactory.getLogger(ServerBase.class);
    // 进程正常退出。
    private static final int SUCCESS_EXIT_CODE = 0;
    // 进程通用失败退出。
    protected static final int FAILURE_EXIT_CODE = 1;
    // 启动阶段发生异常退出
    private static final int STARTUP_FAILURE_RETURN_CODE = 2;
    // 运行阶段发生未捕获异常退出。
    private static final int RUNTIME_FAILURE_RETURN_CODE = 3;
    // 发生致命错误时，等待系统关闭的硬超时时间（10秒）。
    private static final long FATAL_ERROR_SHUTDOWN_TIMEOUT_MS = 10000L;
    // 启动失败执行清理操作的超时时间。
    private static final Duration INITIALIZATION_SHUTDOWN_TIMEOUT = Duration.ofSeconds(30L);

    protected static final long ZOOKEEPER_REGISTER_TOTAL_WAIT_TIME_MS = 60 * 1000L;
    protected static final long ZOOKEEPER_REGISTER_RETRY_INTERVAL_MS = 3 * 1000L;
    // 存储服务的全部配置信息
    protected final Configuration conf;
    // 指向远程存储（如 S3, OSS, HDFS）的句柄，处理分层存储逻辑。
    protected FileSystem remoteFileSystem;
    // 管理 Fluss 的插件（如不同的文件系统实现、安全认证插件等）
    protected PluginManager pluginManager;

    protected ServerBase(Configuration conf) {
        this.conf = conf;
    }
    // JVM 关闭挂钩线程，确保在用户按下 Ctrl+C 或系统终止信号时执行清理。
    private Thread shutDownHook;
    // 调用工具类解析命令行参数并加载配置文件。如果解析失败，直接终止进程。
    protected static Configuration loadConfiguration(String[] args, String serverClassName) {
        try {
            return ConfigurationParserUtils.loadCommonConfiguration(args, serverClassName);
        } catch (FlussParseException fpe) {
            LOG.error("Could not load the configuration.", fpe);
            System.exit(FAILURE_EXIT_CODE);
            return null;
        }
    }
    // 调用 server.start()，并阻塞等待 getTerminationFuture()
    protected static void startServer(ServerBase server) {
        String serverName = server.getServerName();
        LOG.info("Starting {}.", server.getServerName());
        try {
            server.start();
        } catch (Exception e) {
            LOG.error("Could not start {}.", serverName, e);
            System.exit(STARTUP_FAILURE_RETURN_CODE);
        }
        int returnCode;
        Throwable throwable = null;

        try {
            // 当 Future 完成时，获取 Result 中的退出码，并执行 System.exit()。这是保持 Java 进程存活的关键。
            returnCode = server.getTerminationFuture().get().getExitCode();
        } catch (Throwable e) {
            throwable = ExceptionUtils.stripExecutionException(e);
            returnCode = RUNTIME_FAILURE_RETURN_CODE;
        }

        LOG.info("Terminating {} process with exit code {}.", serverName, returnCode, throwable);
        System.exit(returnCode);
    }
    // Fluss 服务端（无论是 Coordinator 还是 Tablet Server）启动的核心工作流。它负责从系统底层（信号处理、插件、文件系统）到上层业务服务的初始化
    public void start() throws Exception {
        // 1. 注册信号处理器。
        // 作用：拦截操作系统的信号（如 SIGTERM, SIGINT）。
        // 这样当用户按下 Ctrl+C 或执行 kill 命令时，服务器能记录日志并平滑退出，而不是瞬间崩掉。
        SignalHandler.register(LOG);
        try {
            // 2. 添加 JVM 关闭钩子（Shutdown Hook）。
            // 作用：当 JVM 因为各种原因退出时，确保能触发 closeAsync() 释放资源、刷写日志。
            addShutDownHook();

            // at first, we need to initialize the file system
            // 3. 创建插件管理器。
            // 作用：从配置指定的根目录加载外部插件（比如特定的文件系统实现或安全验证组件）。
            pluginManager = PluginUtils.createPluginManagerFromRootFolder(conf);
            // 4. 初始化文件系统抽象层。
            // 作用：全局初始化 FileSystem 类。Fluss 支持多种存储（本地、S3、HDFS），
            // 这一步是让系统知道如何根据 URL 协议头加载对应的驱动。
            FileSystem.initialize(conf, pluginManager);

            // get uri for remote data dir
            // 5. 从配置中获取远程数据目录的路径（REMOTE_DATA_DIR）。
            // 它是“湖仓一体”架构中“湖”存储的关键路径。
            String remoteDir = conf.get(ConfigOptions.REMOTE_DATA_DIR);
            remoteFileSystem = new FsPath(remoteDir).getFileSystem();
            // 7. 启动子类特定的业务服务。
            // 作用：这是一个抽象方法。
            // 如果是 TabletServer，会启动分片存储、RPC 服务等；
            // 如果是 CoordinatorServer，会启动元数据管理器、集群协调器等。
            startServices();
        } catch (Throwable t) {
            final Throwable strippedThrowable =
                    ExceptionUtils.stripException(t, UndeclaredThrowableException.class);
            try {
                // clean up any partial state
                closeAsync(Result.FAILURE)
                        .get(INITIALIZATION_SHUTDOWN_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            } catch (InterruptedException | ExecutionException | TimeoutException e) {
                strippedThrowable.addSuppressed(e);
            }
            LOG.error("Could not start the {}.", getServerName(), strippedThrowable);
            throw new FlussException(
                    String.format("Failed to start the %s.", getServerName()), strippedThrowable);
        }
    }

    private void addShutDownHook() {
        shutDownHook =
                ShutdownHookUtil.addShutdownHook(
                        () -> {
                            this.closeAsync(Result.JVM_SHUTDOWN).join();
                            shutdownLogIfPossible();
                        },
                        getServerName(),
                        LOG);
    }

    @Override
    public void onFatalError(Throwable exception) {
        // todo, enrich coordinator server error like Flink
        LOG.error(
                "Fatal error occurred while running the {}. Shutting it down...",
                getServerName(),
                exception);
        if (ExceptionUtils.isJvmFatalError(exception)) {
            System.exit(-1);
        } else {
            closeAsync(Result.FAILURE);
            FutureUtils.orTimeout(
                    getTerminationFuture(),
                    FATAL_ERROR_SHUTDOWN_TIMEOUT_MS,
                    TimeUnit.MILLISECONDS,
                    String.format(
                            "Waiting for %s shutting down timed out after %s ms.",
                            getServerName(), FATAL_ERROR_SHUTDOWN_TIMEOUT_MS));
        }
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        ShutdownHookUtil.removeShutdownHook(shutDownHook, getServerName(), LOG);
        return closeAsync(Result.SUCCESS).thenAccept(ignored -> {});
    }

    protected abstract void startServices() throws Exception;

    protected abstract CompletableFuture<Result> closeAsync(Result result);

    protected abstract CompletableFuture<Result> getTerminationFuture();

    protected abstract String getServerName();

    @VisibleForTesting
    public abstract @Nullable Authorizer getAuthorizer();

    /** Result for run {@link ServerBase}. */
    public enum Result {
        SUCCESS(SUCCESS_EXIT_CODE),
        JVM_SHUTDOWN(FAILURE_EXIT_CODE),
        FAILURE(FAILURE_EXIT_CODE);

        private final int exitCode;

        Result(int exitCode) {
            this.exitCode = exitCode;
        }

        public int getExitCode() {
            return exitCode;
        }
    }
}
