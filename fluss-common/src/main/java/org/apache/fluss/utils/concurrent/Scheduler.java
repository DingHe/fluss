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

package org.apache.fluss.utils.concurrent;

import org.apache.fluss.annotation.Internal;

import java.util.concurrent.ScheduledFuture;

/**
 * A scheduler for running jobs.
 *
 * <p>This interface controls a job scheduler that allows scheduling either repeating background
 * jobs that execute periodically, or one-time jobs that execute once.
 */
// Scheduler 接口的主要作用是管理和执行后台异步任务。
// 在分布式系统如 Fluss 中，有很多工作不需要立即同步完成，或者需要周期性地运行。Scheduler 提供了一个统一的机制来处理这些需求：
// 周期性任务：例如 ISR 副本同步状态检查、清理过期日志、定时持久化 Checkpoint 等。
// 一次性延迟任务：例如在某个延迟时间后触发特定的恢复逻辑或超时处理。
// 资源抽象：它隐藏了底层线程池（通常是 ScheduledThreadPoolExecutor）的复杂性，让开发者只需关注“任务是什么”和“什么时候跑”。
@Internal
public interface Scheduler {

    /** Initialize this scheduler, so it is ready to accept scheduling of tasks. */
    // 在调度器接收任何任务之前必须调用此方法。它通常用于启动底层的执行线程池，分配必要的系统资源，确保调度环境就绪。
    void startup();

    /**
     * Shutdown this scheduler. When this method is complete no more executions of background tasks
     * will occur. This includes tasks scheduled with a delayed execution.
     */
    // 关闭调度器。
    void shutdown() throws InterruptedException;
    // 立即执行一次性任务。
    default ScheduledFuture<?> scheduleOnce(String name, Runnable task) {
        return scheduleOnce(name, task, 0L);
    }
    // 在指定的延迟后执行一次性任务。
    default ScheduledFuture<?> scheduleOnce(String name, Runnable task, long delayMs) {
        return schedule(name, task, delayMs, -1);
    }

    /**
     * Schedule a task.
     *
     * @param name The name of this task
     * @param task The task to run
     * @param delayMs The number of milliseconds to wait before the first execution
     * @param periodMs The period in milliseconds with which to execute the task. If &lt; 0 the task
     *     will execute only once.
     * @return A Future object to manage the task scheduled.
     */
    // 支持一次性或周期性任务。
    ScheduledFuture<?> schedule(String name, Runnable task, long delayMs, long periodMs);
}
