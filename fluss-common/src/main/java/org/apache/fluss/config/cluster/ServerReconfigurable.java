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

package org.apache.fluss.config.cluster;

import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.ConfigException;

/** Server Reconfigurable Interface which can dynamically respond to configuration changes. */
// 定义了服务器组件如何实现动态配置更新（Dynamic Reconfiguration）
// 在分布式存储系统（如 Fluss 或 Kafka）中，某些配置（例如日志清理策略、线程池大小、限流阈值等）如果需要修改，通常有两种方式：一种是重启服务，另一种是动态生效。
// ServerReconfigurable 接口的作用就是让组件具备**“热更新”**的能力：
// 无需停机：允许在不重启 Tablet Server 或 Coordinator 的情况下，实时调整内部参数。
// 两阶段提交保证安全性：通过“先校验（Validate）、后应用（Reconfigure）”的模式，确保错误的配置不会破坏正在运行的系统。
public interface ServerReconfigurable {

    /**
     * Validates the provided configuration. The provided map contains all configs including any
     * reconfigurable configs that may be different from the initial configuration. Reconfiguration
     * will be not performed if this method throws any exception.
     *
     * <p>This method should check that the new configuration values are valid and will not cause
     * issues when applied. It should throw a ConfigException with a descriptive message if any
     * validation fails.
     *
     * <p>This validation step is crucial as it prevents applying invalid configurations that could
     * potentially cause the component to malfunction or behave unexpectedly.
     *
     * @param newConfig the new configuration that would be applied if validation succeeds. This
     *     contains all configuration properties, not just the ones that changed or are
     *     reconfigurable.
     * @throws ConfigException if the configuration is invalid or cannot be applied to this
     *     component
     */
    // 预校验阶段。在正式应用新配置之前，对配置的合法性、兼容性和逻辑正确性进行检查。
    // newConfig 包含了当前完整的配置集合
    void validate(Configuration newConfig) throws ConfigException;

    /**
     * Reconfigures the component with the provided configuration.
     *
     * <p>This method is called after validation succeeds and should apply the new configuration to
     * the component. The implementation should update internal state and make necessary adjustments
     * to adapt to the new configuration values.
     *
     * <p>This method should be designed to be thread-safe as it may be called concurrently with
     * other operations on the component. It should also be prepared to handle being called multiple
     * times with different configurations.
     *
     * @param newConfig the validated configuration to apply to this component
     * @throws ConfigException if there is an error applying the configuration, though this should
     *     generally be avoided as the validation step should catch most issues
     */
    // 正式生效阶段。
    // 将通过校验的新配置实际应用到组件的运行逻辑中。
    void reconfigure(Configuration newConfig) throws ConfigException;
}
