/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.bookkeeper.common.component;

import java.lang.Thread.UncaughtExceptionHandler;

/**
 * 基于生命周期管理的组件。
 * A component based on lifecycle management.
 */
public interface LifecycleComponent extends AutoCloseable {
    // 返回组件名字
    String getName();
    // 获取组件生命周期状态
    Lifecycle.State lifecycleState();
    // 添加到生命周期中的listener
    void addLifecycleListener(LifecycleListener listener);

    // 移除listener
    void removeLifecycleListener(LifecycleListener listener);

    // 发布信息
    default void publishInfo(ComponentInfoPublisher componentInfoPublisher) {
    }
    // 启动
    void start();
    // 停止
    void stop();

    // 关闭
    @Override
    void close();

    /**
     * 设置生命周期组件由于未捕获的异常而突然终止时调用的默认处理程序。
     * Set the default handler invoked when a lifecycle component
     * abruptly terminates due an uncaught exception.
     *
     * @param handler handler invoked when an uncaught exception happens
     *                in the lifecycle component.
     */
    void setExceptionHandler(UncaughtExceptionHandler handler);
}
