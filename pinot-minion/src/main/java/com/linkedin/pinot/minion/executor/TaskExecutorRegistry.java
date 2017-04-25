/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.minion.executor;

import java.util.HashMap;
import java.util.Map;


/**
 * Registry for all {@link PinotTaskExecutor}.
 */
public class TaskExecutorRegistry {
  private TaskExecutorRegistry() {
  }

  private static final Map<String, Class<? extends PinotTaskExecutor>> TASK_EXECUTOR_REGISTRY = new HashMap<>();

  static {
    // TODO: register all task executors here, key should match the task type in task generator
    TASK_EXECUTOR_REGISTRY.put(DummyTaskExecutor.TASK_TYPE, DummyTaskExecutor.class);
  }

  public static Map<String, Class<? extends PinotTaskExecutor>> getTaskExecutorRegistry() {
    return TASK_EXECUTOR_REGISTRY;
  }
}
