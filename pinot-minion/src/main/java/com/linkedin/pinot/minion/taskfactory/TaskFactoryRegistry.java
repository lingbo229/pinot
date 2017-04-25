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
package com.linkedin.pinot.minion.taskfactory;

import com.linkedin.pinot.common.config.PinotTaskConfig;
import com.linkedin.pinot.minion.exception.FatalException;
import com.linkedin.pinot.minion.exception.TaskCancelledException;
import com.linkedin.pinot.minion.executor.PinotTaskExecutor;
import com.linkedin.pinot.minion.executor.TaskExecutorRegistry;
import java.util.HashMap;
import java.util.Map;
import org.apache.helix.task.Task;
import org.apache.helix.task.TaskCallbackContext;
import org.apache.helix.task.TaskConfig;
import org.apache.helix.task.TaskFactory;
import org.apache.helix.task.TaskResult;


/**
 * Registry for all {@link TaskFactory}.
 * <p>All {@link PinotTaskExecutor} in {@link TaskExecutorRegistry} will automatically be registered.
 */
public class TaskFactoryRegistry {
  private TaskFactoryRegistry() {
  }

  private final static Map<String, TaskFactory> TASK_FACTORY_REGISTRY = new HashMap<>();

  static {
    Map<String, Class<? extends PinotTaskExecutor>> taskExecutorRegistry =
        TaskExecutorRegistry.getTaskExecutorRegistry();
    for (Map.Entry<String, Class<? extends PinotTaskExecutor>> entry : taskExecutorRegistry.entrySet()) {
      final String taskType = entry.getKey();
      final Class<? extends PinotTaskExecutor> pinotTaskExecutorClass = entry.getValue();
      TaskFactory taskFactory = new TaskFactory() {
        @Override
        public Task createNewTask(final TaskCallbackContext taskCallbackContext) {
          try {
            return new Task() {
              private final TaskConfig _taskConfig = taskCallbackContext.getTaskConfig();
              private final PinotTaskExecutor _pinotTaskExecutor = pinotTaskExecutorClass.newInstance();

              @Override
              public TaskResult run() {
                try {
                  _pinotTaskExecutor.executeTask(PinotTaskConfig.fromHelixTaskConfig(_taskConfig));
                  return new TaskResult(TaskResult.Status.COMPLETED, "Succeeded");
                } catch (TaskCancelledException e) {
                  return new TaskResult(TaskResult.Status.CANCELED, e.toString());
                } catch (FatalException e) {
                  return new TaskResult(TaskResult.Status.FATAL_FAILED, e.toString());
                } catch (Exception e) {
                  return new TaskResult(TaskResult.Status.FAILED, e.toString());
                }
              }

              @Override
              public void cancel() {
                _pinotTaskExecutor.cancel();
              }
            };
          } catch (Exception e) {
            throw new RuntimeException("Caught exception while creating new task", e);
          }
        }
      };
      TASK_FACTORY_REGISTRY.put(taskType, taskFactory);
    }
  }

  public static Map<String, TaskFactory> getTaskFactoryRegistry() {
    return TASK_FACTORY_REGISTRY;
  }
}
