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

import com.google.common.util.concurrent.Uninterruptibles;
import com.linkedin.pinot.common.config.PinotTaskConfig;
import com.linkedin.pinot.minion.exception.TaskCancelledException;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The class <code>DummyTaskExecutor</code> is an example of {@link PinotTaskExecutor}.
 */
public class DummyTaskExecutor extends BaseTaskExecutor {
  private static final Logger LOGGER = LoggerFactory.getLogger(DummyTaskExecutor.class);

  public static final String TASK_TYPE = "DummyTask";

  @Override
  public void executeTask(@Nonnull PinotTaskConfig pinotTaskConfig) {
    LOGGER.info("Executing task with config: " + pinotTaskConfig.toString());
    Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);

    if (_cancelled) {
      throw new TaskCancelledException("Task has been cancelled");
    }
  }
}
