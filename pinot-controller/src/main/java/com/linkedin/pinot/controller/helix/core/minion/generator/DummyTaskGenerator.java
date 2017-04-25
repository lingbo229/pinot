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
package com.linkedin.pinot.controller.helix.core.minion.generator;

import com.linkedin.pinot.common.config.AbstractTableConfig;
import com.linkedin.pinot.common.config.PinotTaskConfig;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;


/**
 * The class <code>DummyTaskGenerator</code> is an example of {@link PinotTaskGenerator}.
 */
public class DummyTaskGenerator implements PinotTaskGenerator {
  public static final String TASK_TYPE = "DummyTask";

  @Nonnull
  @Override
  public String getTaskType() {
    return TASK_TYPE;
  }

  @Nonnull
  @Override
  public List<PinotTaskConfig> generateTasks(@Nonnull List<AbstractTableConfig> tableConfigs) {
    Map<String, String> config1 = new HashMap<>();
    config1.put("arg1", "foo1");
    config1.put("arg2", "bar1");
    Map<String, String> config2 = new HashMap<>();
    config2.put("arg1", "foo2");
    config2.put("arg2", "bar2");
    return Arrays.asList(new PinotTaskConfig(TASK_TYPE, config1), new PinotTaskConfig(TASK_TYPE, config2));
  }
}
