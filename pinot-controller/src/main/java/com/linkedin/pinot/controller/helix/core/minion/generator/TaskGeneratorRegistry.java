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

import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nonnull;


/**
 * Registry for all {@link PinotTaskGenerator}.
 */
public class TaskGeneratorRegistry {
  private TaskGeneratorRegistry() {
  }

  private static final Map<String, PinotTaskGenerator> TASK_GENERATOR_REGISTRY = new HashMap<>();

  static {
    // TODO: register all task generators here
    DummyTaskGenerator dummyTaskGenerator = new DummyTaskGenerator();
    TASK_GENERATOR_REGISTRY.put(dummyTaskGenerator.getTaskType(), dummyTaskGenerator);
  }

  @Nonnull
  public static Map<String, PinotTaskGenerator> getTaskGeneratorRegistry() {
    return TASK_GENERATOR_REGISTRY;
  }
}
