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
package com.linkedin.pinot.controller.helix.core.minion;

import com.linkedin.pinot.common.config.AbstractTableConfig;
import com.linkedin.pinot.common.config.PinotTaskConfig;
import com.linkedin.pinot.controller.helix.core.PinotHelixResourceManager;
import com.linkedin.pinot.controller.helix.core.minion.generator.PinotTaskGenerator;
import com.linkedin.pinot.controller.helix.core.minion.generator.TaskGeneratorRegistry;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;
import org.apache.helix.task.TaskDriver;
import org.apache.helix.task.WorkflowConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The class <code>PinotTaskManager</code> is the component inside Pinot Controller to periodically check the Pinot
 * cluster status and schedule new tasks.
 * <p><code>PinotTaskManager</code> is also responsible for checking the health status on each type of tasks, detect and
 * fix issues accordingly.
 */
public class PinotTaskManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(PinotTaskManager.class);

  private final TaskDriver _taskDriver;
  private final PinotHelixResourceManager _pinotHelixResourceManager;
  private final PinotHelixTaskResourceManager _pinotHelixTaskResourceManager;
  private final ClusterInfoProvider _clusterInfoProvider;
  private final int _runFrequencyInSeconds;

  private ScheduledExecutorService _executorService;

  public PinotTaskManager(@Nonnull TaskDriver taskDriver, @Nonnull PinotHelixResourceManager pinotHelixResourceManager,
      @Nonnull PinotHelixTaskResourceManager pinotHelixTaskResourceManager, int runFrequencyInSeconds) {
    _taskDriver = taskDriver;
    _pinotHelixResourceManager = pinotHelixResourceManager;
    _pinotHelixTaskResourceManager = pinotHelixTaskResourceManager;
    _clusterInfoProvider = new ClusterInfoProvider(pinotHelixResourceManager, pinotHelixTaskResourceManager);
    _runFrequencyInSeconds = runFrequencyInSeconds;
  }

  /**
   * Start the <code>PinotTaskManager</code>.
   */
  public void start() {
    // Ensure job queue exists for all registered jobs
    Map<String, WorkflowConfig> workflows = _taskDriver.getWorkflows();
    for (String taskType : TaskGeneratorRegistry.getTaskGeneratorRegistry().keySet()) {
      if (!workflows.containsKey(taskType)) {
        _pinotHelixTaskResourceManager.createJobQueue(taskType);
      }
    }

    // Schedule the execute method
    _executorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
      @Override
      public Thread newThread(@Nonnull Runnable r) {
        Thread thread = new Thread(r);
        thread.setName("PinotTaskManagerExecutorService");
        return thread;
      }
    });
    _executorService.scheduleWithFixedDelay(new Runnable() {
      @Override
      public void run() {
        execute();
      }
    }, 0, _runFrequencyInSeconds, TimeUnit.SECONDS);
  }

  /**
   * Stop the <code>PinotTaskManager</code>.
   */
  public void stop() {
    _executorService.shutdown();
  }

  /**
   * Check the Pinot cluster status and schedule new tasks.
   */
  private void execute() {
    // Only schedule new tasks from leader controller
    if (!_pinotHelixResourceManager.isLeader()) {
      LOGGER.info("Skip scheduling new tasks on non-leader controller");
      return;
    }

    // TODO: add JobQueue health check here

    Map<String, List<AbstractTableConfig>> enabledTableConfigMap = new HashMap<>();
    for (String taskType : TaskGeneratorRegistry.getTaskGeneratorRegistry().keySet()) {
      enabledTableConfigMap.put(taskType, new ArrayList<AbstractTableConfig>());
    }

    // Scan all table configs to get the tables with tasks enabled
    for (String tableName : _pinotHelixResourceManager.getAllPinotTableNames()) {
      AbstractTableConfig tableConfig = _pinotHelixResourceManager.getTableConfig(tableName);
      if (tableConfig != null) {
        // TODO: add table configs that have certain types of tasks enabled into the map
      }
    }

    // Generate each type of tasks
    for (Map.Entry<String, PinotTaskGenerator> entry : TaskGeneratorRegistry.getTaskGeneratorRegistry().entrySet()) {
      String taskType = entry.getKey();
      LOGGER.info("Generating tasks for task type: {}", taskType);
      PinotTaskGenerator pinotTaskGenerator = entry.getValue();
      List<PinotTaskConfig> pinotTaskConfigs = pinotTaskGenerator.generateTasks(enabledTableConfigMap.get(taskType));
      for (PinotTaskConfig pinotTaskConfig : pinotTaskConfigs) {
        _pinotHelixTaskResourceManager.submitTask(taskType, pinotTaskConfig);
      }
    }
  }
}
