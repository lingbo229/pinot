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
package com.linkedin.pinot.integration.tests;

import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.common.utils.ZkStarter;
import com.linkedin.pinot.controller.helix.core.PinotHelixResourceManager;
import com.linkedin.pinot.controller.helix.core.minion.PinotHelixTaskResourceManager;
import com.linkedin.pinot.controller.helix.core.minion.PinotTaskManager;
import com.linkedin.pinot.controller.helix.core.minion.generator.DummyTaskGenerator;
import com.linkedin.pinot.minion.MinionStarter;
import java.util.Map;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.helix.task.TaskDriver;
import org.apache.helix.task.TaskState;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class MinionClusterIntegrationTest {
  private static final int ZK_PORT = 2191;
  private static final String ZK_ADDRESS = "localhost:" + ZK_PORT;
  private static final String HELIX_CLUSTER_NAME = "pinot-minion-test";
  private static final String CONTROLLER_ID = "controller";
  private static final int NUM_WORKERS = 3;

  private ZkStarter.ZookeeperInstance _zookeeperInstance;
  private PinotHelixResourceManager _pinotHelixResourceManager;
  private PinotHelixTaskResourceManager _pinotHelixTaskResourceManager;
  private PinotTaskManager _pinotTaskManager;
  private MinionStarter[] _minionStarters = new MinionStarter[NUM_WORKERS];

  @BeforeClass
  public void setUp()
      throws Exception {
    // Start a ZooKeeper instance.
    _zookeeperInstance = ZkStarter.startLocalZkServer(ZK_PORT);

    // Start a PinotHelixResourceManager
    _pinotHelixResourceManager = new PinotHelixResourceManager(ZK_ADDRESS, HELIX_CLUSTER_NAME, CONTROLLER_ID, null);
    _pinotHelixResourceManager.start();

    // Initialize a PinotTaskManager
    TaskDriver taskDriver = new TaskDriver(_pinotHelixResourceManager.getHelixZkManager());
    _pinotHelixTaskResourceManager = new PinotHelixTaskResourceManager(taskDriver);
    _pinotTaskManager = new PinotTaskManager(taskDriver, _pinotHelixResourceManager, _pinotHelixTaskResourceManager, 1);

    // Initialize minion starters.
    for (int i = 0; i < NUM_WORKERS; i++) {
      Configuration config = new PropertiesConfiguration();
      config.setProperty(CommonConstants.Helix.Instance.INSTANCE_ID_KEY, "minion" + i);
      _minionStarters[i] = new MinionStarter(ZK_ADDRESS, HELIX_CLUSTER_NAME, config);
    }
  }

  @Test
  public void testStopAndResumeJobQueue()
      throws Exception {
    // Schedule 10 dummy tasks (2 per seconds)
    _pinotTaskManager.start();
    Thread.sleep(5_000L);
    _pinotTaskManager.stop();

    // Stop the job queue
    _pinotHelixTaskResourceManager.stopJobQueue(DummyTaskGenerator.TASK_TYPE);

    // Wait for all 10 dummy tasks shown up in cluster
    Map<String, TaskState> taskStates;
    do {
      Thread.sleep(1_000L);
      taskStates = _pinotHelixTaskResourceManager.getTaskStates(DummyTaskGenerator.TASK_TYPE);
    } while (taskStates.size() != 10);
    for (TaskState taskState : taskStates.values()) {
      Assert.assertTrue(taskState.equals(TaskState.COMPLETED) || taskState.equals(TaskState.STOPPED));
    }

    // Resume the job queue
    _pinotHelixTaskResourceManager.resumeJobQueue(DummyTaskGenerator.TASK_TYPE);

    // Wait for all tasks completed
    Thread.sleep(5_000L);
    taskStates = _pinotHelixTaskResourceManager.getTaskStates(DummyTaskGenerator.TASK_TYPE);
    for (TaskState taskState : taskStates.values()) {
      Assert.assertEquals(taskState, TaskState.COMPLETED);
    }

    // Delete the job queue
    _pinotHelixTaskResourceManager.deleteJobQueue(DummyTaskGenerator.TASK_TYPE);
  }

  @AfterClass
  public void tearDown()
      throws Exception {
    for (MinionStarter minionStarter : _minionStarters) {
      minionStarter.stop();
    }
    _pinotHelixResourceManager.stop();
    ZkStarter.stopLocalZkServer(_zookeeperInstance);
  }
}
