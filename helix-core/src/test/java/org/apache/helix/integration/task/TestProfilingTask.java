package org.apache.helix.integration.task;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.helix.AccessOption;
import org.apache.helix.HelixException;
import org.apache.helix.TestHelper;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.manager.zk.ZNRecordStreamingSerializer;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.MasterSlaveSMD;
import org.apache.helix.task.JobConfig;
import org.apache.helix.task.JobContext;
import org.apache.helix.task.TaskConfig;
import org.apache.helix.task.TaskPartitionState;
import org.apache.helix.task.TaskState;
import org.apache.helix.task.TaskUtil;
import org.apache.helix.task.Workflow;
import org.apache.helix.task.WorkflowConfig;
import org.apache.helix.task.WorkflowContext;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;

public class TestProfilingTask extends TaskTestBase {
  private static final String DATABASE = "TestDB_" + TestHelper.getTestClassName();
  private static final String DEFAULT_QUOTA_TYPE = "DEFAULT";

  @BeforeClass
  public void beforeClass() throws Exception {
    _numNodes = 3;
    super.beforeClass();
    _controller.syncStop();
  }

  @AfterClass
  public void afterClass() throws Exception {
    super.afterClass();
  }

  @Test
  public void TestProfilingTaskAddTaskOneByOne() throws Exception {
    ZNRecordStreamingSerializer serializer = new ZNRecordStreamingSerializer();
    String generatedString = RandomStringUtils.random(60, true, true);

    String workflowName = TestHelper.getTestMethodName();
    String jobName = generatedString;

    JobConfig.Builder jobBuilder1 = new JobConfig.Builder().setWorkflow(workflowName)
        .setNumberOfTasks(1).setNumConcurrentTasksPerInstance(100).setCommand(MockTask.TASK_COMMAND)
        .setDisableExternalView(false).setExpiry(9223372036854774L).setFailureThreshold(322).setMaxAttemptsPerTask(2147483647).setMaxAttemptsPerTask(2147483647).setRebalanceRunningTask(false).setTimeoutPerTask(9223372036854774000L)
        .setIgnoreDependentJobFailure(false)
        .setJobCommandConfigMap(ImmutableMap.of(MockTask.JOB_DELAY, "99999999"));

    Workflow.Builder workflowBuilder1 =
        new Workflow.Builder(workflowName).addJob(jobName, jobBuilder1);
    _driver.start(workflowBuilder1.build());

    // Add short running task
    for (int i = 1; i < 10000; i++) {
      Map<String, String> newTaskConfig =
          new HashMap<String, String>(ImmutableMap.of("TASK_SUCCESS_OPTIONAL", "true", RandomStringUtils.random(38, true, true),
              RandomStringUtils.random(273, true, true), "job.name", generatedString, "task.id",
              generatedString + "_" + RandomStringUtils.random(3, false, true)));
      TaskConfig task = new TaskConfig(null, newTaskConfig, null, null);
      _driver.addTask(workflowName, jobName, task);
      System.out.println(i + " " + serializer.serialize(_driver.getJobConfig(TaskUtil.getNamespacedJobName(workflowName,jobName)).getRecord()).length);
    }
    Thread.sleep(500000000000L);
  }
}
