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
import org.apache.helix.zookeeper.datamodel.ZNRecord;
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
    String jobName = RandomStringUtils.random(200, true, true);

    JobConfig.Builder jobBuilder1 = new JobConfig.Builder().setWorkflow(workflowName)
        .setNumberOfTasks(1).setNumConcurrentTasksPerInstance(100).setCommand(MockTask.TASK_COMMAND)
        .setJobCommandConfigMap(ImmutableMap.of(MockTask.JOB_DELAY, "9999999999999"));

    Workflow.Builder workflowBuilder1 =
        new Workflow.Builder(workflowName).addJob(jobName, jobBuilder1);
    _driver.start(workflowBuilder1.build());

    _driver.pollForJobState(workflowName, TaskUtil.getNamespacedJobName(workflowName, jobName),
        TaskState.IN_PROGRESS);

    Thread.sleep(10000L);
    _controller.syncStop();

    String contextPathP0 = _manager.getHelixDataAccessor().keyBuilder().jobContextZNode(workflowName, jobName).toString();

    ZNRecord record = new ZNRecord(jobName);
    record.setSimpleField("EXECUTION_START_TIME", "1612486800285");
    record.setSimpleField("FINISH_TIME", "1612486868549");
    record.setSimpleField("NAME", "jobName");
    record.setSimpleField("START_TIME", "1612486800283");
    for (int i=0 ; i< 1000000; i++) {
      Map<String, String> temp = new HashMap<>();
      temp.put("ASSIGNED_PARTICIPANT", RandomStringUtils.random(40, true, true));
      temp.put("FINISH_TIME", RandomStringUtils.random(13, false, true));
      temp.put("INFO", "");
      temp.put("NUM_ATTEMPTS", "1");
      temp.put( "START_TIME", RandomStringUtils.random(13, false, true));
      temp.put( "STATE", "COMPLETED");
      temp.put("TARGET", RandomStringUtils.random(20, true, true));
      record.setMapField(Integer.toString(i),temp);
      _manager.getHelixDataAccessor().getBaseDataAccessor().set(contextPathP0, record,
          AccessOption.PERSISTENT);

      System.out.println(i + " " + serializer
          .serialize(_driver.getJobContext(TaskUtil.getNamespacedJobName(workflowName, jobName))
              .getRecord()).length);
    }
  }
}
