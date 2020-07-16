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

import org.apache.helix.HelixDataAccessor;
import org.apache.helix.PropertyKey;
import org.apache.helix.TestHelper;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.participant.StateMachineEngine;
import org.apache.helix.task.JobConfig;
import org.apache.helix.task.JobContext;
import org.apache.helix.task.JobQueue;
import org.apache.helix.task.TargetState;
import org.apache.helix.task.TaskCallbackContext;
import org.apache.helix.task.TaskFactory;
import org.apache.helix.task.TaskPartitionState;
import org.apache.helix.task.TaskState;
import org.apache.helix.task.TaskStateModelFactory;
import org.apache.helix.task.TaskUtil;
import org.apache.helix.task.Workflow;
import org.apache.helix.task.WorkflowConfig;
import org.apache.helix.task.WorkflowContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;


public class TestStuckTaskQuota extends TaskTestBase {

  @BeforeClass
  public void beforeClass() throws Exception {
    _numNodes = 2;
    super.beforeClass();

    // Stop participants that have been started in super class
    for (int i = 0; i < _numNodes; i++) {
      super.stopParticipant(i);
      Assert.assertFalse(_participants[i].isConnected());
    }
    _participants = new MockParticipantManager[_numNodes];

    // Start first participant
    startParticipantAndRegisterNewMockTask(0);
  }

  @Test
  public void testNew() throws Exception {
    String workflowName1 = TestHelper.getTestMethodName()+"_1";
    String workflowName2 = TestHelper.getTestMethodName()+"_2";
    String workflowName3 = TestHelper.getTestMethodName()+"_3";
    JobConfig.Builder jobBuilder1 = new JobConfig.Builder().setWorkflow(workflowName1)
        .setNumberOfTasks(40).setNumConcurrentTasksPerInstance(100).setCommand(MockTask.TASK_COMMAND)
        .setJobCommandConfigMap(ImmutableMap.of(MockTask.JOB_DELAY, "99999999"));

    JobConfig.Builder jobBuilder2 = new JobConfig.Builder().setWorkflow(workflowName2)
        .setNumberOfTasks(1).setCommand(MockTask.TASK_COMMAND)
        .setJobCommandConfigMap(ImmutableMap.of(MockTask.JOB_DELAY, "99999999"));

    JobConfig.Builder jobBuilder3 = new JobConfig.Builder().setWorkflow(workflowName3)
        .setNumberOfTasks(1).setCommand(MockTask.TASK_COMMAND)
        .setJobCommandConfigMap(ImmutableMap.of(MockTask.JOB_DELAY, "99999999"));

    Workflow.Builder workflowBuilder1 = new Workflow.Builder(workflowName1).addJob("JOB0", jobBuilder1);
    Workflow.Builder workflowBuilder2 = new Workflow.Builder(workflowName2).addJob("JOB0", jobBuilder2);
    Workflow.Builder workflowBuilder3 = new Workflow.Builder(workflowName3).addJob("JOB0", jobBuilder3);
    _driver.start(workflowBuilder1.build());
    Thread.sleep(2000L);

    // Start the second participant
    startParticipantAndRegisterNewMockTask(1);
    Thread.sleep(2000L);
    _driver.start(workflowBuilder2.build());
    Thread.sleep(2000L);
    _driver.delete(workflowName1);
    Thread.sleep(2000L);
    _driver.start(workflowBuilder3.build());
    _driver.pollForJobState(workflowName3, TaskUtil.getNamespacedJobName(workflowName3, "JOB0"),
        TaskState.IN_PROGRESS);

    // Although there are quota available on both of the instances, controller does not schedule the task
    Assert.assertTrue(TestHelper.verify(() -> (TaskPartitionState.RUNNING.equals(_driver
        .getJobContext(TaskUtil.getNamespacedJobName(workflowName3, "JOB0")).getPartitionState(0))),
        TestHelper.WAIT_DURATION));
  }

  private void startParticipantAndRegisterNewMockTask(int participantIndex) {
      Map<String, TaskFactory> taskFactoryReg = new HashMap<>();
      taskFactoryReg.put(NewMockTask.TASK_COMMAND, NewMockTask::new);
      String instanceName = PARTICIPANT_PREFIX + "_" + (_startPort + participantIndex);
      _participants[participantIndex] = new MockParticipantManager(ZK_ADDR, CLUSTER_NAME, instanceName);

      // Register a Task state model factory.
      StateMachineEngine stateMachine = _participants[participantIndex].getStateMachineEngine();
      stateMachine.registerStateModelFactory("Task",
          new TaskStateModelFactory(_participants[participantIndex], taskFactoryReg));
      _participants[participantIndex].syncStart();
  }

  /**
   * A mock task that extents MockTask class to count the number of cancel messages.
   */
  private class NewMockTask extends MockTask {

    NewMockTask(TaskCallbackContext context) {
      super(context);
    }

    @Override
    public void cancel() {
      // Increment the cancel count so we know cancel() has been called
      try {
        Thread.sleep(10000000L);
      } catch (Exception e) {
        // OK
      }
      super.cancel();
    }
  }
}

