package org.apache.helix.task;

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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.helix.common.caches.TaskDataCache;
import org.apache.helix.controller.dataproviders.WorkflowControllerDataProvider;
import org.apache.helix.controller.stages.BestPossibleStateOutput;
import org.apache.helix.controller.stages.CurrentStateOutput;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.Message;
import org.apache.helix.model.Partition;
import org.apache.helix.model.ResourceAssignment;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestNumberOfAttempts {
  private static final String CLUSTER_NAME = "TestCluster";
  private static final String INSTANCE_PREFIX = "Instance_";
  private static final int NUM_PARTICIPANTS = 3;
  private static final String WORKFLOW_NAME = "TestWorkflow";
  private static final String JOB_NAME = "TestJob";
  private static final String PARTITION_NAME = "0";
  private static final String TARGET_RESOURCES = "TestDB";
  private Map<String, LiveInstance> _liveInstances;
  private Map<String, InstanceConfig> _instanceConfigs;
  private ClusterConfig _clusterConfig;
  private AssignableInstanceManager _assignableInstanceManager;

  @BeforeClass
  public void beforeClass() {
    // Populate live instances and their corresponding instance configs
    _liveInstances = new HashMap<>();
    _instanceConfigs = new HashMap<>();
    _clusterConfig = new ClusterConfig(CLUSTER_NAME);
    for (int i = 0; i < NUM_PARTICIPANTS; i++) {
      String instanceName = INSTANCE_PREFIX + i;
      LiveInstance liveInstance = new LiveInstance(instanceName);
      InstanceConfig instanceConfig = new InstanceConfig(instanceName);
      _liveInstances.put(instanceName, liveInstance);
      _instanceConfigs.put(instanceName, instanceConfig);
    }
    _assignableInstanceManager = new AssignableInstanceManager();
  }

  /**
   * Scenario:   JobState: In-Progress
   * Task Pid   | CurrState | PreviousAssignment  | Context   | Pending Messages
   * 0          | TimedOut  | Running             | Init      | TimedOut -> Init
   * 1          | Completed | Completed           | Completed | Null
   * 2          | Running   | Running             | Running   | Null
   *
   * Result:    JobState: Failing
   * Task Pid   | CurrState | PreviousAssignment  | Context   | Messages
   * 0          | TimedOut  | Running             | TimedOut  |
   * 1          | Completed | Aborted             | Completed | Completed -> Init
   * 2          | Running   | Aborted             | Running   | Running -> Aborted
   */
  @Test
  public void testNumberOfAttemptsPipeline1() {
    MockTestInformation mock = new MockTestInformation();
    when(mock._cache.getWorkflowConfig(WORKFLOW_NAME)).thenReturn(mock._workflowConfig);
    when(mock._cache.getJobConfig(JOB_NAME)).thenReturn(mock._jobConfig);
    when(mock._cache.getTaskDataCache()).thenReturn(mock._taskDataCache);
    when(mock._cache.getJobContext(JOB_NAME)).thenReturn(mock._jobContext);
    when(mock._cache.getIdealStates()).thenReturn(mock._idealStates);
    when(mock._cache.getEnabledLiveInstances()).thenReturn(_liveInstances.keySet());
    when(mock._cache.getInstanceConfigMap()).thenReturn(_instanceConfigs);
    when(mock._cache.getTaskDataCache().getPreviousAssignment(JOB_NAME))
        .thenReturn(mock._resourceAssignment);
    when(mock._cache.getClusterConfig()).thenReturn(_clusterConfig);
    when(mock._taskDataCache.getRuntimeJobDag(WORKFLOW_NAME)).thenReturn(mock._runtimeJobDag);
    _assignableInstanceManager.buildAssignableInstances(_clusterConfig, mock._taskDataCache,
        _liveInstances, _instanceConfigs);
    when(mock._cache.getAssignableInstanceManager()).thenReturn(_assignableInstanceManager);
    when(mock._cache.getExistsLiveInstanceOrCurrentStateChange()).thenReturn(true);
    Set<String> inflightJobDag = new HashSet<>();
    inflightJobDag.add(JOB_NAME);
    when(mock._taskDataCache.getRuntimeJobDag(WORKFLOW_NAME).getInflightJobList())
        .thenReturn(inflightJobDag);
    WorkflowDispatcher workflowDispatcher = new WorkflowDispatcher();
    workflowDispatcher.updateCache(mock._cache);
    BestPossibleStateOutput bestPossibleStateOutput = new BestPossibleStateOutput();
    workflowDispatcher.updateWorkflowStatus(WORKFLOW_NAME, mock._workflowConfig,
        mock._workflowContext, mock._currentStateOutput, bestPossibleStateOutput);
    Partition taskPartition = new Partition(JOB_NAME + "_" + PARTITION_NAME);
  }

  /**
   * Scenario:   JobState: Failing
   * Task Pid   | CurrState | PreviousAssignment  | Context   | Pending Messages
   * 0          | Init      | Running             | TimedOut  |
   * 1          | Completed | Aborted             | Completed | Completed -> Init
   * 2          | Running   | Aborted             | Running   | Running -> Aborted
   *
   * Result:     JobState: Failing
   * Task Pid   | CurrState | PreviousAssignment  | Context   | Messages
   * 0          | Init      | Null                | Init      |
   * 1          | Completed | Aborted             | Completed |
   * 2          | Running   | Aborted             | Running   |
   */
  @Test
  public void testNumberOfAttemptsPipeline2() {
    MockTestInformation mock = new MockTestInformation();
    when(mock._cache.getWorkflowConfig(WORKFLOW_NAME)).thenReturn(mock._workflowConfig);
    when(mock._cache.getJobConfig(JOB_NAME)).thenReturn(mock._jobConfig);
    when(mock._cache.getTaskDataCache()).thenReturn(mock._taskDataCache);
    when(mock._cache.getJobContext(JOB_NAME)).thenReturn(mock._jobContext2);
    when(mock._cache.getIdealStates()).thenReturn(mock._idealStates);
    when(mock._cache.getEnabledLiveInstances()).thenReturn(_liveInstances.keySet());
    when(mock._cache.getInstanceConfigMap()).thenReturn(_instanceConfigs);
    when(mock._cache.getTaskDataCache().getPreviousAssignment(JOB_NAME))
        .thenReturn(mock._resourceAssignment2);
    when(mock._cache.getClusterConfig()).thenReturn(_clusterConfig);
    when(mock._taskDataCache.getRuntimeJobDag(WORKFLOW_NAME)).thenReturn(mock._runtimeJobDag);
    _assignableInstanceManager.buildAssignableInstances(_clusterConfig, mock._taskDataCache,
        _liveInstances, _instanceConfigs);
    when(mock._cache.getAssignableInstanceManager()).thenReturn(_assignableInstanceManager);
    when(mock._cache.getExistsLiveInstanceOrCurrentStateChange()).thenReturn(true);
    Set<String> inflightJobDag = new HashSet<>();
    inflightJobDag.add(JOB_NAME);
    when(mock._taskDataCache.getRuntimeJobDag(WORKFLOW_NAME).getInflightJobList())
        .thenReturn(inflightJobDag);
    WorkflowDispatcher workflowDispatcher = new WorkflowDispatcher();
    workflowDispatcher.updateCache(mock._cache);
    BestPossibleStateOutput bestPossibleStateOutput = new BestPossibleStateOutput();
    workflowDispatcher.updateWorkflowStatus(WORKFLOW_NAME, mock._workflowConfig,
        mock._workflowContext2, mock._currentStateOutput2, bestPossibleStateOutput);
  }

  /**
   * Scenario:   JobState: Failing
   * Task Pid   | CurrState | PreviousAssignment  | Context   | Pending Messages
   * 0          | Init      | Null                | Init      |
   * 1          | Init      | Aborted             | Completed |
   * 2          | Running   | Aborted             | Running   | Running -> Aborted
   *
   * Result:     JobState: Failing
   * Task Pid   | CurrState | PreviousAssignment  | Context   | Messages
   * 0          | ?         | Running             | Init      |
   * 1          | ?         | Null                | Init      |
   * 2          | ?         | Abort               | Running   |
   *
   * Number of Attempts for Pid 0 will be 3 which is not correct.
   */
  @Test
  public void testNumberOfAttemptsPipeline3() {
    MockTestInformation mock = new MockTestInformation();
    when(mock._cache.getWorkflowConfig(WORKFLOW_NAME)).thenReturn(mock._workflowConfig);
    when(mock._cache.getJobConfig(JOB_NAME)).thenReturn(mock._jobConfig);
    when(mock._cache.getTaskDataCache()).thenReturn(mock._taskDataCache);
    when(mock._cache.getJobContext(JOB_NAME)).thenReturn(mock._jobContext3);
    when(mock._cache.getIdealStates()).thenReturn(mock._idealStates);
    when(mock._cache.getEnabledLiveInstances()).thenReturn(_liveInstances.keySet());
    when(mock._cache.getInstanceConfigMap()).thenReturn(_instanceConfigs);
    when(mock._cache.getTaskDataCache().getPreviousAssignment(JOB_NAME))
        .thenReturn(mock._resourceAssignment3);
    when(mock._cache.getClusterConfig()).thenReturn(_clusterConfig);
    when(mock._taskDataCache.getRuntimeJobDag(WORKFLOW_NAME)).thenReturn(mock._runtimeJobDag);
    _assignableInstanceManager.buildAssignableInstances(_clusterConfig, mock._taskDataCache,
        _liveInstances, _instanceConfigs);
    when(mock._cache.getAssignableInstanceManager()).thenReturn(_assignableInstanceManager);
    when(mock._cache.getExistsLiveInstanceOrCurrentStateChange()).thenReturn(true);
    Set<String> inflightJobDag = new HashSet<>();
    inflightJobDag.add(JOB_NAME);
    when(mock._taskDataCache.getRuntimeJobDag(WORKFLOW_NAME).getInflightJobList())
        .thenReturn(inflightJobDag);
    WorkflowDispatcher workflowDispatcher = new WorkflowDispatcher();
    workflowDispatcher.updateCache(mock._cache);
    BestPossibleStateOutput bestPossibleStateOutput = new BestPossibleStateOutput();
    workflowDispatcher.updateWorkflowStatus(WORKFLOW_NAME, mock._workflowConfig,
        mock._workflowContext2, mock._currentStateOutput3, bestPossibleStateOutput);
  }

  private WorkflowConfig prepareWorkflowConfig() {
    WorkflowConfig.Builder workflowConfigBuilder = new WorkflowConfig.Builder();
    workflowConfigBuilder.setWorkflowId(WORKFLOW_NAME);
    workflowConfigBuilder.setTerminable(true);
    workflowConfigBuilder.setTargetState(TargetState.START);
    workflowConfigBuilder.setJobQueue(false);
    workflowConfigBuilder.setParallelJobs(1);
    workflowConfigBuilder.setAllowOverlapJobAssignment(false);
    workflowConfigBuilder.setCapacity(Integer.MAX_VALUE);
    JobDag jobDag = new JobDag();
    jobDag.addNode(JOB_NAME);
    workflowConfigBuilder.setJobDag(jobDag);
    return workflowConfigBuilder.build();
  }

  private JobConfig prepareJobConfig() {
    JobConfig.Builder jobConfigBuilder = new JobConfig.Builder();
    jobConfigBuilder.setWorkflow(WORKFLOW_NAME);
    jobConfigBuilder.setCommand("TestCommand");
    jobConfigBuilder.setTargetResource(TARGET_RESOURCES);
    jobConfigBuilder.setJobId(JOB_NAME);
    List<TaskConfig> taskConfigs = new ArrayList<>();
    TaskConfig.Builder taskConfigBuilder0 = new TaskConfig.Builder();
    TaskConfig.Builder taskConfigBuilder1 = new TaskConfig.Builder();
    TaskConfig.Builder taskConfigBuilder2 = new TaskConfig.Builder();
    taskConfigs.add(taskConfigBuilder0.build());
    taskConfigs.add(taskConfigBuilder1.build());
    taskConfigs.add(taskConfigBuilder2.build());
    jobConfigBuilder.addTaskConfigs(taskConfigs);
    jobConfigBuilder.setFailureThreshold(0);
    jobConfigBuilder.setRebalanceRunningTask(false);
    jobConfigBuilder.setMaxAttemptsPerTask(2);
    return jobConfigBuilder.build();
  }

  private WorkflowContext prepareWorkflowContext(String jobStatus) {
    ZNRecord record = new ZNRecord(WORKFLOW_NAME);
    record.setSimpleField(WorkflowContext.WorkflowContextProperties.StartTime.name(), "0");
    record.setSimpleField(WorkflowContext.WorkflowContextProperties.NAME.name(), WORKFLOW_NAME);
    record.setSimpleField(WorkflowContext.WorkflowContextProperties.STATE.name(),
        TaskState.IN_PROGRESS.name());
    Map<String, String> jobState = new HashMap<>();
    jobState.put(JOB_NAME, jobStatus);
    record.setMapField(WorkflowContext.WorkflowContextProperties.JOB_STATES.name(), jobState);
    return new WorkflowContext(record);
  }

  private JobContext prepareJobContext(String instance0, TaskPartitionState state0, String instance1, TaskPartitionState state1, String instance2, TaskPartitionState state2) {
    ZNRecord record = new ZNRecord(JOB_NAME);
    JobContext jobContext = new JobContext(record);
    jobContext.setStartTime(0L);
    jobContext.setName(JOB_NAME);
    jobContext.setStartTime(0L);
    jobContext.setPartitionState(0, state0);
    jobContext.setPartitionNumAttempts(0,2);
    jobContext.setAssignedParticipant(0, instance0);
    jobContext.setPartitionTarget(0, TARGET_RESOURCES + "_0");

    jobContext.setPartitionState(1, state1);
    jobContext.setPartitionNumAttempts(1,1);
    jobContext.setAssignedParticipant(1, instance1);
    jobContext.setPartitionTarget(1, TARGET_RESOURCES + "_1");

    jobContext.setPartitionState(2, state2);
    jobContext.setPartitionNumAttempts(2,1);
    jobContext.setAssignedParticipant(2, instance2);
    jobContext.setPartitionTarget(2, TARGET_RESOURCES + "_2");

    return jobContext;
  }

  private Map<String, IdealState> prepareIdealStates(String instance1, String instance2,
      String instance3) {
    ZNRecord record = new ZNRecord(JOB_NAME);
    record.setSimpleField(IdealState.IdealStateProperty.NUM_PARTITIONS.name(), "3");
    record.setSimpleField(IdealState.IdealStateProperty.EXTERNAL_VIEW_DISABLED.name(), "true");
    record.setSimpleField(IdealState.IdealStateProperty.IDEAL_STATE_MODE.name(), "AUTO");
    record.setSimpleField(IdealState.IdealStateProperty.REBALANCE_MODE.name(), "TASK");
    record.setSimpleField(IdealState.IdealStateProperty.REPLICAS.name(), "1");
    record.setSimpleField(IdealState.IdealStateProperty.STATE_MODEL_DEF_REF.name(), "Task");
    record.setSimpleField(IdealState.IdealStateProperty.STATE_MODEL_FACTORY_NAME.name(), "DEFAULT");
    record.setSimpleField(IdealState.IdealStateProperty.REBALANCER_CLASS_NAME.name(),
        "org.apache.helix.task.JobRebalancer");
    record.setMapField(JOB_NAME + "_0", new HashMap<>());
    record.setListField(JOB_NAME + "_0", new ArrayList<>());
    record.setMapField(JOB_NAME + "_1", new HashMap<>());
    record.setListField(JOB_NAME + "_1", new ArrayList<>());
    record.setMapField(JOB_NAME + "_2", new HashMap<>());
    record.setListField(JOB_NAME + "_2", new ArrayList<>());
    Map<String, IdealState> idealStates = new HashMap<>();
    idealStates.put(JOB_NAME, new IdealState(record));

    ZNRecord recordDB = new ZNRecord(TARGET_RESOURCES);
    recordDB.setSimpleField(IdealState.IdealStateProperty.REPLICAS.name(), "3");
    recordDB.setSimpleField(IdealState.IdealStateProperty.REBALANCE_MODE.name(), "FULL_AUTO");
    record.setSimpleField(IdealState.IdealStateProperty.IDEAL_STATE_MODE.name(), "AUTO_REBALANCE");
    record.setSimpleField(IdealState.IdealStateProperty.STATE_MODEL_DEF_REF.name(), "MasterSlave");
    record.setSimpleField(IdealState.IdealStateProperty.STATE_MODEL_DEF_REF.name(),
        "org.apache.helix.controller.rebalancer.strategy.CrushEdRebalanceStrategy");
    record.setSimpleField(IdealState.IdealStateProperty.REBALANCER_CLASS_NAME.name(),
        "org.apache.helix.controller.rebalancer.DelayedAutoRebalancer");
    Map<String, String> mapping = new HashMap<>();
    mapping.put(instance1, "MASTER");
    recordDB.setMapField(TARGET_RESOURCES + "_0", mapping);
    List<String> listField = new ArrayList<>();
    listField.add(instance1);
    recordDB.setListField(TARGET_RESOURCES + "_0", listField);

    mapping = new HashMap<>();
    mapping.put(instance2, "MASTER");
    recordDB.setMapField(TARGET_RESOURCES + "_1", mapping);
    listField = new ArrayList<>();
    listField.add(instance2);
    recordDB.setListField(TARGET_RESOURCES + "_1", listField);

    mapping = new HashMap<>();
    mapping.put(instance3, "MASTER");
    recordDB.setMapField(TARGET_RESOURCES + "_2", mapping);
    listField = new ArrayList<>();
    listField.add(instance3);
    recordDB.setListField(TARGET_RESOURCES + "_2", listField);

    idealStates.put(TARGET_RESOURCES, new IdealState(recordDB));

    return idealStates;
  }

  private CurrentStateOutput prepareCurrentState(String instance1, String state1, String instance2, String state2, String instance3, String state3) {
    CurrentStateOutput currentStateOutput = new CurrentStateOutput();
    currentStateOutput.setResourceStateModelDef(JOB_NAME, "TASK");
    currentStateOutput.setBucketSize(JOB_NAME, 0);
    Partition taskPartition1 = new Partition(JOB_NAME + "_0");
    currentStateOutput.setEndTime(JOB_NAME, taskPartition1, instance1, 0L);
    currentStateOutput.setCurrentState(JOB_NAME, taskPartition1, instance1, state1);
    currentStateOutput.setInfo(JOB_NAME, taskPartition1, instance1, "");

    currentStateOutput.setBucketSize(JOB_NAME, 0);
    Partition taskPartition2 = new Partition(JOB_NAME + "_1");
    currentStateOutput.setEndTime(JOB_NAME, taskPartition2, instance2, 0L);
    currentStateOutput.setCurrentState(JOB_NAME, taskPartition2, instance2, state2);
    currentStateOutput.setInfo(JOB_NAME, taskPartition2, instance2, "");

    currentStateOutput.setBucketSize(JOB_NAME, 0);
    Partition taskPartition3 = new Partition(JOB_NAME + "_2");
    currentStateOutput.setEndTime(JOB_NAME, taskPartition3, instance3, 0L);
    currentStateOutput.setCurrentState(JOB_NAME, taskPartition3, instance3, state3);
    currentStateOutput.setInfo(JOB_NAME, taskPartition3, instance3, "");

    currentStateOutput.setResourceStateModelDef(TARGET_RESOURCES, "MasterSlave");
    currentStateOutput.setBucketSize(TARGET_RESOURCES, 0);

    Partition dbPartition = new Partition(TARGET_RESOURCES + "_0");
    currentStateOutput.setEndTime(TARGET_RESOURCES, dbPartition, instance1, 0L);
    currentStateOutput.setCurrentState(TARGET_RESOURCES, dbPartition, instance1, "MASTER");
    currentStateOutput.setInfo(TARGET_RESOURCES, dbPartition, instance1, "");


    dbPartition = new Partition(TARGET_RESOURCES + "_1");
    currentStateOutput.setEndTime(TARGET_RESOURCES, dbPartition, instance2, 0L);
    currentStateOutput.setCurrentState(TARGET_RESOURCES, dbPartition, instance2, "MASTER");
    currentStateOutput.setInfo(TARGET_RESOURCES, dbPartition, instance2, "");


    dbPartition = new Partition(TARGET_RESOURCES + "_2");
    currentStateOutput.setEndTime(TARGET_RESOURCES, dbPartition, instance3, 0L);
    currentStateOutput.setCurrentState(TARGET_RESOURCES, dbPartition, instance3, "MASTER");
    currentStateOutput.setInfo(TARGET_RESOURCES, dbPartition, instance3, "");

    Message message = new Message(Message.MessageType.STATE_TRANSITION, "123456789");
    message.setFromState(TaskPartitionState.TIMED_OUT.name());
    message.setToState(TaskPartitionState.INIT.name());
    currentStateOutput.setPendingMessage(JOB_NAME, taskPartition1, instance1, message);
    //currentStateOutput.setRequestedState(JOB_NAME, taskPartition, masterInstance, TaskPartitionState.TIMED_OUT.name());


    return currentStateOutput;
  }

  private CurrentStateOutput prepareCurrentState2(String instance1, String state1, String instance2, String state2, String instance3, String state3) {
    CurrentStateOutput currentStateOutput = new CurrentStateOutput();
    currentStateOutput.setResourceStateModelDef(JOB_NAME, "TASK");
    currentStateOutput.setBucketSize(JOB_NAME, 0);
    Partition taskPartition1 = new Partition(JOB_NAME + "_0");
    currentStateOutput.setEndTime(JOB_NAME, taskPartition1, instance1, 0L);
    currentStateOutput.setCurrentState(JOB_NAME, taskPartition1, instance1, state1);
    currentStateOutput.setInfo(JOB_NAME, taskPartition1, instance1, "");

    currentStateOutput.setBucketSize(JOB_NAME, 0);
    Partition taskPartition2 = new Partition(JOB_NAME + "_1");
    currentStateOutput.setEndTime(JOB_NAME, taskPartition2, instance2, 0L);
    currentStateOutput.setCurrentState(JOB_NAME, taskPartition2, instance2, state2);
    currentStateOutput.setInfo(JOB_NAME, taskPartition2, instance2, "");

    currentStateOutput.setBucketSize(JOB_NAME, 0);
    Partition taskPartition3 = new Partition(JOB_NAME + "_2");
    currentStateOutput.setEndTime(JOB_NAME, taskPartition3, instance3, 0L);
    currentStateOutput.setCurrentState(JOB_NAME, taskPartition3, instance3, state3);
    currentStateOutput.setInfo(JOB_NAME, taskPartition3, instance3, "");

    currentStateOutput.setResourceStateModelDef(TARGET_RESOURCES, "MasterSlave");
    currentStateOutput.setBucketSize(TARGET_RESOURCES, 0);

    Partition dbPartition = new Partition(TARGET_RESOURCES + "_0");
    currentStateOutput.setEndTime(TARGET_RESOURCES, dbPartition, instance1, 0L);
    currentStateOutput.setCurrentState(TARGET_RESOURCES, dbPartition, instance1, "MASTER");
    currentStateOutput.setInfo(TARGET_RESOURCES, dbPartition, instance1, "");


    dbPartition = new Partition(TARGET_RESOURCES + "_1");
    currentStateOutput.setEndTime(TARGET_RESOURCES, dbPartition, instance2, 0L);
    currentStateOutput.setCurrentState(TARGET_RESOURCES, dbPartition, instance2, "MASTER");
    currentStateOutput.setInfo(TARGET_RESOURCES, dbPartition, instance2, "");


    dbPartition = new Partition(TARGET_RESOURCES + "_2");
    currentStateOutput.setEndTime(TARGET_RESOURCES, dbPartition, instance3, 0L);
    currentStateOutput.setCurrentState(TARGET_RESOURCES, dbPartition, instance3, "MASTER");
    currentStateOutput.setInfo(TARGET_RESOURCES, dbPartition, instance3, "");

    Message message = new Message(Message.MessageType.STATE_TRANSITION, "123456789");
    message.setFromState(TaskPartitionState.COMPLETED.name());
    message.setToState(TaskPartitionState.INIT.name());
    currentStateOutput.setPendingMessage(JOB_NAME, taskPartition2, instance2, message);

    Message message2 = new Message(Message.MessageType.STATE_TRANSITION, "123456780");
    message2.setFromState(TaskPartitionState.RUNNING.name());
    message2.setToState(TaskPartitionState.TASK_ABORTED.name());
    currentStateOutput.setPendingMessage(JOB_NAME, taskPartition3, instance3, message2);

    return currentStateOutput;
  }

  private CurrentStateOutput prepareCurrentState3(String instance1, String state1, String instance2, String state2, String instance3, String state3) {
    CurrentStateOutput currentStateOutput = new CurrentStateOutput();
    currentStateOutput.setResourceStateModelDef(JOB_NAME, "TASK");
    currentStateOutput.setBucketSize(JOB_NAME, 0);
    Partition taskPartition1 = new Partition(JOB_NAME + "_0");
    currentStateOutput.setEndTime(JOB_NAME, taskPartition1, instance1, 0L);
    currentStateOutput.setCurrentState(JOB_NAME, taskPartition1, instance1, state1);
    currentStateOutput.setInfo(JOB_NAME, taskPartition1, instance1, "");

    currentStateOutput.setBucketSize(JOB_NAME, 0);
    Partition taskPartition2 = new Partition(JOB_NAME + "_1");
    currentStateOutput.setEndTime(JOB_NAME, taskPartition2, instance2, 0L);
    currentStateOutput.setCurrentState(JOB_NAME, taskPartition2, instance2, state2);
    currentStateOutput.setInfo(JOB_NAME, taskPartition2, instance2, "");

    currentStateOutput.setBucketSize(JOB_NAME, 0);
    Partition taskPartition3 = new Partition(JOB_NAME + "_2");
    currentStateOutput.setEndTime(JOB_NAME, taskPartition3, instance3, 0L);
    currentStateOutput.setCurrentState(JOB_NAME, taskPartition3, instance3, state3);
    currentStateOutput.setInfo(JOB_NAME, taskPartition3, instance3, "");

    currentStateOutput.setResourceStateModelDef(TARGET_RESOURCES, "MasterSlave");
    currentStateOutput.setBucketSize(TARGET_RESOURCES, 0);

    Partition dbPartition = new Partition(TARGET_RESOURCES + "_0");
    currentStateOutput.setEndTime(TARGET_RESOURCES, dbPartition, instance1, 0L);
    currentStateOutput.setCurrentState(TARGET_RESOURCES, dbPartition, instance1, "MASTER");
    currentStateOutput.setInfo(TARGET_RESOURCES, dbPartition, instance1, "");


    dbPartition = new Partition(TARGET_RESOURCES + "_1");
    currentStateOutput.setEndTime(TARGET_RESOURCES, dbPartition, instance2, 0L);
    currentStateOutput.setCurrentState(TARGET_RESOURCES, dbPartition, instance2, "MASTER");
    currentStateOutput.setInfo(TARGET_RESOURCES, dbPartition, instance2, "");


    dbPartition = new Partition(TARGET_RESOURCES + "_2");
    currentStateOutput.setEndTime(TARGET_RESOURCES, dbPartition, instance3, 0L);
    currentStateOutput.setCurrentState(TARGET_RESOURCES, dbPartition, instance3, "MASTER");
    currentStateOutput.setInfo(TARGET_RESOURCES, dbPartition, instance3, "");

    Message message2 = new Message(Message.MessageType.STATE_TRANSITION, "123456780");
    message2.setFromState(TaskPartitionState.RUNNING.name());
    message2.setToState(TaskPartitionState.TASK_ABORTED.name());
    currentStateOutput.setPendingMessage(JOB_NAME, taskPartition3, instance3, message2);

    return currentStateOutput;
  }

  private ResourceAssignment preparePreviousAssignment(String instance1, String state1, String instance2, String state2,String instance3, String state3) {
    ResourceAssignment prevAssignment = new ResourceAssignment(JOB_NAME);
    Map<String, String> replicaMap = new HashMap<>();
    if (state1 != null) {
      replicaMap.put(instance1, state1);
      Partition taskPartition = new Partition(JOB_NAME + "_0");
      prevAssignment.addReplicaMap(taskPartition, replicaMap);
    }
    replicaMap = new HashMap<>();
    replicaMap.put(instance2, state2);
    Partition taskPartition1 = new Partition(JOB_NAME + "_1");
    prevAssignment.addReplicaMap(taskPartition1, replicaMap);

    replicaMap = new HashMap<>();
    replicaMap.put(instance3, state3);
    Partition taskPartition2 = new Partition(JOB_NAME + "_2");
    prevAssignment.addReplicaMap(taskPartition2, replicaMap);

    return prevAssignment;
  }

  private class MockTestInformation {
    private static final String INSTANCE1 = INSTANCE_PREFIX + "0";
    private static final String INSTANCE2 = INSTANCE_PREFIX + "1";
    private static final String INSTANCE3 = INSTANCE_PREFIX + "2";

    private WorkflowControllerDataProvider _cache = mock(WorkflowControllerDataProvider.class);
    private WorkflowConfig _workflowConfig = prepareWorkflowConfig();
    private WorkflowContext _workflowContext = prepareWorkflowContext(TaskState.IN_PROGRESS.name());
    private Map<String, IdealState> _idealStates =
        prepareIdealStates(INSTANCE1, INSTANCE2, INSTANCE3);
    private JobConfig _jobConfig = prepareJobConfig();
    private JobContext _jobContext = prepareJobContext(INSTANCE1, TaskPartitionState.INIT ,INSTANCE2, TaskPartitionState.COMPLETED, INSTANCE3, TaskPartitionState.RUNNING);
    private CurrentStateOutput _currentStateOutput =
        prepareCurrentState(INSTANCE1, TaskPartitionState.TIMED_OUT.name(), INSTANCE2, TaskPartitionState.COMPLETED.name(), INSTANCE3, TaskPartitionState.RUNNING.name());
    private ResourceAssignment _resourceAssignment =
        preparePreviousAssignment(INSTANCE1, TaskPartitionState.RUNNING.name(), INSTANCE2, TaskPartitionState.COMPLETED.name(), INSTANCE3, TaskPartitionState.RUNNING.name());
    private TaskDataCache _taskDataCache = mock(TaskDataCache.class);
    private RuntimeJobDag _runtimeJobDag = mock(RuntimeJobDag.class);

    private JobContext _jobContext2 = prepareJobContext(INSTANCE1, TaskPartitionState.TIMED_OUT ,INSTANCE2, TaskPartitionState.COMPLETED, INSTANCE3, TaskPartitionState.RUNNING);
    private ResourceAssignment _resourceAssignment2 =
        preparePreviousAssignment(INSTANCE1, TaskPartitionState.RUNNING.name(), INSTANCE2, TaskPartitionState.TASK_ABORTED.name(), INSTANCE3, TaskPartitionState.TASK_ABORTED.name());
    private WorkflowContext _workflowContext2 = prepareWorkflowContext(TaskState.FAILING.name());
    private CurrentStateOutput _currentStateOutput2 =
        prepareCurrentState2(INSTANCE1, TaskPartitionState.INIT.name(), INSTANCE2, TaskPartitionState.COMPLETED.name(), INSTANCE3, TaskPartitionState.RUNNING.name());

    private ResourceAssignment _resourceAssignment3 =
        preparePreviousAssignment(INSTANCE1, null, INSTANCE2, TaskPartitionState.TASK_ABORTED.name(), INSTANCE3, TaskPartitionState.TASK_ABORTED.name());
    private JobContext _jobContext3 = prepareJobContext(INSTANCE1, TaskPartitionState.INIT ,INSTANCE2, TaskPartitionState.COMPLETED, INSTANCE3, TaskPartitionState.RUNNING);
    private CurrentStateOutput _currentStateOutput3 =
        prepareCurrentState3(INSTANCE1, TaskPartitionState.INIT.name(), INSTANCE2, TaskPartitionState.INIT.name(), INSTANCE3, TaskPartitionState.RUNNING.name());
    MockTestInformation() {
    }
  }
}