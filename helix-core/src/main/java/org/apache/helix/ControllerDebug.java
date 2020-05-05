package org.apache.helix;

import java.util.Iterator;
import java.util.Set;
import org.apache.helix.controller.dataproviders.ResourceControllerDataProvider;
import org.apache.helix.controller.dataproviders.WorkflowControllerDataProvider;
import org.apache.helix.controller.pipeline.Stage;
import org.apache.helix.controller.pipeline.StageContext;
import org.apache.helix.controller.stages.AttributeName;
import org.apache.helix.controller.stages.BestPossibleStateCalcStage;
import org.apache.helix.controller.stages.ClusterDataCache;
import org.apache.helix.controller.stages.ClusterEvent;
import org.apache.helix.controller.stages.ClusterEventType;
import org.apache.helix.controller.stages.CurrentStateComputationStage;
import org.apache.helix.controller.stages.IntermediateStateCalcStage;
import org.apache.helix.controller.stages.ReadClusterDataStage;
import org.apache.helix.controller.stages.ResourceComputationStage;
import org.apache.helix.controller.stages.ResourceValidationStage;
import org.apache.helix.controller.stages.task.TaskSchedulingStage;

public class ControllerDebug {
  public static void main(String[] args) throws Exception {
    //    String zkaddress = "zk-lor1-espresso.prod.linkedin.com:12913";
    String zkaddress = "localhost:4185";
    String clusterName = "ESPRESSO-DATA-VALIDATION";
    HelixManager manager = HelixManagerFactory
        .getZKHelixManager(clusterName, "Test", InstanceType.ADMINISTRATOR, zkaddress);
    manager.connect();

    // double[] percents = new double[] {1.0, 0.7, 0.5, 0.3, 0.2, 0.1, 0};
    double[] percents = new double[] { 0 };
    for (double percent : percents) {
      System.out.println("Process with removing percentage : " + (percent * 100) + "%");
      process(percent, manager);
      System.out.println();
      System.out.println();
    }
    manager.disconnect();
  }

  protected static void process(double percent, HelixManager manager) {
    ClusterEvent event = new ClusterEvent(ClusterEventType.Unknown);
    event.addAttribute(AttributeName.helixmanager.name(), manager);
    event.addAttribute(AttributeName.changeContext.name(), new NotificationContext(manager));
    //    cache.setTaskCache(true);
    event.addAttribute(AttributeName.ControllerDataProvider.name(), new WorkflowControllerDataProvider("ESPRESSO-DATA-VALIDATION"));
    ResourceComputationStage computationStage = new ResourceComputationStage();
    ResourceValidationStage validationStage = new ResourceValidationStage();
    CurrentStateComputationStage currentStateComputationStage = new CurrentStateComputationStage();
    BestPossibleStateCalcStage bestPossibleStateCalcStage = new BestPossibleStateCalcStage();
    ReadClusterDataStage readClusterDataStage = new ReadClusterDataStage();
    IntermediateStateCalcStage intermediateStateCalcStage = new IntermediateStateCalcStage();

    System.out.print("First Pipeline");
    runStage(event, readClusterDataStage);
    System.out.println("Read Data Cache Complete");
    runStage(event, computationStage);
    System.out.println("Resource Computation Complete");
    runStage(event, validationStage);
    System.out.println("Resource validation Complete");
    runStage(event, currentStateComputationStage);
    System.out.println("Current state Complete");
    runStage(event, new TaskSchedulingStage());
    System.out.println("Best possible Complete");
    runStage(event, intermediateStateCalcStage);
    System.out.println("Intermediate Complete");

    //    ClusterDataCache cache = event.getAttribute(AttributeName.ClusterDataCache.name());
    //    int numberToRemove = (int) (percent * cache.getCachedIdealMapping().size());
    //    System.out.println(numberToRemove + " idealstates are removed. " + (percent * 100)
    //        + "% of idealstates got refreshed.");
    //
    //    Iterator<String> idealStateKeys =  ((ClusterDataCache) event
    //        .getAttribute(AttributeName.ClusterDataCache.name())).getOriginMapping().keySet().iterator();
    //    while (numberToRemove-- > 0) {
    //      idealStateKeys.remove();
    //    }
    //
    //    System.out.println("Second Pipeline");
    //    long startTime = System.currentTimeMillis();
    //    runStage(event, readClusterDataStage);
    //    System.out.println("Read Data Cache Complete with " + (System.currentTimeMillis() - startTime) + "ms");
    //    startTime = System.currentTimeMillis();
    //    runStage(event, computationStage);
    //    System.out.println("Resource Computation Complete with " + (System.currentTimeMillis() - startTime) + "ms");
    //    startTime = System.currentTimeMillis();
    //    runStage(event, validationStage);
    //    System.out.println("Resource validation Complete with " + (System.currentTimeMillis() - startTime) + "ms");
    //    startTime = System.currentTimeMillis();
    //    runStage(event, currentStateComputationStage);
    //    System.out.println("Current state Complete with " + (System.currentTimeMillis() - startTime) + "ms");
    //    startTime = System.currentTimeMillis();
    //    runStage(event, bestPossibleStateCalcStage);
    //    System.out.println("Best possible Complete with " + (System.currentTimeMillis() - startTime) + "ms");
  }

  protected static void runStage(ClusterEvent event, Stage stage) {
    StageContext context = new StageContext();
    stage.init(context);
    stage.preProcess();
    try {
      stage.process(event);
    } catch (Exception e) {
      e.printStackTrace();
    }
    stage.postProcess();
  }
}
