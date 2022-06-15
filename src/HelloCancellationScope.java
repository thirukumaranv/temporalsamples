/*
 *  Copyright (c) 2020 Temporal Technologies, Inc. All Rights Reserved
 *
 *  Copyright 2012-2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *  Modifications copyright (C) 2017 Uber Technologies, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"). You may not
 *  use this file except in compliance with the License. A copy of the License is
 *  located at
 *
 *  http://aws.amazon.com/apache2.0
 *
 *  or in the "license" file accompanying this file. This file is distributed on
 *  an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *  express or implied. See the License for the specific language governing
 *  permissions and limitations under the License.
 */

package io.temporal.samples.hello;

import io.temporal.activity.*;
import io.temporal.api.common.v1.WorkflowExecution;
import io.temporal.client.ActivityCompletionException;
import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowOptions;
import io.temporal.common.RetryOptions;
import io.temporal.serviceclient.WorkflowServiceStubs;
import io.temporal.worker.Worker;
import io.temporal.worker.WorkerFactory;
import io.temporal.worker.WorkerOptions;
import io.temporal.workflow.*;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * Sample Temporal Workflow Definition that demonstrates parallel Activity Executions with a
 * Cancellation Scope. When one of the Activity Executions finish, we cancel the execution of the
 * other Activities and wait for their cancellation to complete.
 */
public class HelloCancellationScope {

  public static final String CANCEL_CONST = "CANCEL";

  // Define the task queue name
  static final String TASK_QUEUE = "HelloCancellationScopeTaskQueue";

  // Define our workflow unique id
  static final String WORKFLOW_ID = "HelloCancellationScopeWorkflow";

  private static final int ACTIVITY_MAX_SLEEP_SECONDS = 30;
  private static final int ACTIVITY_MAX_CLEANUP_SECONDS = 5;
  private static final int ACTIVITY_START_TO_CLOSE_TIMEOUT =
          ACTIVITY_MAX_SLEEP_SECONDS + ACTIVITY_MAX_CLEANUP_SECONDS + 10;

  private static final String[] greetings =
          new String[] {
                  "Hallo1", "Hallo2", "Hallo3", "Hallo4", "Hallo5", "Hallo6", "Hallo7", "Hallo8", "Hallo9",
                  "Hallo10", "Hallo11", "Hallo12", "Hallo13", "Hallo14", "Hallo15", "Hallo16", "Hallo17",
                  "Hallo18", "Hallo19", "Hallo20", "Hallo21", "Hallo22", "Hallo23", "Hallo24"
          };

  @WorkflowInterface
  public interface ParentWorkflow {
    @WorkflowMethod
    String runParentWorkflow(String name);

    @SignalMethod
    void cancel();
  }

  @WorkflowInterface
  public interface ChildWorkflow {
    @WorkflowMethod
    String runChildWorkflow(String parentName);
  }

  public static class ChildWorkflowImpl implements ChildWorkflow {

    @Override
    public String runChildWorkflow(String parentName) {
      final GreetingActivities activities =
              Workflow.newActivityStub(
                      GreetingActivities.class,
                      ActivityOptions.newBuilder()
                              .setStartToCloseTimeout(Duration.ofSeconds(ACTIVITY_START_TO_CLOSE_TIMEOUT))
                              .setCancellationType(ActivityCancellationType.WAIT_CANCELLATION_COMPLETED)
                              .setRetryOptions(
                                      RetryOptions.newBuilder().setMaximumAttempts(1).setDoNotRetry().build())
                              .build());
      System.out.println("Inside child workflow: Start of workflow method");
      List<Promise<String>> results = new ArrayList<>(greetings.length);
      // trigger a bunch of parallel activities and wait for their completion
      for (String greeting : greetings) {
        System.out.println("Scheduling greeting Activity: " + greeting);
        results.add(
                Async.function(
                        () -> {
                          String result = activities.greetingActivity(greeting, "dummyname");
                          return handleActivityResult(result);
                        }));
      }
      // wait for all activities to complete
      System.out.println(
              "Inside child workflow: scheduled and waiting for activities to compelete");
      results.forEach(Promise::get);
      System.out.println("Inside child workflow: Got all results");
      return "RAN CHILD WORFLOW SUCCESSFULLY";
    }

    //triggers a local activity to send an external message
    private String handleActivityResult(String result) {
      MsgActivities msgActivty =
              Workflow.newLocalActivityStub(
                      MsgActivities.class,
                      LocalActivityOptions.newBuilder()
                              .setStartToCloseTimeout(Duration.ofMinutes(1))
                              .setRetryOptions(
                                      RetryOptions.newBuilder().setMaximumAttempts(1).setDoNotRetry().build())
                              .build());
      return msgActivty.sendMsg(result, "Dummy Message");
    }
  }

  @ActivityInterface
  public interface MsgActivities {
    String sendMsg(String greeting, String name);
  }

  public static class MsgActivitiesImpl implements MsgActivities {
    @Override
    public String sendMsg(String greeting, String name) {
      System.out.println("Inside Send Message ACtivity");
      try {
        Thread.sleep(3 * 1000);
      } catch (InterruptedException ee) {
        // Empty
      }
      return "MSG Sent";
    }
  }

  @ActivityInterface
  public interface GreetingActivities {
    String greetingActivity(String greeting, String name);
  }

  // Define the workflow implementation which implements our getGreeting workflow method.
  public static class ParentWorkflowImpl implements ParentWorkflow {

    public ParentWorkflowImpl() {}

    // this string will be null but will be set to CANCEL_CONST str when the cancel signal method is
    // called
    // once this cancelString is set to CANCEL_CONST, the cancellationscope that is executing will
    // be cancelled
    // the cancellation scope is executing child workflow(which inturn has scheduled activities) and
    // the cancellation
    // scope should stop its execution
    private String cancelString = " ";

    @Override
    public String runParentWorkflow(String name) {
      System.out.println("Inside Parent workflow: inside runParentGreeting method");
      List<Promise<Void>> results = new ArrayList<>(greetings.length);
      /*
       * Create our CancellationScope. Within this scope we call the workflow
       * composeGreeting method asynchronously for each of our defined greetings in different
       * languages.
       */
      CancellationScope scope =
              Workflow.newCancellationScope(
                      () -> {
                        System.out.println("Inside Parent workflow: start of cancellation scope");
                        final ChildWorkflowOptions childWorkflowOptions =
                                ChildWorkflowOptions.newBuilder()
                                        .setRetryOptions(RetryOptions.newBuilder().setMaximumAttempts(1).build())
                                        .setCancellationType(
                                                ChildWorkflowCancellationType.WAIT_CANCELLATION_COMPLETED)
                                        .setTaskQueue(TASK_QUEUE)
                                        .build();
                        ChildWorkflow cwf =
                                Workflow.newChildWorkflowStub(ChildWorkflow.class, childWorkflowOptions);
                        Promise<Void> result =
                                Async.procedure(cwf::runChildWorkflow, "ChildWorkflowGreetingArgument");
                        results.add(result);
                        // This is to wait till the execution of the workflow starts and NOT wait for the
                        // workflow to complete
                        // execution
                        Promise<WorkflowExecution> executionPromise = Workflow.getWorkflowExecution(cwf);
                        executionPromise.get();
                        System.out.println("Inside Parent workflow: scheduled child workflow execution");
                      });

      /*
       * Execute all activities within the CancellationScope. Note that this execution is
       * non-blocking as the code inside our cancellation scope is also non-blocking.
       */
      scope.run();
      System.out.println("Inside Parent workflow: scope run started");

      // wait for the cancel() signal method to be called on the parentworkflow. the signal method
      // will set the CANCEL_CONST string
      Workflow.await(
              () -> {
                System.out.println("waiting for cancel");
                return CANCEL_CONST.equalsIgnoreCase(cancelString);
              });
      System.out.println("RECEIVED CANCEL");
      scope.cancel();
      System.out.println("Inside Parent workflow: scope cancel done");

      // query the result after cancel. This should ideally throw and exception and come out
      results.get(0).get();
      System.out.println("Inside Parent workflow: Got the child workflow result");

      System.out.println("CANCEL COMPLETE");

      return "CANCEL COMPLETE";
    }

    public void cancel() {
      System.out.println("CANCEL SIGNAL METHOD CALLED");
      cancelString = CANCEL_CONST;
    }
  }

  public static class GreetingActivitiesImpl implements GreetingActivities {

    @Override
    public String greetingActivity(String greeting, String name) {
      System.out.println("Inside Activity: greeting name: " + greeting);
      // Get the activity execution context
      ActivityExecutionContext context = Activity.getExecutionContext();

      int seconds = 30;
      for (int i = 0; i < seconds; i++) {
        sleep(1);
        try {
          // Perform the heartbeat. Used to notify the workflow that activity execution is alive
          context.heartbeat(i);
        } catch (ActivityCompletionException e) {
          // complete any cleanup
          System.out.println("start cleanup" + greeting);
          sleep(seconds);
          System.out.println("End cleanup" + greeting);
          throw e;
        }
      }
      // return results of activity invocation
      System.out.println("Activity for " + greeting + " completed");
      return greeting + " " + name + "!";
    }

    public static void sleep(int seconds) {
      try {
        Thread.sleep(seconds * 1000);
      } catch (InterruptedException ee) {
        // Empty
      }
    }
  }

  public static void main(String[] args) {

    WorkflowServiceStubs service = WorkflowServiceStubs.newLocalServiceStubs();
    WorkflowClient client = WorkflowClient.newInstance(service);
    WorkerFactory factory = WorkerFactory.newInstance(client);
    Worker worker =
            factory.newWorker(
                    TASK_QUEUE,
                    WorkerOptions.newBuilder().setMaxConcurrentActivityExecutionSize(2).build());

    worker.registerWorkflowImplementationTypes(ParentWorkflowImpl.class);
    worker.registerWorkflowImplementationTypes(ChildWorkflowImpl.class);
    worker.registerActivitiesImplementations(new GreetingActivitiesImpl());
    worker.registerActivitiesImplementations(new MsgActivitiesImpl());
    factory.start();

    // Create the workflow client stub. It is used to start our workflow execution asynchronously.
    ParentWorkflow workflow =
            client.newWorkflowStub(
                    ParentWorkflow.class,
                    WorkflowOptions.newBuilder()
                            .setWorkflowId(WORKFLOW_ID)
                            .setTaskQueue(TASK_QUEUE)
                            .setRetryOptions(RetryOptions.newBuilder().setMaximumAttempts(1).build())
                            .build());

    System.out.println("Starting workflow");
    WorkflowClient.execute(workflow::runParentWorkflow, "World");
    System.out.println("WORKFLOW IN PROGRESS");

    // ######################## CANCEL WORKFLOW
    // wait for 5 seconds and then cancel
    try {
      Thread.sleep(5000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    System.out.println("About to cancel PARENT WORKFLOW");
    ParentWorkflow existingwfl = client.newWorkflowStub(ParentWorkflow.class, WORKFLOW_ID);
    existingwfl.cancel();
    System.out.println("cancel RETURNED");
  }
}
