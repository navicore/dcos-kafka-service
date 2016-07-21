package org.apache.mesos.kafka.scheduler;

import org.apache.mesos.Protos;
import org.apache.mesos.offer.OfferRequirement;
import org.apache.mesos.scheduler.registry.Task;
import org.apache.mesos.scheduler.registry.TaskRegistry;
import org.apache.mesos.scheduler.txnplan.Operation;
import org.apache.mesos.scheduler.txnplan.OperationDriver;

import java.util.Collection;
import java.util.Collections;

import static org.apache.mesos.Protos.*;

/**
 * Created by dgrnbrg on 7/18/16.
 */
public class HardResetBrokerOp implements Operation {
    private String taskName;
    private OfferRequirement offerRequirement;

    private HardResetBrokerOp() {

    }

    public HardResetBrokerOp(String taskName, OfferRequirement offerRequirement) {
        this.offerRequirement = offerRequirement;
        this.taskName = taskName;
    }

    @Override
    public void doAction(TaskRegistry taskRegistry, OperationDriver operationDriver) throws Exception {
        Task oldTask = taskRegistry.getTask(taskName);
        if (oldTask.hasStatus() && oldTask.getLatestTaskStatus().getState().equals(TaskState.TASK_RUNNING)) {
            operationDriver.info("No need to hard reset, task is running fine");
            return;
        }
        if (oldTask.hasStatus()) {
            TaskID id = oldTask.getLatestTaskStatus().getTaskId();
            operationDriver.info("Clean up kill of " + taskName + " which had id " + id);
            taskRegistry.getSchedulerDriver().killTask(id);
        }
        TaskID newID = taskRegistry.replaceTask(taskName, offerRequirement);
        operationDriver.save(newID);
        operationDriver.info("Waiting for the task " + taskName + " with id " + newID + " to start");
        taskRegistry.getTask(taskName).waitForStatus(s -> s.getState().equals(TaskState.TASK_RUNNING));
        operationDriver.info("Task " + taskName + " restarted successfully");
    }

    @Override
    public void unravel(TaskRegistry taskRegistry, OperationDriver operationDriver) throws Exception {
        //TODO: modify the code from CreateOp if necessary
    }

    @Override
    public Collection<String> lockedTasks() {
        return Collections.singletonList(taskName);
    }
}
