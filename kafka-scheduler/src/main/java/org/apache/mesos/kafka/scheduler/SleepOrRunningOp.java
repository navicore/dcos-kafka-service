package org.apache.mesos.kafka.scheduler;

import org.apache.mesos.Protos;
import org.apache.mesos.scheduler.registry.Task;
import org.apache.mesos.scheduler.registry.TaskRegistry;
import org.apache.mesos.scheduler.txnplan.OperationDriver;
import org.apache.mesos.scheduler.txnplan.ops.SleepOp;

/**
 * Created by dgrnbrg on 7/18/16.
 */
public class SleepOrRunningOp extends SleepOp {
    private String taskName;

    private SleepOrRunningOp() {
        super();
    }

    public SleepOrRunningOp(long durationSeconds, boolean resumable, String taskName) {
        super(durationSeconds, resumable);
        this.taskName = taskName;
    }

    @Override
    public boolean shortCircuit(TaskRegistry registry, OperationDriver driver) {
        Task task = registry.getTask(taskName);
        if (task.hasStatus()) {
            return task.getLatestTaskStatus().getState().equals(Protos.TaskState.TASK_RUNNING);
        } else {
            return false;
        }
    }
}
