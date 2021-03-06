package com.mesosphere.dcos.kafka.plan;

import com.mesosphere.dcos.kafka.offer.KafkaOfferRequirementProvider;
import com.mesosphere.dcos.kafka.scheduler.KafkaScheduler;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.mesos.Protos.TaskID;
import org.apache.mesos.Protos.TaskInfo;
import org.apache.mesos.Protos.TaskState;
import org.apache.mesos.Protos.TaskStatus;
import com.mesosphere.dcos.kafka.offer.OfferUtils;
import com.mesosphere.dcos.kafka.state.FrameworkState;
import org.apache.mesos.offer.OfferRequirement;
import org.apache.mesos.offer.TaskRequirement;
import org.apache.mesos.offer.TaskUtils;
import org.apache.mesos.scheduler.plan.Block;
import org.apache.mesos.scheduler.plan.Status;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class KafkaUpdateBlock implements Block {
  private final Log log = LogFactory.getLog(KafkaUpdateBlock.class);

  private final KafkaOfferRequirementProvider offerReqProvider;
  private final String targetConfigName;
  private final FrameworkState state;
  private final int brokerId;
  private final UUID blockUuid;

  private final Object pendingTaskIdsLock = new Object();
  private List<TaskID> pendingTaskIds;
  private Status status = Status.Pending;

  public KafkaUpdateBlock(
    FrameworkState state,
    KafkaOfferRequirementProvider offerReqProvider,
    String targetConfigName,
    int brokerId) {

    this.state = state;
    this.offerReqProvider = offerReqProvider;
    this.targetConfigName = targetConfigName;
    this.brokerId = brokerId;
    this.blockUuid = UUID.randomUUID();

    TaskInfo taskInfo = fetchTaskInfo();
    pendingTaskIds = getUpdateIds(taskInfo);
    initializeStatus(taskInfo);
  }

  @Override
  public boolean isPending() {
    return status == Status.Pending;
  }

  @Override
  public boolean isInProgress() {
    return status == Status.InProgress;
  }

  @Override
  public boolean isComplete() {
    return status == Status.Complete;
  }

  @Override
  public OfferRequirement start() {
    log.info("Starting block: " + getName() + " with status: " + Block.getStatus(this));

    if (!isPending()) {
      log.warn("Block is not pending.  start() should not be called.");
      return null;
    }

    TaskStatus taskStatus = fetchTaskStatus();
    if (taskIsRunningOrStaging(taskStatus)) {
      log.info("Adding task to restart list. Block: " + getName() + " Status: " + taskStatus);
      KafkaScheduler.restartTasks(fetchTaskInfo());
      return null;
    }

    try {
      OfferRequirement offerReq = getOfferRequirement(fetchTaskInfo());
      List<TaskID> newPendingTasks = new ArrayList<TaskID>();
      // in practice there should only be one TaskRequirement, see PersistentOfferRequirementProvider
      for (TaskRequirement taskRequirement : offerReq.getTaskRequirements()) {
        newPendingTasks.add(taskRequirement.getTaskInfo().getTaskId());
      }
      synchronized (pendingTaskIdsLock) {
        pendingTaskIds = newPendingTasks;
      }
      return offerReq;
    } catch (Exception e) {
      log.error("Error getting offerRequirement: ", e);
    }

    return null;
  }

  @Override
  public void updateOfferStatus(boolean accepted) {
    if (accepted) {
      setStatus(Status.InProgress);
    } else {
      setStatus(Status.Pending);
    }
  }

  @Override
  public void restart() {
    setStatus(Status.Pending);
  }

  @Override
  public void forceComplete() {
    try {
      KafkaScheduler.rescheduleTask(fetchTaskInfo());
    } catch (Exception ex) {
      log.error("Failed to force completion of Block: " + getId() + "with exception: ", ex);
      return;
    }
  }

  @Override
  public void update(TaskStatus taskStatus) {
    synchronized (pendingTaskIdsLock) {
      log.info(Block.getStatus(this) + " Block " + getName() + " received TaskStatus. "
          + "Pending tasks: " + pendingTaskIds);

      if (isPending()) {
        log.info("Ignoring TaskStatus (Block " + getName() + " is Pending): " + taskStatus);
        return;
      }

      if (taskStatus.getReason().equals(TaskStatus.Reason.REASON_RECONCILIATION)) {
        log.info("Ignoring TaskStatus (Reason is RECONCILIATION): " + taskStatus);
        return;
      }

      if (!pendingTaskIds.contains(taskStatus.getTaskId())) {
        log.info("Ignoring TaskStatus (TaskId " + taskStatus.getTaskId().getValue() +
            " not found in pending tasks): " + taskStatus);
        return;
      }

      if (taskStatus.getState().equals(TaskState.TASK_RUNNING)) {
        pendingTaskIds.remove(taskStatus.getTaskId());
        log.info(getName() + " has updated pending tasks: " + pendingTaskIds);
      } else if (isInProgress() && TaskUtils.isTerminated(taskStatus)) {
        log.info("Received terminal TaskStatus while " + getName() + " is InProgress: " + taskStatus);
        setStatus(Status.Pending);
        return;
      } else {
        log.warn("TaskStatus with no effect encountered: " + taskStatus);
      }

      if (pendingTaskIds.size() == 0) {
        setStatus(Status.Complete);
      }
    }
  }

  @Override
  public UUID getId() {
    return blockUuid;
  }

  @Override
  public String getMessage() {
    return "Broker-" + getBrokerId() + " is " + Block.getStatus(this);
  }

  @Override
  public String getName() {
    return OfferUtils.idToName(getBrokerId());
  }

  public int getBrokerId() {
    return brokerId;
  }

  List<TaskID> getPendingTaskIds() {
    synchronized (pendingTaskIdsLock) {
      return pendingTaskIds;
    }
  }

  private void initializeStatus(TaskInfo taskInfo) {
    log.info("Setting initial status for: " + getName());

    if (taskInfo != null) {
      String configName = OfferUtils.getConfigName(taskInfo);
      log.info("TargetConfigName: " + targetConfigName + " currentConfigName: " + configName);
      if (configName.equals(targetConfigName)) {
        setStatus(Status.Complete);
      } else {
        setStatus(Status.Pending);
      }
    }

    log.info("Status initialized as " + Block.getStatus(this) + " for block: " + getName());
  }

  private OfferRequirement getOfferRequirement(TaskInfo taskInfo) throws Exception {
    if (taskInfo == null) {
      return offerReqProvider.getNewOfferRequirement(targetConfigName, getBrokerId());
    } else {
      return offerReqProvider.getUpdateOfferRequirement(targetConfigName, taskInfo);
    }
  }

  private void setStatus(Status newStatus) {
    Status oldStatus = status;
    status = newStatus;
    log.info(getName() + ": changed status from: " + oldStatus + " to: " + newStatus);
  }

  private TaskStatus fetchTaskStatus() {
    try {
      return state.getTaskStatusForBroker(getBrokerId());
    } catch (Exception ex) {
      log.error(String.format("Failed to retrieve TaskStatus for broker %d", getBrokerId()), ex);
      return null;
    }
  }

  private TaskInfo fetchTaskInfo() {
    try {
      return state.getTaskInfoForBroker(getBrokerId());
    } catch (Exception ex) {
      log.error(String.format("Failed to retrieve TaskInfo for broker %d", getBrokerId()), ex);
      return null;
    }
  }

  private static List<TaskID> getUpdateIds(TaskInfo taskInfo) {
    List<TaskID> taskIds = new ArrayList<>();

    if (taskInfo != null) {
      taskIds.add(taskInfo.getTaskId());
    }

    return taskIds;
  }

  private static boolean taskIsRunningOrStaging(TaskStatus taskStatus) {
    if (null == taskStatus) {
      return false;
    }
    switch (taskStatus.getState()) {
    case TASK_RUNNING:
    case TASK_STAGING:
      return true;
    default:
      return false;
    }
  }

}
