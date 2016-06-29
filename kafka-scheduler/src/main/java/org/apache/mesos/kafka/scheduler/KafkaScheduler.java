package org.apache.mesos.kafka.scheduler;

import java.util.*;

import io.dropwizard.setup.Environment;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.BoundedExponentialBackoffRetry;
import org.apache.mesos.Protos.*;
import org.apache.mesos.Scheduler;
import org.apache.mesos.SchedulerDriver;

import org.apache.mesos.config.ConfigStoreException;
import org.apache.mesos.kafka.config.ConfigStateUpdater;
import org.apache.mesos.kafka.config.ConfigStateValidator.ValidationError;
import org.apache.mesos.kafka.config.ConfigStateValidator.ValidationException;
import org.apache.mesos.kafka.config.KafkaConfigState;
import org.apache.mesos.kafka.config.KafkaSchedulerConfiguration;
import org.apache.mesos.kafka.offer.KafkaOfferRequirementProvider;
import org.apache.mesos.kafka.offer.OfferUtils;
import org.apache.mesos.kafka.offer.PersistentOfferRequirementProvider;
import org.apache.mesos.kafka.offer.PersistentOperationRecorder;
import org.apache.mesos.kafka.plan.KafkaUpdatePhase;
import org.apache.mesos.kafka.state.KafkaStateService;

import org.apache.mesos.offer.*;

import org.apache.mesos.reconciliation.DefaultReconciler;
import org.apache.mesos.reconciliation.Reconciler;
import org.apache.mesos.scheduler.SchedulerDriverFactory;
import org.apache.mesos.scheduler.plan.*;
import org.apache.mesos.scheduler.registry.RegistryStorageDriver;
import org.apache.mesos.scheduler.registry.Task;
import org.apache.mesos.scheduler.registry.TaskRegistry;
import org.apache.mesos.scheduler.registry.ZKRegistryStorageDriver;
import org.apache.mesos.scheduler.txnplan.*;
import org.apache.mesos.scheduler.txnplan.ops.CreateTaskOp;

/**
 * Kafka Framework Scheduler.
 */
public class KafkaScheduler extends Observable implements Scheduler, Runnable {
  private static final Log log = LogFactory.getLog(KafkaScheduler.class);

  private static final int TWO_WEEK_SEC = 2 * 7 * 24 * 60 * 60;

  private final KafkaConfigState configState;
  private final KafkaSchedulerConfiguration envConfig;
  private final KafkaStateService kafkaState;

  private final DefaultStageScheduler stageScheduler;
  private final KafkaRepairScheduler repairScheduler;
  private final TaskRegistry registry;
  private final PlanExecutor planExecutor;

  private final OfferAccepter offerAccepter;
  private final Reconciler reconciler;
  private final StageManager stageManager;
  private SchedulerDriver driver;
  private static final Integer restartLock = 0;
  private static List<String> tasksToRestart = new ArrayList<String>();
  private static final Integer rescheduleLock = 0;
  private static List<String> tasksToReschedule = new ArrayList<String>();
  private final KafkaSchedulerConfiguration configuration;
  private final KafkaOfferRequirementProvider offerRequirementProvider;
  private final CuratorFramework curator;

  public KafkaScheduler(KafkaSchedulerConfiguration configuration, Environment environment) throws ConfigStoreException {
    ConfigStateUpdater configStateUpdater = new ConfigStateUpdater(configuration);
    List<String> stageErrors = new ArrayList<>();
    KafkaSchedulerConfiguration targetConfigToUse;

    try {
      targetConfigToUse = configStateUpdater.getTargetConfig();
    } catch (ValidationException e) {
      // New target config failed to validate and was not used. Fall back to previous target config.
      log.error("Got " + e.getValidationErrors().size() + " errors from new config. Falling back to last valid config.");
      targetConfigToUse = configStateUpdater.getConfigState().getTargetConfig();
      for (ValidationError err : e.getValidationErrors()) {
        stageErrors.add(err.toString());
      }
    }

    envConfig = targetConfigToUse;
    reconciler = new DefaultReconciler();

    this.configuration = configuration;
    configState = configStateUpdater.getConfigState();
    kafkaState = configStateUpdater.getKafkaState();
    addObserver(kafkaState);

    offerAccepter =
      new OfferAccepter(Arrays.asList(new PersistentOperationRecorder(kafkaState)));

    offerRequirementProvider =
      new PersistentOfferRequirementProvider(kafkaState, configState);

    List<Phase> phases = Arrays.asList(
        ReconciliationPhase.create(reconciler, kafkaState),
        new KafkaUpdatePhase(
            configState.getTargetName().toString(),
            envConfig,
            kafkaState,
            offerRequirementProvider));
    // If config validation had errors, expose them via the Stage.
    Stage stage = stageErrors.isEmpty()
        ? DefaultStage.fromList(phases)
        : DefaultStage.withErrors(phases, stageErrors);

    stageManager = new DefaultStageManager(stage, getPhaseStrategyFactory(envConfig));
    addObserver(stageManager);

    stageScheduler = new DefaultStageScheduler(offerAccepter);
    repairScheduler = new KafkaRepairScheduler(
        configState.getTargetName().toString(),
        kafkaState,
        offerRequirementProvider,
        offerAccepter);

    curator = CuratorFrameworkFactory.builder()
            .connectString(configuration.getKafkaConfiguration().getZkAddress())
            .retryPolicy(new BoundedExponentialBackoffRetry(100, 120000, 10))
            .namespace("kafka/txnplan")
            .build();
    curator.start();
    registry = new TaskRegistry(offerAccepter, new ZKRegistryStorageDriver(curator));
    planExecutor = new PlanExecutor(registry,
            new ZKOperationDriverFactory(curator),
            new ZKPlanStorageDriver(curator));

    new Thread(new Runnable() {
      @Override
      public void run() {
        Thread.currentThread().setName("repair scheduler");
        while (true) {
          try {
            Thread.sleep(1000);
            Collection<Task> tasks = registry.getAllTasks();
            for (Task task : tasks) {
              if (task.getLatestTaskStatus() == null) {
                continue;
              }
              if (TaskUtils.isTerminated(task.getLatestTaskStatus())) {
                log.warn("Detected failed task, replacing");
                registry.replaceTask(task.getName(),
                        offerRequirementProvider.getReplacementOfferRequirement(task.getTaskInfo()));
              }
            }
          } catch (Throwable t) {
            log.error("Error happened during repair scheduler cycle", t);
          }
        }
      }
    }).start();
  }

  private static PhaseStrategyFactory getPhaseStrategyFactory(KafkaSchedulerConfiguration config) {
    String strategy = config.getServiceConfiguration().getPhaseStrategy();

    switch (strategy) {
      case "INSTALL":
        return new DefaultStrategyFactory();
      case "STAGE":
        return new StageStrategyFactory();
      default:
        log.warn("Unknown strategy: " + strategy);
        return new StageStrategyFactory();
    }
  }

  public static void restartTasks(List<String> taskIds) {
    synchronized (restartLock) {
      tasksToRestart.addAll(taskIds);
    }
  }

  public static void rescheduleTasks(List<String> taskIds) {
    synchronized (rescheduleLock) {
      tasksToReschedule.addAll(taskIds);
    }
  }

  @Override
  public void disconnected(SchedulerDriver driver) {
    log.info("Scheduler driver disconnected");
  }

  @Override
  public void error(SchedulerDriver driver, String message) {
    log.error("Scheduler driver error: " + message);
    System.exit(2);
  }

  @Override
  public void executorLost(SchedulerDriver driver, ExecutorID executorID, SlaveID slaveID, int status) {
    log.info("Executor lost: executorId: " + executorID.getValue()
        + " slaveId: " + slaveID.getValue() + " status: " + status);
  }

  @Override
  public void frameworkMessage(SchedulerDriver driver, ExecutorID executorID, SlaveID slaveID,
      byte[] data) {
    log.info("Framework message: executorId: " + executorID.getValue() + " slaveId: "
        + slaveID.getValue() + " data: '" + Arrays.toString(data) + "'");
  }

  @Override
  public void offerRescinded(SchedulerDriver driver, OfferID offerId) {
    log.info("Offer rescinded: offerId: " + offerId.getValue());
  }

  @Override
  public void registered(SchedulerDriver driver, FrameworkID frameworkId, MasterInfo masterInfo) {
    log.info("Registered framework with frameworkId: " + frameworkId.getValue());
    kafkaState.setFrameworkId(frameworkId);
    new Thread(new Runnable() {
      @Override
      public void run() {
        log.info("Starting reconciliation");
        registry.reconcile();
        log.info("Reconciliation finished, launching plan");
        Plan launchPlan = new Plan();
        List<Step> newBrokers = new ArrayList<>();
        Collection<Task> existingTasks = registry.getAllTasks();
        for (int i = 0; i < configuration.getServiceConfiguration().getCount(); i++) {
          String name = OfferUtils.idToName(i);
          if (existingTasks.stream().anyMatch(t -> t.getName().equals(name))) {
            log.info("Already have " + name + " as a task, not launching a new one");
            continue;
          }
          OfferRequirement req = null;
          try {
            req = offerRequirementProvider.getNewOfferRequirement(configState.getTargetName().toString(), i);
          } catch (InvalidRequirementException e) {
            e.printStackTrace();
          } catch (ConfigStoreException e) {
            e.printStackTrace();
          }
          newBrokers.add(launchPlan.step(CreateTaskOp.make("broker-" + i, req)));
        }
        planExecutor.submitPlan(launchPlan);
        log.warn("submitted plan " + launchPlan.getUuid());
      }
    }).start();
  }

  @Override
  public void reregistered(SchedulerDriver driver, MasterInfo masterInfo) {
    log.info("Reregistered framework.");
    registry.reconcileAsync();
  }

  @Override
  public void statusUpdate(SchedulerDriver driver, TaskStatus status) {
    /*log.info(String.format(
        "Received status update for taskId=%s state=%s message='%s'",
        status.getTaskId().getValue(),
        status.getState().toString(),
        status.getMessage()));
        */

    registry.handleStatusUpdate(driver, status);
  }

  @Override
  public void resourceOffers(SchedulerDriver driver, List<Offer> offers) {
    registry.handleOffers(driver, offers);
  }

  private ResourceCleanerScheduler getCleanerScheduler() {
    try {
      ResourceCleaner cleaner = new ResourceCleaner(kafkaState.getExpectedResources());
      return new ResourceCleanerScheduler(cleaner, offerAccepter);
    } catch (Exception ex) {
      log.error("Failed to construct ResourceCleaner with exception:", ex);
      return null;
    }
  }

  @Override
  public void slaveLost(SchedulerDriver driver, SlaveID slaveId) {
    log.info("Slave lost slaveId: " + slaveId.getValue());
  }

  @Override
  public void run() {
    Thread.currentThread().setName("KafkaScheduler");
    Thread.currentThread().setUncaughtExceptionHandler(getUncaughtExceptionHandler());

    String zkPath = "zk://" + envConfig.getKafkaConfiguration().getZkAddress() + "/mesos";
    FrameworkInfo fwkInfo = getFrameworkInfo();
    log.info("Registering framework with: " + fwkInfo);
    registerFramework(this, fwkInfo, zkPath);
  }

  private FrameworkInfo getFrameworkInfo() {
    FrameworkInfo.Builder fwkInfoBuilder = FrameworkInfo.newBuilder()
      .setName(envConfig.getServiceConfiguration().getName())
      .setFailoverTimeout(TWO_WEEK_SEC)
      .setUser(envConfig.getServiceConfiguration().getUser())
      .setRole(envConfig.getServiceConfiguration().getRole())
      .setPrincipal(envConfig.getServiceConfiguration().getPrincipal())
      .setCheckpoint(true);

    FrameworkID fwkId = kafkaState.getFrameworkId();
    if (fwkId != null) {
      fwkInfoBuilder.setId(fwkId);
    }

    return fwkInfoBuilder.build();
  }


  private void declineOffer(SchedulerDriver driver, Offer offer) {
    OfferID offerId = offer.getId();
    log.info(String.format("Scheduler declining offer: %s", offerId));
    driver.declineOffer(offerId);
  }

  private void registerFramework(KafkaScheduler sched, FrameworkInfo frameworkInfo, String masterUri) {
    log.info("Registering without authentication");
    driver = new SchedulerDriverFactory().create(sched, frameworkInfo, masterUri);
    driver.run();
  }

  // TODO this should move to the commons to solve a common serious error in java development
  private Thread.UncaughtExceptionHandler getUncaughtExceptionHandler() {

    return new Thread.UncaughtExceptionHandler() {
      @Override
      public void uncaughtException(Thread t, Throwable e) {
        final String msg = "Scheduler exiting due to uncaught exception";
        log.error(msg, e);
        log.fatal(msg, e);
        System.exit(2);
      }
    };
  }

  public KafkaConfigState getConfigState() {
    return configState;
  }

  public KafkaStateService getKafkaState() {
    return kafkaState;
  }

  public StageManager getStageManager() {
    return stageManager;
  }
}
