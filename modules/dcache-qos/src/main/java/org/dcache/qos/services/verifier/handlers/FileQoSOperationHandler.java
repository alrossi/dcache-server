/*
COPYRIGHT STATUS:
Dec 1st 2001, Fermi National Accelerator Laboratory (FNAL) documents and
software are sponsored by the U.S. Department of Energy under Contract No.
DE-AC02-76CH03000. Therefore, the U.S. Government retains a  world-wide
non-exclusive, royalty-free license to publish or reproduce these documents
and software for U.S. Government purposes.  All documents and software
available from this server are protected under the U.S. and Foreign
Copyright Laws, and FNAL reserves all rights.

Distribution of the software available from this server is free of
charge subject to the user following the terms of the Fermitools
Software Legal Information.

Redistribution and/or modification of the software shall be accompanied
by the Fermitools Software Legal Information  (including the copyright
notice).

The user is asked to feed back problems, benefits, and/or suggestions
about the software to the Fermilab Software Providers.

Neither the name of Fermilab, the  URA, nor the names of the contributors
may be used to endorse or promote products derived from this software
without specific prior written permission.

DISCLAIMER OF LIABILITY (BSD):

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
"AS IS" AND ANY EXPRESS OR IMPLIED  WARRANTIES, INCLUDING, BUT NOT
LIMITED TO, THE IMPLIED  WARRANTIES OF MERCHANTABILITY AND FITNESS
FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL FERMILAB,
OR THE URA, OR THE U.S. DEPARTMENT of ENERGY, OR CONTRIBUTORS BE LIABLE
FOR  ANY  DIRECT, INDIRECT,  INCIDENTAL, SPECIAL, EXEMPLARY, OR
CONSEQUENTIAL DAMAGES  (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT
OF SUBSTITUTE  GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR
BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY  OF
LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT  OF THE USE OF THIS
SOFTWARE, EVEN IF ADVISED OF THE  POSSIBILITY OF SUCH DAMAGE.

Liabilities of the Government:

This software is provided by URA, independent from its Prime Contract
with the U.S. Department of Energy. URA is acting independently from
the Government and in its own private capacity and is not acting on
behalf of the U.S. Government, nor as its contractor nor its agent.
Correspondingly, it is understood and agreed that the U.S. Government
has no connection to this software and in no manner whatsoever shall
be liable for nor assume any responsibility or obligation for any claim,
cost, or damages arising out of or resulting from the use of the software
available from this server.

Export Control:

All documents and software available from this server are subject to U.S.
export control laws.  Anyone downloading information from this server is
obligated to secure any necessary Government licenses before exporting
documents or software obtained from this server.
 */
package org.dcache.qos.services.verifier.handlers;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.util.concurrent.RateLimiter;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.PnfsId;
import java.io.Serializable;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.dcache.alarms.AlarmMarkerFactory;
import org.dcache.alarms.PredefinedAlarm;
import org.dcache.qos.QoSException;
import org.dcache.qos.data.FileQoSRequirements;
import org.dcache.qos.data.FileQoSUpdate;
import org.dcache.qos.data.QoSAction;
import org.dcache.qos.data.QoSAdjustmentStatus;
import org.dcache.qos.data.QoSMessageType;
import org.dcache.qos.listeners.QoSActionCompletedListener;
import org.dcache.qos.listeners.QoSAdjustmentListener;
import org.dcache.qos.listeners.QoSPoolScanResponseListener;
import org.dcache.qos.listeners.QoSRequirementsListener;
import org.dcache.qos.services.verifier.data.FileQoSOperation;
import org.dcache.qos.services.verifier.data.FileQoSOperationMap;
import org.dcache.qos.services.verifier.data.PoolInfoMap;
import org.dcache.qos.services.verifier.util.QoSVerifierCounters;
import org.dcache.qos.util.CacheExceptionUtils;
import org.dcache.qos.util.ExceptionMessage;
import org.dcache.qos.vehicles.QoSAdjustmentRequest;
import org.dcache.qos.vehicles.QoSAdjustmentResponse;
import org.dcache.qos.vehicles.QoSScannerVerificationRequest;
import org.dcache.qos.vehicles.QoSVerificationRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.dcache.qos.data.QoSAction.VOID;
import static org.dcache.qos.data.QoSMessageType.CHECK_CUSTODIAL_ONLINE;
import static org.dcache.qos.data.QoSMessageType.VALIDATE_ONLY;
import static org.dcache.qos.services.verifier.handlers.FileQoSStatusVerifier.VERIFY_FAILURE_MESSAGE;

/**
 *  Main handler responsible for processing verification requests and operations.
 *  <p/>
 *  Receives the initial updates+requirements and determines whether to add an operation
 *  for that file to the operation map.
 *  <p/>
 *  Processes operation verification (called by the verification task).
 *  <p/>
 *  Updates operations when responses from the adjuster arrive.
 *  <p/>
 *  Tracks batch verification requests and cancellations arriving from the scanner.
 *  <p/>
 *  Relays notification from the operation map to the engine when an action is aborted or completed.
 *  <p/>
 *  Class is not marked final for stubbing/mocking purposes.
 */
public class FileQoSOperationHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(FileQoSOperationHandler.class);
  private static final Logger ABORTED_LOGGER = LoggerFactory.getLogger("org.dcache.qos-log");

  private static final String ABORT_LOG_MESSAGE
      = "Storage unit {}: aborted operation for {}; "
      + "referring pool {}; pools tried: {}; {}";

  private static final String ABORT_ALARM_MESSAGE
      = "There are files (storage unit {}) for which an operation "
      + "has been aborted; please consult the qos "
      + "logs or 'history errors' for details.";

  private static final String INACCESSIBLE_FILE_MESSAGE
      = "Pool {} is inaccessible but it contains  "
      + "one or more QoS 'disk' files with no currently readable locations. "
      + "Administrator intervention is required; please consult the qos "
      + "logs or 'history errors' for details.";

  private static final String MISSING_LOCATIONS_MESSAGE
      = "{} has no locations in the namespace (file is lost). "
      + "Administrator intervention is required.";

  private static final String INCONSISTENT_LOCATIONS_ALARM
      = "QoS has detected an inconsistency or lag between "
      + "the namespace replica locations and the actual locations on disk.";

  private static final String MISCONFIGURED_POOL_GROUP_ALARM
      = "QoS has detected that pool group {} does not have enough pools to "
      + "meet the requirements of files linked to it.";

  private static final RateLimiter LIMITER = RateLimiter.create(0.001);

  class ScanRecord {
    private final AtomicInteger done = new AtomicInteger(0);
    private final AtomicInteger failures = new AtomicInteger(0);
    private final AtomicInteger arrived = new AtomicInteger(0);

    private boolean cancelled = false;

    void cancel() {
      cancelled = true;
    }

    void updateArrived(int count) {
      arrived.addAndGet(count);
    }

    void updateTerminated(boolean failed) {
      done.incrementAndGet();
      if (failed) {
        failures.incrementAndGet();
      }
    }

    boolean isCancelled() {
      return cancelled;
    }

    boolean isComplete() {
      return cancelled || arrived.get() == done.get();
    }
  }

  class ScanRecordMap {
    private final Map<String, ScanRecord> scanRecords = new HashMap<>();
    private final Multimap<String, Future> futures =  HashMultimap.create();

    synchronized void addFuture(String pool, Future future) {
      futures.put(pool, future);
    }

    synchronized void cancel(String pool) {
      ScanRecord record = scanRecords.get(pool);
      if (record != null) {
        LOGGER.trace("cancelled scan record for {}.", pool);
        record.cancel();
      }
      LOGGER.trace("cancelling futures for {}.", pool);
      futures.get(pool).stream().forEach(f -> f.cancel(true));
      futures.removeAll(pool);
    }

    synchronized ScanRecord updateArrived(String pool, int count) {
      LOGGER.trace("updateArrived, pool {}, count {}.", pool, count);
      ScanRecord record = scanRecords.get(pool);

      if (record == null) {
        record = new ScanRecord();
        scanRecords.put(pool, record);
      }

      if (record.isCancelled()) {
        LOGGER.trace("updateArrived, scan of {} cancelled.", pool);
        return record;
      }

      record.updateArrived(count);

      return record;
    }

    synchronized void updateCompleted(String pool, boolean failed) {
      ScanRecord record = scanRecords.get(pool);
      if (record != null) {
        record.updateTerminated(failed);
      }
    }

    synchronized Optional<ScanRecord> fetchAndRemoveIfCompleted(String pool) {
      ScanRecord record = scanRecords.get(pool);
      if (record == null || !record.isComplete()) {
        return Optional.empty();
      }
      LOGGER.trace("fetchAndRemoveIfCompleted, record is complete {}.", pool);
      scanRecords.remove(pool);
      futures.removeAll(pool);
      return Optional.of(record);
    }
  }

  private final ScanRecordMap scanRecordMap = new ScanRecordMap();

  private FileQoSOperationMap fileOpMap;
  private PoolInfoMap poolInfoMap;
  private FileQoSStatusVerifier statusVerifier;
  private QoSAdjustmentListener adjustmentListener;
  private QoSPoolScanResponseListener scanResponseListener;
  private QoSRequirementsListener requirementsListener;
  private QoSActionCompletedListener actionCompletedListener;

  private ExecutorService updateExecutor;
  private ExecutorService bulkExecutor;
  private ScheduledExecutorService taskExecutor;

  private QoSVerifierCounters counters;

  private long launchDelay = 0L;
  private TimeUnit launchDelayUnit = TimeUnit.SECONDS;

  public long getLaunchDelay() {
    return launchDelay;
  }

  public TimeUnit getLaunchDelayUnit() {
    return launchDelayUnit;
  }

  public ScheduledExecutorService getTaskExecutor() {
    return taskExecutor;
  }

  /**
   *  Callback from the adjustment service.
   */
  public void handleAdjustmentResponse(QoSAdjustmentResponse response) {
    counters.incrementReceived(QoSVerifierCounters.ADJ_RESP_MESSAGE);
    PnfsId pnfsId = response.getPnfsId();
    QoSAdjustmentStatus status = response.getStatus();
    Serializable error = response.getError();
    LOGGER.debug("{}, handleAdjustmentResponse: {}{}.", pnfsId, status,
        error == null ? "" : " " + error);
    updateExecutor.submit(() -> {
      switch (status) {
        case FAILED:
          fileOpMap.updateOperation(pnfsId, CacheExceptionUtils.getCacheExceptionFrom(error));
          break;
        case CANCELLED:
        case COMPLETED:
          fileOpMap.updateOperation(pnfsId, null);
          break;
        default:
      }
    });
  }

  /**
   *  From a pool mode or exclusion change communicated by the scanner.
   *  Just sets new info on map, and can be done on message thread.
   */
  public void handleExcludedStatusChange(String location, boolean excluded) {
    counters.incrementReceived(QoSVerifierCounters.LOC_EXCL_MESSAGE);
    poolInfoMap.updatePoolInfo(location, excluded);
  }

  /**
   *  Callback from scanner.
   */
  public void handleFileOperationsCancelledForPool(String pool) {
    counters.incrementReceived(QoSVerifierCounters.BVRF_CNCL_MESSAGE);
    /*
     *  Cancel any ongoing batch requests.
     */
    scanRecordMap.cancel(pool);
    /*
     *  Do not use the bulk executor as cancellation could be queued behind running
     *  bulk updates that it pertains to.
     */
    updateExecutor.submit(() -> fileOpMap.cancelFileOpForPool(pool, true));
  }

  /**
   *  Incoming cancellation request from a client. Forces removal of operation.
   */
  public void handleFileOperationCancelled(PnfsId pnfsId) {
    counters.incrementReceived(QoSVerifierCounters.VRF_CNCL_MESSAGE);
    updateExecutor.submit(() -> fileOpMap.cancel(pnfsId, true));
  }

  /**
   *  Called in the post-process method of the operation map.
   */
  public void handleQoSActionCompleted(PnfsId pnfsId, int opState, QoSAction type, Serializable error) {
    actionCompletedListener.fileQoSActionCompleted(pnfsId, type, error);

    /*
     *  Need to call out to adjuster if this is a cancellation
     */
    if (opState == FileQoSOperation.CANCELED) {
      try {
        adjustmentListener.fileQoSAdjustmentCancelled(pnfsId);
      } catch (QoSException e) {
        LOGGER.warn("Failed to notify adjustment service to that {} was cancelled; {}.",
            pnfsId, e.getMessage());
      }
    }
  }

  /**
   *  The entry method for a verify operation.
   *  <p/>
   *  If the entry is already in the current map, its storage group info is updated.
   *  <p/>
   *  All requirements of the file that are necessary for qos processing are then fetched if
   *  not present.  Preliminary checks run for disqualifying conditions here include
   *  whether this is a storage unit modification, in which case the task is registered
   *  if the file has the storage unit in question. Otherwise, verification proceeds with a
   *  series of checks for other disqualifying conditions. If the pnfsId does qualify,
   *  an entry is added to the {@link FileQoSOperationMap}.
   */
  public void handleUpdate(FileQoSUpdate data, FileQoSRequirements requirements) {
    LOGGER.debug("handleUpdate {}", data);

    if (requirements == null) {
      try {
        requirements = requirementsListener.fileQoSRequirementsRequested(data);
      } catch (QoSException e) {
        LOGGER.error("Could not get requirements for {}: {}.", data, e.toString());
        handleVerificationNop(data, true);
        return;
      }
    }

    if (requirements == null) {
      /*
       *  Should only happen when a CLEAR CACHE LOCATION finds no locations.
       */
      LOGGER.debug("No requirements for {}.", data);
      handleVerificationNop(data, false);
      return;
    }

    data.setSize(requirements.getAttributes().getSize());

    /*
     *  Determine if action needs to be taken.
     *
     *  Verification can be skipped under a few special conditions, but most of the
     *  time this will be true.
     */
    if (!statusVerifier.isActionable(data, requirements)) {
      handleVerificationNop(data, false);
      return;
    }

    LOGGER.debug("handleUpdate, update to be registered: {}", data);
    if (!fileOpMap.register(data)) {
      LOGGER.debug("handleUpdate, operation already registered for: {}", data.getPnfsId());
      handleVerificationNop(data, false);
    };
  }

  /**
   *  Called by the verification task.  Checks the status of the file and takes appropriate
   *  action.  This includes (a) failing the operation if the error is fatal; (b) voiding
   *  the operation if nothing else needs to be done; (c) sending an adjustment request
   *  to the adjustment listener.
   */
  public void handleVerification(PnfsId pnfsId) {
    FileQoSOperation operation = fileOpMap.getOperation(pnfsId);
    if (operation == null) {
      LOGGER.warn("handleVerification: file operation for {} does not exist; returning.", pnfsId);
      return;
    }

    /*
     *  Check for cancellation.  This is rechecked just before dispatching an adjustment request.
     */
    if (operation.isInTerminalState()) {
      LOGGER.info("handleVerification: file operation for {} already terminated; returning.",
          pnfsId);
      return;
    }

    FileQoSRequirements requirements;
    QoSAction action;
    try {
      FileQoSUpdate data = new FileQoSUpdate(operation.getPnfsId(), null, VALIDATE_ONLY);
      requirements = requirementsListener.fileQoSRequirementsRequested(data);
      action = statusVerifier.verify(requirements, operation);
    } catch (QoSException e) {
      String message = CacheExceptionUtils.getCacheExceptionErrorMessage(VERIFY_FAILURE_MESSAGE,
                                                                         pnfsId,
                                                                         VOID,
                                                                    null, e.getCause());
      /*
       *  FATAL error, should abort operation.
       */
      CacheException exception = new CacheException(CacheException.UNEXPECTED_SYSTEM_EXCEPTION,
                                                    message, e.getCause());
      fileOpMap.updateOperation(pnfsId, exception);
      return;
    } catch (InterruptedException e) {
      LOGGER.debug("file operation for {} was interrupted; returning.", pnfsId);
      return;
    }

    LOGGER.debug("handleVerification for {}, action is {}.", pnfsId, action);

    switch (action) {
      case VOID:
        /**  signals the operation map so that the operation can be removed. **/
        fileOpMap.voidOperation(pnfsId);
        break;
      case NOTIFY_MISSING:
        /**  signals the operation map so that the operation can be removed. **/
        handleNoLocationsForFile(operation);
        break;
      case NOTIFY_INACCESSIBLE:
        /**  signals the operation map so that the operation can be removed. **/
        handleInaccessibleFile(operation);
        break;
      case NOTIFY_OUT_OF_SYNC:
        /**  signals the operation map so that the operation can be removed. **/
        handleNamespaceSyncError(pnfsId);
        break;
      case MISCONFIGURED_POOL_GROUP:
        /**  signals the operation map so that the operation can be removed. **/
        handleMisconfiguredGroupError(operation);
        break;
      case WAIT_FOR_STAGE:
        /**  signals the operation map if operation is suspended. **/
        if (!fileOpMap.canStage(operation)) {
          break;
        }
        /** fall through if available for staging **/
      case CACHE_REPLICA:
      case PERSIST_REPLICA:
      case UNSET_PRECIOUS_REPLICA:
      case COPY_REPLICA:
      case FLUSH:
        /**  updates the operation action and sends out an adjustment notification. **/
        try {
          handleAdjustment(requirements, operation, action);
        } catch (QoSException e) {
          /*
           *  FATAL error, should abort operation.
           */
          CacheException exception = CacheExceptionUtils.getCacheExceptionFrom(e);
          fileOpMap.updateOperation(pnfsId, exception);
        }
    }
  }

  /**
   *  Request originating from the engine in response to a location update or QoS change.
   */
  public void handleVerificationRequest(QoSVerificationRequest request) {
    counters.incrementReceived(QoSVerifierCounters.VRF_REQ_MESSAGE);
    LOGGER.debug("handleVerificationRequest for {}.", request.getUpdate());
    updateExecutor.submit(() -> handleUpdate(request.getUpdate(), request.getRequirements()));
  }

  /**
   *  Request originating from the scanner in response to a pool status change or forced scan.
   */
  public void handleVerificationRequest(QoSScannerVerificationRequest request) {
    counters.incrementReceived(QoSVerifierCounters.BVRF_REQ_MESSAGE);
    LOGGER.debug("handleVerificationRequest for {}, type is {}, group is {}, unit is {}.",
                  request.getPool(), request.getType(), request.getGroup(), request.getStorageUnit());
    QoSMessageType type = request.getType();
    String pool = request.getPool();
    String group = request.getGroup();
    String storageUnit = request.getStorageUnit();
    List<PnfsId> replicas = request.getReplicas();
    boolean forced = request.isForced();
    ScanRecord record = scanRecordMap.updateArrived(type == CHECK_CUSTODIAL_ONLINE ?
                                                    CHECK_CUSTODIAL_ONLINE.name() : pool,
                                                    replicas.size());
    if (!record.isCancelled()) {
      scanRecordMap.addFuture(pool,
          bulkExecutor.submit(() -> updateAll(replicas, pool, type, group, storageUnit, forced)));
    }
  }

  /**
   *  Callback from the operation map when operation fails fatally.
   */
  public void operationAborted(FileQoSOperation operation,
                               String pool,
                               Set<String> triedSources,
                               int maxRetries) {
    PnfsId pnfsId = operation.getPnfsId();
    String storageUnit =  poolInfoMap.getUnit(operation.getStorageUnit());
    int retried = operation.getRetried();
    Exception e = operation.getException();
    if (retried >= maxRetries) {
      e = new Exception(String.format("Maximum number of attempts (%s) has been reached", maxRetries),
                        e);
    }

    Calendar ref = Calendar.getInstance();
    ref.set(Calendar.MINUTE, 0);
    ref.set(Calendar.SECOND, 0);
    ref.set(Calendar.MILLISECOND, 0);

    storageUnit = storageUnit == null ? "*" : storageUnit;

    /*
     *  Alarm notification is keyed to the storage group, so as to avoid
     *  spamming the server or email forwarding. The alarm key changes every hour.
     *  This guarantees that a new alarm is registered each hour.
     *  Send this at warn level, so it is possible to throttle repeated
     *  messages in the domain log.
     */
    LOGGER.warn(AlarmMarkerFactory.getMarker(PredefinedAlarm.FAILED_REPLICATION,
        storageUnit,
        "ABORT_REPLICATION-" + ref.getTimeInMillis()),
        ABORT_ALARM_MESSAGE,
        storageUnit);

    /*
     *  Full info on the file is logged to the ".qos" log.
     */
    ABORTED_LOGGER.error(ABORT_LOG_MESSAGE,
        storageUnit,
        pnfsId,
        pool == null ? "none" : pool,
        triedSources,
        new ExceptionMessage(e));
  }

  public void setActionCompletedListener(QoSActionCompletedListener actionCompletedListener) {
    this.actionCompletedListener = actionCompletedListener;
  }

  public void setAdjustmentListener(QoSAdjustmentListener adjustmentListener) {
    this.adjustmentListener = adjustmentListener;
  }

  public void setBulkExecutor(ExecutorService bulkExecutor) {
    this.bulkExecutor = bulkExecutor;
  }

  public void setCounters(QoSVerifierCounters counters) {
    this.counters = counters;
  }

  public void setFileOpMap(FileQoSOperationMap fileOpMap) {
    this.fileOpMap = fileOpMap;
  }

  public void setLaunchDelay(long launchDelay) {
    this.launchDelay = launchDelay;
  }

  public void setLaunchDelayUnit(TimeUnit launchDelayUnit) {
    this.launchDelayUnit = launchDelayUnit;
  }

  public void setPoolInfoMap(PoolInfoMap poolInfoMap) {
    this.poolInfoMap = poolInfoMap;
  }

  public void setRequirementsListener(QoSRequirementsListener requirementsListener) {
    this.requirementsListener = requirementsListener;
  }

  public void setScanResponseListener(QoSPoolScanResponseListener scanResponseListener) {
    this.scanResponseListener = scanResponseListener;
  }

  public void setStatusVerifier(FileQoSStatusVerifier statusVerifier) {
    this.statusVerifier = statusVerifier;
  }

  public void setTaskExecutor(ScheduledExecutorService taskExecutor) {
    this.taskExecutor = taskExecutor;
  }

  public void setUpdateExecutor(ExecutorService updateExecutor) {
    this.updateExecutor = updateExecutor;
  }

  public void updateScanRecord(String pool, boolean failure) {
    LOGGER.debug("updateScanRecord {}, failure {}.", pool, failure);
    scanRecordMap.updateCompleted(pool, failure);

    /*
     *  While the interface allows for batched updates, the
     *  notification by single increments is less error prone.
     */
    if (failure) {
      scanResponseListener.scanRequestUpdated(pool, 0, 1);
    } else {
      scanResponseListener.scanRequestUpdated(pool, 1, 0);
    }

    Optional<ScanRecord> optional = scanRecordMap.fetchAndRemoveIfCompleted(pool);

    if (optional.isPresent()) {
      ScanRecord record = optional.get();
      int failed = record.failures.get();
      int succeeded = record.done.get() - failed;
      LOGGER.debug("updateScanRecord {}, succeeded {}, failed {}.", pool, succeeded, failed);
    }
  }

  private void handleAdjustment(FileQoSRequirements requirements,
                                FileQoSOperation operation,
                                QoSAction action) throws QoSException {
    QoSAdjustmentRequest request = new QoSAdjustmentRequest();
    request.setAction(action);
    request.setPnfsId(requirements.getPnfsId());
    request.setAttributes(requirements.getAttributes());
    request.setPoolGroup(requirements.getRequiredPoolGroup());

    Integer source = operation.getSource();

    if (source != null) {
      request.setSource(poolInfoMap.getPool(source));
    }

    Integer target = operation.getTarget();
    if (target != null) {
      request.setTarget(poolInfoMap.getPool(target));
      request.setTargetInfo(poolInfoMap.getPoolManagerInfo(target));
    }

    /*
     *  Notify subscribers.  This will normally be the adjustment service.
     *  Source and target have been set on the operation.
     *  Here we also set the action.
     */
    operation.requestAdjustment(request, adjustmentListener);
  }

  private void handleInaccessibleFile(FileQoSOperation operation) {
    Integer pindex = operation.getParent();
    if (pindex == null) {
      pindex = operation.getSource();
    }

    if (pindex != null) {
      String pool = poolInfoMap.getPool(pindex);
      LOGGER.error(AlarmMarkerFactory.getMarker(PredefinedAlarm.INACCESSIBLE_FILE,
          pool),
          INACCESSIBLE_FILE_MESSAGE, pool, pool);
    }

    PnfsId pnfsId = operation.getPnfsId();
    String error = String.format("%s currently has no active locations.", pnfsId);
    CacheException exception
        = CacheExceptionUtils.getCacheException(CacheException.PANIC, VERIFY_FAILURE_MESSAGE,
                                                 pnfsId, VOID, error, null);

    fileOpMap.updateOperation(pnfsId, exception);
  }

  private void handleMisconfiguredGroupError(FileQoSOperation operation) {
    String poolGroup = poolInfoMap.getGroup(operation.getPoolGroup());
    sendPoolGroupMisconfiguredAlarm(poolGroup);
    PnfsId pnfsId = operation.getPnfsId();
    String error
        = String.format("Pool group %s cannot satisfy the requirements for %s.", poolGroup, pnfsId);
    CacheException exception
        = CacheExceptionUtils.getCacheException(CacheException.PANIC, VERIFY_FAILURE_MESSAGE,
                                                 pnfsId, VOID, error, null);
    fileOpMap.updateOperation(pnfsId, exception);
  }

  private void handleNamespaceSyncError(PnfsId pnfsId) {
    sendOutOfSyncAlarm();
    String error
        = String.format("The namespace is not in sync with the pool repositories for %s.", pnfsId);
    CacheException exception
        = CacheExceptionUtils.getCacheException(
        CacheException.PANIC,
        VERIFY_FAILURE_MESSAGE,
        pnfsId, VOID, error, null);
    fileOpMap.updateOperation(pnfsId, exception);
  }

  private void handleNoLocationsForFile(FileQoSOperation operation) {
    PnfsId pnfsId = operation.getPnfsId();
    LOGGER.error(AlarmMarkerFactory.getMarker(PredefinedAlarm.LOST_RESILIENT_FILE,
        pnfsId.toString()),
        MISSING_LOCATIONS_MESSAGE, pnfsId);
    String error = String.format("%s has no locations.", pnfsId);
    CacheException exception
        = CacheExceptionUtils.getCacheException(
        CacheException.PANIC,
        VERIFY_FAILURE_MESSAGE,
        pnfsId, VOID, error, null);
    fileOpMap.updateOperation(pnfsId, exception);
  }

  private void handleVerificationNop(FileQoSUpdate data, boolean failed) {
    switch (data.getMessageType()) {
      case POOL_STATUS_DOWN:
      case POOL_STATUS_UP:
        String pool = data.getMessageType() == CHECK_CUSTODIAL_ONLINE ?
            CHECK_CUSTODIAL_ONLINE.name() : data.getPool();
        LOGGER.debug("handleVerificationNop {}, updating scan record for {}", data.getPnfsId(), pool);
        updateScanRecord(pool, failed);
        break;
      case QOS_MODIFIED:
        actionCompletedListener.fileQoSActionCompleted(data.getPnfsId(), VOID, null);
        break;
      default:
        // nothing to do
    }
  }

  private static void sendPoolGroupMisconfiguredAlarm(String poolGroup) {
    /*
     *  Create a new alarm every hour by keying the alarm to
     *  an hourly timestamp.  Otherwise, the alarm counter will
     *  just be updated for each alarm sent.  The rate limiter
     *  will not alarm more than once every 1000 seconds (once every
     *  15 minutes).
     */
    if (LIMITER.tryAcquire()) {
      LOGGER.warn(AlarmMarkerFactory.getMarker(PredefinedAlarm.RESILIENCE_PGROUP_ISSUE,
                                               Instant.now().truncatedTo(ChronoUnit.HOURS).toString()),
                  MISCONFIGURED_POOL_GROUP_ALARM, poolGroup);
    }
  }

  private static void sendOutOfSyncAlarm() {
    /*
     *  Create a new alarm every hour by keying the alarm to
     *  an hourly timestamp.  Otherwise, the alarm counter will
     *  just be updated for each alarm sent.  The rate limiter
     *  will not alarm more than once every 1000 seconds (once every
     *  15 minutes).
     */
    if (LIMITER.tryAcquire()) {
      LOGGER.warn(AlarmMarkerFactory.getMarker(PredefinedAlarm.RESILIENCE_LOC_SYNC_ISSUE,
          Instant.now().truncatedTo(ChronoUnit.HOURS).toString()),
          INCONSISTENT_LOCATIONS_ALARM);
    }
  }

  private void updateAll(List<PnfsId> replicas,
                         String pool,
                         QoSMessageType type,
                         String group,
                         String storageUnit,
                         boolean forced) {
    replicas.stream().forEach(r ->
        handleUpdate(new FileQoSUpdate(r, pool, type, group, storageUnit, forced),null));
  }
}
