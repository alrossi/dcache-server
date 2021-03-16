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
package org.dcache.qos.services.verifier.data;

import com.google.common.annotations.VisibleForTesting;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.PnfsId;
import dmg.cells.nucleus.CellInfoProvider;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import org.dcache.qos.data.FileQoSUpdate;
import org.dcache.qos.data.QoSMessageType;
import org.dcache.qos.services.verifier.handlers.CheckpointHandler;
import org.dcache.qos.services.verifier.handlers.FileQoSOperationHandler;
import org.dcache.qos.services.verifier.util.QoSVerificationTask;
import org.dcache.qos.services.verifier.util.QoSVerifierCounters;
import org.dcache.qos.util.CacheExceptionUtils;
import org.dcache.qos.util.CacheExceptionUtils.FailureType;
import org.dcache.qos.util.QoSHistory;
import org.dcache.util.RunnableModule;

import static org.dcache.qos.data.QoSAction.VOID;
import static org.dcache.qos.data.QoSMessageType.CHECK_CUSTODIAL_ONLINE;
import static org.dcache.qos.data.QoSMessageType.POOL_STATUS_DOWN;
import static org.dcache.qos.data.QoSMessageType.POOL_STATUS_UP;
import static org.dcache.qos.services.verifier.data.FileQoSOperation.ABORTED;
import static org.dcache.qos.services.verifier.data.FileQoSOperation.CANCELED;
import static org.dcache.qos.services.verifier.data.FileQoSOperation.DONE;
import static org.dcache.qos.services.verifier.data.FileQoSOperation.FAILED;
import static org.dcache.qos.services.verifier.data.FileQoSOperation.RUNNING;
import static org.dcache.qos.services.verifier.data.FileQoSOperation.SUSPENDED;
import static org.dcache.qos.services.verifier.data.PoolInfoMap.ONLINE_CUSTODIAL_SCAN_INDEX;

/**
 *  The main locus of operations for verification.</p>
 *  <p/>
 *  Tracks all operations on individual files via an instance of {@link FileQoSOperation},
 *  whose lifecycle is initiated by the the arrival of a message, and which terminates when
 *  all work on the associated pnfsid has completed or has been aborted/cancelled.
 *  <p/>
 *  When more than one event references a pnfsid which has a current entry in the map,
 *  only the current entry's storage unit is updated. This is because when the current
 *  adjustment finishes, another verification is always done.  When the file's requirements
 *  have been satisfied, the operation is voided to enable removal from the map.
 *  <p/>
 *  The runnable logic entails scanning the queues in order to post-process terminated
 *  adjustments, and to launch new adjustment requests for eligible operations if there are
 *  slots available.  The number of slots for running adjustments should not exceed the
 *  number of maxRunning tasks chosen for the adjustment service.
 *  <p/>
 *  Fairness is defined here as the availability of a second replica for a given file.
 *  This means that operations are processed FIFO, but those requiring more than one
 *  adjustment are requeued after each adjustment.
 *  <p/>
 *  The map distinguishes between three classes of requests, to which it dedicates three
 *  separate queues:  new cache locations and cancellations, pool scan verifications,
 *  and qos modifications. A simple clock algorithm is used to select operations from
 *  them to run.
 *  <p/>
 *  Because of the potential for long-running staging operations to starve out the running
 *  queue, an allocation quota (configurable) is used to determine if more staging requests
 *  can be sent; in the case where the quota has been reached, the running operation is
 *  suspended. The operation then gets placed on a fourth queue and is reset to ready
 *  for retry.  The fourth queue is included in the clock selection algorithm, so that there
 *  is a fair chance that the suspended operations will be reattempted reasonably early when
 *  running slots have been freed.
 *  <p/>
 *  A periodic checkpointer, if on, writes out selected data from each operation entry.
 *  In the case of crash and restart of this domain, the checkpoint file is reloaded into memory.
 *  <p/>
 *  Access to the index map is not synchronized, because it is implemented using a
 *  ConcurrentHashMap.  This is the most efficient solution for allowing multiple inserts
 *  and reads to take place concurrently with any consumer thread removes.  All updating
 *  of operation state or settings in fact is done through an index read, since the necessary
 *  synchronization of those values is handled inside the operation object.  Only the initial
 *  queueing and cancellation requests require additional synchronization.
 *  <p/>
 *  However, since index reads are not blocked, the list and count methods, which filter
 *  against the index (and not the queues), along with the checkpointer, which attempts
 *  to persist live operations, will return or write out a dirty snapshot which is only
 *  an approximation of state, so as not to block consumer processing.
 *  <p/>
 *  Class is not marked final for stubbing/mocking purposes.
 */
public class FileQoSOperationMap extends RunnableModule implements CellInfoProvider {
    private static final String MISSING_ENTRY = "Entry for %s + was removed from map before "
                                    + "completion of outstanding operation.";

    private static final String COUNTS_FORMAT = "    %-24s %15s\n";

    /*
     *  The indices of the READY queues.
     */
    private static final int LOC = 0; // from location or cancellation request
    private static final int SCN = 1; // from pool scan request
    private static final int MOD = 2; // from qos modification request
    private static final int STG = 3; // staging request temporarily rejected because of quota

    final class Checkpointer implements Runnable {
        long     last;
        long     expiry;
        TimeUnit expiryUnit;
        String   path;
        Thread   thread;

        volatile boolean running        = false;
        volatile boolean resetInterrupt = false;
        volatile boolean runInterrupt   = false;

        public void run() {
            running = true;

            while (running) {
                try {
                    synchronized (this) {
                        wait(expiryUnit.toMillis(expiry));
                    }
                } catch (InterruptedException e) {
                    if (resetInterrupt) {
                        LOGGER.trace("Checkpoint reset: expiry {} {}.",
                                     expiry, expiryUnit);
                        resetInterrupt = false;
                        continue;
                    }

                    if (!runInterrupt) {
                        LOGGER.info("Checkpointer wait was interrupted; exiting.");
                        break;
                    }
                    runInterrupt = false;
                }

                if (running) {
                    save();
                }
            }
        }

        /**
         * Writes out data from the operation map to the checkpoint file.
         */
        @VisibleForTesting
        void save() {
            long start = System.currentTimeMillis();
            long count = checkpointHandler.save(path, index.values().iterator());
            last = System.currentTimeMillis();
            counters.recordCheckpoint(last, last - start, count);
        }
    }

    /**
     *  Handles canceled operations.
     *  <p/>
     *  First, it appends the incoming operations to the appropriate queue.
     *  Next, it handles cancellations, and then searches the running queue to see
     *  which adjustments have completed. All terminated operations are passed
     *  to post-processing, which determines whether the operation can be
     *  permanently removed or if needs to be re-queued.
     */
    class TerminalOperationProcessor {
        private Collection<FileQoSOperation> toProcess = new ArrayList<>();

        void processTerminated() {
            appendIncoming();
            gatherCanceled();
            gatherTerminated();

            LOGGER.trace("Found {} terminated operations.", toProcess.size());

            /*
             *  Only non-voided operations will remain in the map after this call.
             *  If the operation is re-queued because of a retriable failure,
             *  it goes to the head of the queue; else it is appended.
             */
            toProcess.stream().forEach(this::postProcess);
            toProcess.clear();
        }

        private void appendIncoming() {
            synchronized (incoming) {
                while (true) {
                    try {
                        enqueue(incoming.remove(), false);
                    } catch (NoSuchElementException e) {
                        break;
                    }
                }
            }
        }

        /**
         *  This is a potentially expensive operation (O[n] in the queue size),
         *  but should be called relatively infrequently.
         */
        private void cancel(Queue<FileQoSOperation> queue,
                            Collection<FileQoSMatcher> filters) {
            for (Iterator<FileQoSOperation> i = queue.iterator(); i.hasNext();) {
                FileQoSOperation operation = i.next();
                for (FileQoSMatcher filter : filters) {
                    if (filter.matches(operation, poolInfoMap)) {
                        i.remove();
                        removeIfStaging(operation.getPnfsId());
                        if (operation.cancel(filter.isForceRemoval())) {
                            toProcess.add(operation);
                        }
                        break;
                    }
                }
            }
        }

        private void gatherCanceled() {
            Collection<FileQoSMatcher> filters = new ArrayList<>();

            synchronized (cancelFilters) {
                filters.addAll(cancelFilters);
                cancelFilters.clear();
            }

            cancel(running, filters);
            for (Deque<FileQoSOperation> queue : queues) {
                cancel(queue, filters);
            }
        }

        private void gatherTerminated() {
            for (Iterator<FileQoSOperation> i = running.iterator(); i.hasNext();) {
                FileQoSOperation operation = i.next();
                switch (operation.getState()) {
                    case SUSPENDED:
                        operation.resetSourceAndTarget();
                        operation.resetOperation();
                        queues[STG].addLast(operation);
                        i.remove();
                        break;
                    case RUNNING:
                        break;
                    default:
                        removeIfStaging(operation.getPnfsId());
                        i.remove();
                        toProcess.add(operation);
                        break;
                }
            }
        }

        /**
         *  Exceptions are analyzed to determine if any more work can be done.
         *  In the case of fatal errors, an alarm is sent.  Operations that have
         *  not failed fatally (aborted) and are not VOID are reset to ready;
         *  otherwise, the operation record will have been removed when this method returns.
         */
        private void postProcess(FileQoSOperation operation) {
            String pool = operation.getPrincipalPool(poolInfoMap);
            Integer sindex = operation.getSource();
            Integer tindex = operation.getTarget();
            String source = sindex == null ? null : poolInfoMap.getPool(sindex);
            String target = tindex == null ? null : poolInfoMap.getPool(tindex);

            boolean retry = false;
            int opState = operation.getState();

            switch (opState) {
                case FAILED:
                    FailureType type =
                        CacheExceptionUtils.getFailureType(operation.getException(),
                            operation.getAction());
                    switch (type) {
                        case NEWSOURCE:
                            operation.addSourceToTriedLocations();
                            operation.resetSourceAndTarget();
                            retry = true;
                            break;
                        case NEWTARGET:
                            operation.addTargetToTriedLocations();
                            operation.resetSourceAndTarget();
                            retry = true;
                            break;
                        case RETRIABLE:
                            operation.incrementRetried();
                            if (operation.getRetried() < maxRetries) {
                                retry = true;
                                break;
                            }

                            /*
                             *  We don't really know here whether the source
                             *  or the target is bad.  The best we can do is
                             *  retry until we have exhausted all the possible
                             *  pool group members.
                             */
                            operation.addTargetToTriedLocations();
                            if (source != null) {
                                operation.addSourceToTriedLocations();
                            }

                            /*
                             *  All readable pools in the pool group.
                             */
                            int groupMembers = poolInfoMap.getMemberPools (operation.getPoolGroup(),
                                false).size();

                            if (groupMembers > operation.getTried().size()) {
                                operation.resetSourceAndTarget();
                                retry = true;
                                break;
                            }

                            /**
                             * fall through; no more possibilities left
                             */
                        case FATAL:
                            operation.addTargetToTriedLocations();
                            if (source != null) {
                                operation.addSourceToTriedLocations();
                            }

                            Set<String> tried = operation.getTried().stream()
                                                                    .map(poolInfoMap::getPool)
                                                                    .collect(Collectors.toSet());
                            operationHandler.operationAborted(operation, pool, tried, maxRetries);
                            operationHandler.handleQoSActionCompleted(operation.getPnfsId(),
                                                                      opState,
                                                                      operation.getAction(),
                                                                      operation.getException());
                            operation.abortOperation();

                            /*
                             * Only fatal errors count as operation failures.
                             */
                            counters.incrementFailed(pool);
                            break;
                        default:
                            operation.abortOperation();
                            LOGGER.error("{}: No such failure type: {}.", operation.getPnfsId(), type);
                    }
                    break;
                case DONE:
                    /*
                     *  Only count DONE, not CANCELED
                     */
                    counters.increment(source, target, operation.getAction());

                    /*
                     *  fall through to notify and reset
                     */
                case CANCELED:
                    operationHandler.handleQoSActionCompleted(operation.getPnfsId(),
                                                              opState,
                                                              operation.getAction(),
                                                              operation.getException());
                    operation.setSource(null);
                    operation.setTarget(null);
                    break;
            }

            /*
             *  The synchronization protects against the situation where an incoming file
             *  update with this id sees the operation is in the index but does not realize
             *  it is about to be removed, thus skipping the creation of a fresh instance
             *  of the operation.
             *
             *  If the operation has been aborted here, or marked VOID during verification,
             *  it is removed.
             */
            boolean abort = operation.getState() == ABORTED;
            synchronized (incoming) {
                if (abort || (!retry && operation.getAction() == VOID)) {
                    remove(operation.getPnfsId(), abort);
                } else {
                    operation.resetOperation();
                    enqueue(operation, retry);
                }
            }
        }
    }

    /**
     *  Selects from waiting operations and submits them for task execution.
     */
    class ReadyOperationProcessor {
        int current = 0;

        void processReady() {
            int available = maxRunning - running.size();
            int empty = 0;

            LOGGER.trace("Available to run: {}", available);

            /*
             *  Simple clock algorithm
             */
            while (available > 0 && empty < queues.length) {
                FileQoSOperation operation = queues[current].poll();
                current = (current + 1) % queues.length;
                if (operation == null) {
                    ++empty;
                    continue;
                }
                submit(operation);
                --available;
            }
        }
    }

    /**
     *  Accessed mostly for retrieval of the operation.  Writes occur on the handler threads
     *  adding operations and removes occur on the consumer thread.
     *  <p/>
     *  Default sharding is probably OK for the present purposes, even with a large maximum
     *  running operations value, so we have not specified the constructor parameters.
     */
    final Map<PnfsId, FileQoSOperation> index = new ConcurrentHashMap<>();

    /**
     *  These queues are entirely used by the consumer thread. Hence
     *      there is no need for synchronization on any of them.
     *  <p/>
     *  The order for election to run is FIFO.  The operation is removed from these
     *  waiting queues and added to running; an attempt at fairness is made by
     *  appending it back to these queues when it successfully terminates, if more work
     *  is to be done, but to restoring it to the head of the queue if there is
     *  a retriable failure.
     */
    final Deque<FileQoSOperation>[] queues = new Deque[] {
        new LinkedList<FileQoSOperation>(), // LOC
        new LinkedList<FileQoSOperation>(), // SCN
        new LinkedList<FileQoSOperation>(), // MOD
        new LinkedList<FileQoSOperation>()  // STG
    };

    /**
     *  Also used exclusively by the consumer thread.
     */
    final Queue<FileQoSOperation> running = new LinkedList<>();

    /**
     *  Queue of incoming/ready operations.  This buffer is shared between the handler
     *  threads and consumer thread, to avoid synchronizing the internal queues.
     *  The incoming operations are appended to the latter during the consumer scan.
     */
    final Queue<FileQoSOperation> incoming = new LinkedList<>();

    /**
     *  For tracking the current number of running operations that are staging.
     *  Used by consumer thread and verifier threads.
     */
    final Set<PnfsId> staging = new HashSet<>();

    /**
     *  List of filters for cancelling operations.  This buffer is shared between
     *  the caller and the consumer thread.
     *  <p/>
     *  Processing of cancellation is done during the consumer scan, as it would have
     *  to be atomic anyway.  This avoids once again any extra locking on the internal queues.
     */
    final Collection<FileQoSMatcher> cancelFilters = new ArrayList<>();

    /**
     *  For recovery.
     */
    @VisibleForTesting
    final Checkpointer checkpointer = new Checkpointer();

    /**
     *  The consumer thread logic is encapsulated in these two processors.
     */
    final TerminalOperationProcessor terminalProcessor = new TerminalOperationProcessor();
    final ReadyOperationProcessor readyProcessor = new ReadyOperationProcessor();

    /**
     *  For reporting operations terminated or canceled while the consumer thread
     *  is doing work outside the wait monitor.
     */
    final AtomicInteger  signalled = new AtomicInteger(0);

    private PoolInfoMap poolInfoMap;
    private CheckpointHandler checkpointHandler;

    /*
     *  A callback to the handler.  Note, this creates a cyclical dependency in the spring context.
     *  The rationale here is that the map controls the terminal logic for verification
     *  operations on a single consumer thread, and thus needs to notify other components (such
     *  as the engine or scanner) of termination.  It makes sense that only the handler
     *  would communicate with the "outside", and that the map should be internal to this service.
     */
    private FileQoSOperationHandler operationHandler;

    /**
     *  This should be set to something <= the maxRunning value for the adjuster map in order
     *  to keep the memory footprint bounded in the latter. Otherwise we risk doubling
     *  the index held here.
     */
    private int maxRunning = 200;

    /**
     *  Max number of running operations whose action can be WAIT_FOR_STAGE.
     *  This is so we do not block throughput of other types of adjustments.
     */
    private double maxStaging = 0.5;

    /**
     *  Meaning for a given source-target pair. When a source or target is changed,
     *  the retry count is reset to 0.
     */
    private int maxRetries  = 1;

    /**
     *  Statistics collection.
     */
    private QoSVerifierCounters counters;
    private QoSHistory history;

    /**
     *  Degenerate call to {@link #cancel(FileQoSMatcher)}.  Used by admin command.
     *
     *  @param pnfsId single operation to cancel.
     *  @param remove true if the entire entry is to be removed from the
     *                map at the next scan.  Otherwise, cancellation pertains
     *                only to the current (running) verification.
     */
    public void cancel(PnfsId pnfsId, boolean remove) {
        FileQoSFilter filter = new FileQoSCancelFilter();
        filter.setPnfsIds(pnfsId.toString());
        filter.setForceRemoval(remove);
        cancel(filter);
    }

    /**
     *  Batch version of cancel.  In this case, the filter will indicate whether
     *  the operation should be cancelled <i>in toto</i> or only the current verification.
     *  The actual scan is done on the consumer thread.
     */
    public void cancel(FileQoSMatcher filter) {
        synchronized (cancelFilters) {
            cancelFilters.add(filter);
        }
        signalAll();
    }

    /**
     *  Cancel operations with this pool as parent, source or target.
     */
    public void cancelFileOpForPool(String pool, boolean onlyParent) {
        FileQoSFilter fileFilter = new FileQoSCancelFilter();
        fileFilter.setParent(pool);
        fileFilter.setForceRemoval(true);
        cancel(fileFilter);
        if (!onlyParent) {
            fileFilter = new FileQoSCancelFilter();
            fileFilter.setSource(pool);
            fileFilter.setForceRemoval(true);
            cancel(fileFilter);
            fileFilter = new FileQoSCancelFilter();
            fileFilter.setTarget(pool);
            fileFilter.setForceRemoval(true);
            cancel(fileFilter);
        }
    }

    /**
     *  Checks to see if the number of staging operations exceeds the quota.
     *  If so, sets the operation state to suspended; else it adds the id to
     *  the stage list.
     *
     *  @return true if added to the staging list, false if suspended.
     */
    public boolean canStage(FileQoSOperation operation) {
        boolean accepted;

        synchronized (staging) {
            if ((double)staging.size() < (maxStaging * (double)maxRunning)) {
                staging.add(operation.getPnfsId());
                accepted = true;
            } else {
                operation.setState(SUSPENDED);
                accepted = false;
            }
        }

        if (!accepted) {
            signalAll();
        }

        return accepted;
    }

    /**
     *  Used by admin command.
     *
     *  @return number of operations matching the filter.
     */
    public long count(FileQoSMatcher filter, StringBuilder builder) {
        long total = 0;
        Iterator<FileQoSOperation> iterator = index.values().iterator();

        Map<String, AtomicLong> summary = builder == null ? null : new HashMap<>();

        while (iterator.hasNext()) {
            FileQoSOperation operation = iterator.next();
            if (filter.matches(operation, poolInfoMap)) {
                ++total;

                if (summary == null) {
                    continue;
                }

                String pool = operation.getPrincipalPool(poolInfoMap);
                AtomicLong count = summary.get(pool);
                if (count == null) {
                    count = new AtomicLong(0);
                    summary.put(pool, count);
                }
                count.incrementAndGet();
            }
        }

        if (summary != null) {
            summary.entrySet()
                   .stream()
                   .forEach((e) -> builder.append(String.format(COUNTS_FORMAT,
                                   e.getKey(), e.getValue())));
        }

        return total;
    }

    public FileQoSOperation getOperation(PnfsId pnfsId) {
        return index.get(pnfsId);
    }

    public void getInfo(PrintWriter pw) {
        pw.println(infoMessage());
    }

    public String infoMessage() {
        StringBuilder info = new StringBuilder();
        info.append(String.format("maximum concurrent operations %s\n"
                + "maximum staging allocation %s\n"
                + "maximum retries on failure %s\n\n",
            maxRunning,
            maxStaging,
            maxRetries));
        info.append(String.format("sweep interval %s %s\n",
            timeout,
            timeoutUnit));
        info.append(String.format("checkpoint interval %s %s\n"
                + "checkpoint file path %s\n",
            checkpointer.expiry,
            checkpointer.expiryUnit,
            checkpointer.path));
        info.append(String.format("task launch delay %s \n\n",
            operationHandler.getLaunchDelay()));

        counters.appendCounts(info);

        info.append(String.format("\nQUEUES\n%-30s %15s\n%-30s %15s\n"
                + "%-30s %15s\n%-30s %15s\n%-30s %15s\n\n",
            "RUNNING", running.size(),
            "READY (LOCATION)", queues[LOC].size(),
            "READY (POOL SCAN)", queues[SCN].size(),
            "READY (MODIFY QOS)", queues[MOD].size(),
            "READY (STAGE)", queues[STG].size()));

        return info.toString();
    }

    @Override
    public void initialize() {
        super.initialize();
        startCheckpointer();
    }

    public boolean isCheckpointingOn() {
        return checkpointer.running;
    }

    /**
     *  Used by admin command.
     */
    public String list(FileQoSMatcher filter, int limit) {
        StringBuilder builder = new StringBuilder();
        Iterator<FileQoSOperation> iterator = index.values().iterator();

        int total = 0;

        while (iterator.hasNext()) {
            FileQoSOperation operation = iterator.next();
            if (filter.matches(operation, poolInfoMap)) {
                ++total;
                builder.append(operation).append("\n");
            }

            if (total >= limit) {
                break;
            }
        }

        if (total == 0) {
            builder.append("NO (MATCHING) OPERATIONS.\n");
        } else {
            builder.append("TOTAL OPERATIONS:\t\t").append(total).append("\n");
        }
        return builder.toString();
    }

    /**
     *  @return true if add returns true.
     */
    public boolean register(FileQoSUpdate data) {
        PnfsId pnfsId = data.getPnfsId();
        Integer gunit = poolInfoMap.getGroupIndex(data.getEffectivePoolGroup());
        Integer sunit = poolInfoMap.getUnitIndex(data.getStorageUnit());
        QoSMessageType type = data.getMessageType();
        FileQoSOperation operation
            = new FileQoSOperation(pnfsId, type, gunit, sunit, data.getSize());
        boolean isParent = type == POOL_STATUS_DOWN ||
                           type == POOL_STATUS_UP ||
                           type == CHECK_CUSTODIAL_ONLINE;
        int poolIndex = type == CHECK_CUSTODIAL_ONLINE ? ONLINE_CUSTODIAL_SCAN_INDEX :
            poolInfoMap.getPoolIndex(data.getPool());
        operation.setParentOrSource(poolIndex, isParent);
        operation.resetOperation();
        LOGGER.debug("register, before add:  operation group is {}.", operation.getPoolGroup());
        return add(pnfsId, operation);
    }

    /**
     *  Reads in the checkpoint file.  Creates one if it does not exist.
     */
    public void reload() {
        checkpointHandler.load(checkpointer.path);
    }

    /**
     *  Used by admin command.
     *
     *  Called after a change to the checkpoint path and/or interval.
     *  Interrupts the thread so that it resumes with the new settings.
     */
    public void reset() {
        if (isCheckpointingOn()) {
            checkpointer.resetInterrupt = true;
            checkpointer.thread.interrupt();
        }
    }

    /**
     *  The consumer thread. When notified or times out, iterates over the queues to
     *  check the state of running operations and to submit waiting operations if there
     *  are open slots.  Removes completed operations.
     *  <p/>
     *  Note that since the scan takes place outside of the monitor, the signals sent
     *  by various update methods will not be caught before the current thread is inside
     *  {@link #await}; for this reason, a counter is used and reset to 0 before each scan.
     *  No wait occurs if the counter is non-zero after the scan.
     */
    public void run() {
        try {
            while (!Thread.interrupted()) {
                LOGGER.trace("Calling scan.");

                signalled.set(0);

                scan();

                if (signalled.get() > 0) {
                    /*
                     *  Give the operations completed during the scan a chance
                     *  to free slots immediately, if possible, by rescanning now.
                     */
                    LOGGER.trace("Scan complete, received {} signals; "
                                    + "rechecking for requeued operations ...",
                                    signalled.get());
                    continue;
                }

                if (Thread.interrupted()) {
                    break;
                }

                LOGGER.trace("Scan complete, waiting ...");
                await();
            }
        } catch (InterruptedException e) {
            LOGGER.trace("Consumer was interrupted.");
        }

        LOGGER.info("Exiting file operation consumer.");
        clear();

        LOGGER.info("File operation queues and index cleared.");
    }

    /**
     *  Used by admin command.
     *  <p/>
     *  If the checkpointing thread is running, interrupts the wait so that it calls
     *  save immediately.  If it is off, it just calls save. (Note:  the latter is
     *  done on the caller thread; this is used mainly for testing.  For the admin
     *  command, this method is disallowed if checkpointing is off.)
     */
    public void runCheckpointNow() {
        if (isCheckpointingOn()) {
            checkpointer.runInterrupt = true;
            checkpointer.thread.interrupt();
        } else {
            checkpointer.save();
        }
    }

    /**
     *   Operation sweep: the main queue update sequence (run by the consumer).
     */
    @VisibleForTesting
    public void scan() {
        long start = System.currentTimeMillis();
        terminalProcessor.processTerminated();
        readyProcessor.processReady();
        long end = System.currentTimeMillis();
        counters.recordSweep(end, end - start);
    }

    public void setCheckpointExpiry(long checkpointExpiry) {
        checkpointer.expiry = checkpointExpiry;
    }

    public void setCheckpointExpiryUnit(TimeUnit checkpointExpiryUnit) {
        checkpointer.expiryUnit = checkpointExpiryUnit;
    }

    public void setCheckpointFilePath(String checkpointFilePath) {
        checkpointer.path = checkpointFilePath;
    }

    public void setCheckpointHandler(CheckpointHandler checkpointHandler) {
        this.checkpointHandler = checkpointHandler;
    }

    public void setCounters(QoSVerifierCounters counters) {
        this.counters = counters;
    }

    public void setHistory(QoSHistory history) {
        this.history = history;
    }

    public void setMaxRetries(int maxRetries) {
        this.maxRetries = maxRetries;
    }

    public void setMaxRunning(int maxRunning) {
        this.maxRunning = maxRunning;
    }

    public void setMaxStaging(double maxStaging) {
        this.maxStaging = maxStaging;
    }

    public void setOperationHandler(FileQoSOperationHandler operationHandler) {
        this.operationHandler = operationHandler;
    }

    public void setPoolInfoMap(PoolInfoMap poolInfoMap) {
        this.poolInfoMap = poolInfoMap;
    }

    @Override
    public void shutdown() {
        stopCheckpointer();
        super.shutdown();
    }

    public long size() {
        return index.size();
    }

    public void startCheckpointer() {
        checkpointer.thread = new Thread(checkpointer, "Checkpointing");
        checkpointer.thread.start();
    }

    public void stopCheckpointer() {
        checkpointer.running = false;
        if (checkpointer.thread != null) {
            checkpointer.thread.interrupt();
        }
    }

    /**
     *  Terminal update.
     */
    public void updateOperation(PnfsId pnfsId, CacheException error) {
        FileQoSOperation operation = index.get(pnfsId);

        if (operation == null) {
            throw new IllegalStateException(String.format(MISSING_ENTRY, pnfsId));
        }

        if (operation.updateOperation(error)) {
            signalAll();
        }
    }

    /**
     *  Terminal update.
     */
    public void voidOperation(PnfsId pnfsId) {
        FileQoSOperation operation = index.get(pnfsId);

        if (operation == null) {
            return;
        }

        operation.voidOperation();
        signalAll();
    }

    @VisibleForTesting
    void submit(FileQoSOperation operation) {
        operation.setState(RUNNING);
        running.add(operation);
        new QoSVerificationTask(operation.getPnfsId(),
                                operation.getRetried(),
                                operationHandler).submit();
    }

    private boolean add(PnfsId pnfsId, FileQoSOperation operation) {
        synchronized (incoming) {
            FileQoSOperation present = index.get(pnfsId);

            if (present != null) {
                present.updateOperation(operation);
                return false;
            }

            index.put(pnfsId, operation);
            incoming.add(operation);
        }

        signalAll();

        return true;
    }

    private synchronized void await() throws InterruptedException {
        wait(timeoutUnit.toMillis(timeout));
    }

    private void clear() {
        running.clear();
        for (Deque d: queues) {
            d.clear();
        }
        cancelFilters.clear();
        synchronized (staging) {
            staging.clear();
        }
        synchronized (incoming) {
            incoming.clear();
        }
        index.clear();
    }

    private void enqueue(FileQoSOperation operation, boolean retry) {
        QoSMessageType type = QoSMessageType.values()[operation.getMessageType()];
        int index = LOC;
        switch (type) {
            case QOS_MODIFIED:
                index = MOD;
                break;
            case POOL_STATUS_DOWN:
            case POOL_STATUS_UP:
            case CHECK_CUSTODIAL_ONLINE:
                index = SCN;
                break;
            default:
                break;
        }

        if (retry || operation.getNeededAdjustments() < 2) { /** fairness algorithm **/
            queues[index].addFirst(operation);
        } else {
            queues[index].addLast(operation);
        }
    }

    private void remove(PnfsId pnfsId, boolean failed) {
        FileQoSOperation operation = index.remove(pnfsId);

        if (operation == null) {
            return;
        }

        /*
         *  If this is a pool-bound operation (scan, viz. the 'parent' of the
         *  operation is defined), we notify the handler, which will
         *  relay to the scanning service when appropriate.
         */
        Integer pIndex = operation.getParent();
        if (pIndex != null) {
            if (pIndex == ONLINE_CUSTODIAL_SCAN_INDEX) {
                operationHandler.updateScanRecord(CHECK_CUSTODIAL_ONLINE.name(), failed);
            } else {
                operationHandler.updateScanRecord(poolInfoMap.getPool(operation.getParent()), failed);
            }
        }

        history.add(operation.getPnfsId(), operation.toHistoryString(), failed);
    }

    private void removeIfStaging(PnfsId pnfsId) {
        synchronized (staging) {
            staging.remove(pnfsId);
        }
    }

    private synchronized void signalAll() {
        signalled.incrementAndGet();
        notifyAll();
    }
}