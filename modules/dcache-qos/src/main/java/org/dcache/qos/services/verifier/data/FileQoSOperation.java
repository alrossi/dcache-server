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
import com.google.common.collect.ImmutableSet;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.PnfsId;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import org.dcache.qos.QoSException;
import org.dcache.qos.data.FileQoSUpdate;
import org.dcache.qos.data.QoSAction;
import org.dcache.qos.data.QoSMessageType;
import org.dcache.qos.listeners.QoSAdjustmentListener;
import org.dcache.qos.util.ExceptionMessage;
import org.dcache.qos.vehicles.QoSAdjustmentRequest;
import org.dcache.util.NonReindexableList;

import static org.dcache.qos.data.QoSAction.VOID;

/**
 *  Object stored in the operation map.
 *  <p/>
 *  Since this table may grow very large, two strategies have been adopted to try to reduce
 *  the memory footprint of each instance:
 *  <p/>
 *  <ol>
 *    <li>The state enum is replaced by int values and a conversion method.</li>
 *    <li>Apart from the PnfsId and the exception thrown by adjustment failure,
 *        only int indices referencing the {@link PoolInfoMap} are stored.</li>
 *  </ol>
 *  <p/>
 *  The latter choice is to minimize variable allocation to 4-byte primitives
 *  rather than 8-byte object references. This will have some impact on
 *  performance, but the trade-off is suggested by a decision to use only
 *  a simple persistence model for rollback/recovery purposes and otherwise
 *  rely on values stored in memory.
 *  <p/>
 *  Only those methods which are utilized by both the consumer thread in the operation
 *  map and by the handler threads have been synchronized.
 *  <p/>
 *  For the purposes of efficiency, we allow purely read-only access (such as through the admin
 *  interface or the checkpointing operation) to be unsynchronized.
 */
public final class FileQoSOperation {
    /*
     *  Stored state. Instead of using enum, to leave less of a memory footprint.
     *  The map storing operation markers is expected to be very large.
     *  Order is significant.
     */
    public static final int READY           = 0; // READY TO RUN VERIFICATION AGAIN
    public static final int RUNNING         = 1; // VERIFICATION SUBMITTED TO THE EXECUTOR
    public static final int DONE            = 2; // CURRENT ADJUSTMENT COMPLETED WITHOUT ERROR
    public static final int CANCELED        = 3; // CURRENT ADJUSTMENT WAS TERMINATED BY USER
    public static final int FAILED          = 4; // CURRENT ADJUSTMENT FAILED WITH EXCEPTION
    public static final int ABORTED         = 5; // CANNOT DO ANYTHING FURTHER
    public static final int UNINITIALIZED   = 6; // CONSTRUCTED BUT NOT YET CONFIGURED
    public static final int SUSPENDED       = 7; // TEMPORARILY UNABLE TO RUN, WILL BE RESET TO READY

    private static final String TO_STRING = "%s (%s)(%s %s)(parent %s, retried %s)";
    private static final String TO_HISTORY_STRING = "%s (%s)(%s %s)(parent %s, retried %s) %s";

    /*
     *  Hidden marker for null, used with int->Integer autoboxing.
     */
    private static final int NIL = -19;

    private final PnfsId      pnfsId;
    private final long        size;

    private long              lastUpdate;
    private int               messageType;
    private int               poolGroup;
    private int               storageUnit;
    private int               parent;
    private int               source;
    private int               target;
    private int               state;
    private int               retried;
    private int               previousAction;
    private int               action;

    /**
     *  Estimated number of adjustments left
     */
    private int               needed;

    private Collection<Integer> tried;
    private CacheException      exception;

    public FileQoSOperation(PnfsId pnfsId,
                            QoSMessageType type,
                            Integer pgroup,
                            Integer sunit,
                            long size) {
        this.pnfsId = pnfsId;
        messageType = type.ordinal();
        poolGroup = setNilForNull(pgroup);
        storageUnit = setNilForNull(sunit);
        this.size = size;
        state = UNINITIALIZED;
        needed = 0;
        retried = 0;
        parent = NIL;
        source = NIL;
        target = NIL;
        previousAction = VOID.ordinal();
        action = VOID.ordinal();
        lastUpdate = System.currentTimeMillis();
    }

    public CacheException getException() {
        return exception;
    }

    public long getLastUpdate() {
        return lastUpdate;
    }

    public int getMessageType() {
        return messageType;
    }

    public Integer getParent() {
        return getNullForNil(parent);
    }

    public String getPrincipalPool(PoolInfoMap map) {
        if (parent != NIL) {
            return map.getPool(parent);
        }

        if (source != NIL) {
            return map.getPool(source);
        }

        if (target != NIL) {
            return map.getPool(target);
        }

        return "UNDEFINED";
    }

    public PnfsId getPnfsId() {
        return pnfsId;
    }

    public Integer getPoolGroup() {
        return getNullForNil(poolGroup);
    }

    public long getSize() { return size; }

    public Integer getSource() {
        return getNullForNil(source);
    }

    public synchronized int getState() {
        return state;
    }

    public synchronized String getStateName() {
        switch (state) {
            case READY:
                return "READY";
            case RUNNING:
                return "RUNNING";
            case DONE:
                return "DONE";
            case CANCELED:
                return "CANCELED";
            case FAILED:
                return "FAILED";
            case ABORTED:
                return "ABORTED";
            case UNINITIALIZED:
                return "UNINITIALIZED";
            case SUSPENDED:
                return "SUSPENDED";
        }

        throw new IllegalArgumentException("No such state: " + state);
    }

    public Integer getStorageUnit() {
        return getNullForNil(storageUnit);
    }

    public Integer getTarget() {
        return getNullForNil(target);
    }

    public Set<Integer> getTried() {
        if (tried == null) {
            return Collections.EMPTY_SET;
        }
        return ImmutableSet.copyOf(tried);
    }

    public boolean isBackground() {
        return parent != NIL;
    }

    @VisibleForTesting
    public void setLastUpdate(Long lastUpdate) {
        this.lastUpdate = lastUpdate;
    }

    public void setSource(Integer source) {
        this.source = setNilForNull(source);
    }

    public void setTarget(Integer target) {
        this.target = setNilForNull(target);
    }

    public String toString() {
        return String.format(TO_STRING,
                             FileQoSUpdate.getFormattedDateFromMillis(lastUpdate),
                             pnfsId, actionName(previousAction),
                             getStateName(), parent == NIL ? "none" : parent, retried);
    }

    public String toHistoryString() {
        return String.format(TO_HISTORY_STRING,
                             FileQoSUpdate.getFormattedDateFromMillis(lastUpdate),
                             pnfsId, actionName(previousAction),
                             getStateName(), parent == NIL ? "none" : parent,
                             retried, exception == null ? "" : new ExceptionMessage(exception));
    }

    public synchronized void abortOperation() {
        state = ABORTED;
        lastUpdate = System.currentTimeMillis();
        source = NIL;
        target = NIL;
    }

    public void addSourceToTriedLocations() {
        if (source == NIL) {
            return;
        }

        if (tried == null) {
            tried = new HashSet<>();
        }

        tried.add(source);
    }

    public void addTargetToTriedLocations() {
        if (target == NIL) {
            return;
        }

        if (tried == null) {
            tried = new HashSet<>();
        }

        tried.add(target);
    }

    public synchronized boolean cancel(boolean forceRemoval) {
        if (isTerminal()) {
            return false;
        }

        state = CANCELED;

        if (forceRemoval) {
            action = VOID.ordinal();
        }

        lastUpdate = System.currentTimeMillis();
        retried = 0;

        return true;
    }

    public synchronized QoSAction getAction() {
        return QoSAction.values()[action];
    }

    public int getRetried() {
        return retried;
    }

    public int getNeededAdjustments() { return needed; }

    public void incrementRetried() {
        ++retried;
    }

    public synchronized boolean isInTerminalState() {
        return isTerminal();
    }

    /**
     *  Synchronized on the operation instance, so it can return without
     *  submitting request if the operation has in the interim been cancelled.
     *  Also sets the action under lock.
     */
    public synchronized void requestAdjustment(QoSAdjustmentRequest request,
                                               QoSAdjustmentListener adjustmentListener)
            throws QoSException {
        if (isTerminal()) {
            return;
        }

        setAction(request.getAction());
        adjustmentListener.fileQoSAdjustmentRequested(request);
    }

    public synchronized void resetOperation() {
        state = READY;
        exception = null;
        lastUpdate = System.currentTimeMillis();
    }

    public void resetSourceAndTarget() {
        retried = 0;
        source = NIL;
        target = NIL;
    }

    public void setNeeded(int needed) {
        /*
         *  Once needed has been determined to be greater than 1,
         *  each successive verification will decrement this count.
         *  The original count needs to be preserved, however,
         *  so that the task does not inappropriately get moved
         *  to the head of the queue.
         */
        this.needed = Math.max(this.needed, needed);
    }

    public void setParentOrSource(Integer pool, boolean isParent) {
        if (isParent) {
            parent = setNilForNull(pool);
        } else {
            source = setNilForNull(pool);
        }
    }

    public synchronized void setState(int state) {
        this.state = state;
    }

    public synchronized void setState(String state) {
        switch (state) {
            case "READY":       this.state = READY;     break;
            case "RUNNING":     this.state = RUNNING;   break;
            case "SUSPENDED":   this.state = SUSPENDED; break;
            case "DONE":        this.state = DONE;      break;
            case "CANCELED":    this.state = CANCELED;  break;
            case "FAILED":      this.state = FAILED;    break;
            case "ABORTED":     this.state = ABORTED;   break;
            case "UNINITIALIZED":
                throw new IllegalArgumentException("Cannot set "
                                + "operation to UNINITIALIZED.");
            default:
                throw new IllegalArgumentException("No such state: " + state);
        }
    }

    /**
     *  When another operation for this file/pnfsid is to be
     *  queued, we overwrite the storage unit field on this one, in case
     *  there is a storage unit scan being requested.
     */
    public synchronized void updateOperation(FileQoSOperation operation) {
        if (operation.storageUnit != NIL) {
            storageUnit = operation.storageUnit;
        }
    }

    public synchronized boolean updateOperation(CacheException error) {
        if (isTerminal()) {
            return false;
        }

        if (error != null) {
            exception = error;
            state = FAILED;
        } else {
            state = DONE;
            retried = 0;
        }

        lastUpdate = System.currentTimeMillis();
        previousAction = action;
        return true;
    }

    public synchronized void voidOperation() {
        if (!isTerminal()) {
            state = DONE;
        }
        retried = 0;
        source = NIL;
        target = NIL;
        tried = null;
        lastUpdate = System.currentTimeMillis();
        previousAction = action;
        action = VOID.ordinal();
    }

    /*
     * Called under synchronization except during testing.
     */
    @VisibleForTesting
    void setAction(QoSAction action) {
        previousAction = this.action;
        this.action = action.ordinal();
    }

    private Integer getNullForNil(int value) {
        return value == NIL ? null : value;
    }

    private boolean isTerminal() {
        switch (state) {
            case DONE:
            case CANCELED:
            case FAILED:
            case ABORTED:
                return true;
            default:
                return false;
        }
    }

    private synchronized String actionName(int action) {
        return action == NIL ? "NOOP" : QoSAction.values()[action].toString();
    }

    private int setNilForNull(Integer value) {
        return value == null ? NIL : value == NonReindexableList.MISSING_INDEX ? NIL : value;
    }
}
