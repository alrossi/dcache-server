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
package org.dcache.qos.services.scanner.handlers;

import com.google.common.collect.ImmutableSet;
import diskCacheV111.pools.PoolV2Mode;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.Message;
import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.dcache.qos.TestBase;
import org.dcache.qos.TestMessageProcessor;
import org.dcache.qos.TestSynchronousExecutor.Mode;
import org.dcache.qos.data.PoolQoSStatus;
import org.dcache.qos.services.adjuster.util.QoSAdjusterTask;
import org.dcache.qos.services.verifier.data.FileQoSFilter;
import org.dcache.vehicles.qos.ReplicaStatusMessage;
import org.junit.Before;
import org.junit.Test;

import static org.dcache.qos.TestSynchronousExecutor.Mode.RUN;
import static org.junit.Assert.assertEquals;

public class PoolOperationHandlerTest extends TestBase implements TestMessageProcessor {
    String pool;
    List<PnfsId> taskPnfsids;

    @Override
    public void processMessage(Message message) {
        if (message instanceof ReplicaStatusMessage) {
            ReplicaStatusMessage reply = (ReplicaStatusMessage) message;
            reply.setExists(true);
            reply.setBroken(false);
            reply.setReadable(true);
            reply.setRemovable(true);
            reply.setSystemSticky(true);
        }
    }

    @Before
    public void setUp() throws CacheException, InterruptedException {
        setUpBase();
        setPoolMessageProcessor(this);
        setShortExecutionMode(Mode.NOP);
        setLongExecutionMode(RUN);
        setScheduledExecutionMode(RUN);
        createChangeHandlers();
        createFileOperationHandler();
        createFileOperationMap();
        createPoolOperationHandler();
        createPoolOperationMap();
        createAdjusterTaskHandler();
        createAdjusterTaskMap();
        createLocationSelector();
        createRequirementsProvider();
        createLocalClients();
        createLocationSelector();
        initializeCounters();
        wireFileOperationHandler();
        wireFileOperationMap();
        wirePoolOperationHandler();
        wirePoolOperationMap();
        wireAdjusterTaskHandler();
        wireAdjusterTaskMap();
        wireChangeHandlers();
        wireLocalClients();
        wireLocationSelector();
        fileOperationMap.initialize(() -> {});
        fileOperationMap.reload();
        poolOperationMap.setRescanWindow(Integer.MAX_VALUE);
        poolOperationMap.setDownGracePeriod(0);
        poolOperationMap.setRestartGracePeriod(0);
        poolOperationMap.loadPools();
        setAllPoolsToEnabled();
        setAllPoolsOperationsToEnabled();
    }

    @Test
    public void shouldSubmitUpdateWhenPoolIsNotInPrimaryGroup() {
        givenADownStatusChangeFor("standard_pool-0");
        assertEquals("WAITING", poolOperationMap.getState("standard_pool-0"));
    }

    @Test
    public void shouldNotSubmitUpdateWhenReadOnlyIsReceivedOnResilientPool() {
        givenAReadOnlyDownStatusChangeFor("qos_pool-0");
        assertEquals("IDLE", poolOperationMap.getState(pool));
    }

    @Test
    public void shouldSubmitUpdateWhenUpIsReceivedOnResilientPool() {
        givenADownStatusChangeFor("qos_pool-0");
        givenAnUpStatusChangeFor("qos_pool-0");
        assertEquals("WAITING", poolOperationMap.getState(pool));
    }

    @Test
    public void shouldProcess0FilesWhenPoolWithMinReplicasRestarts() {
        givenMinimumReplicasOnPool();
        givenARestartStatusChangeFor(pool);
        whenPoolOpScanIsRun();
        theResultingNumberOfFileOperationsSubmittedWas(0);
    }

    @Test
    public void shouldProcess10FilesWhenPoolWithSingleReplicasGoesDown() {
        givenSingleReplicasOnPool();
        givenADownStatusChangeFor(pool);
        whenPoolOpScanIsRun();

        /*
         * 5 REPLICA ONLINE, 5 CUSTODIAL
         * (the inaccessible handler is invoked later, during
         * the verification phase)
         */
        theResultingNumberOfFileOperationsSubmittedWas(10);
    }

    @Test
    public void shouldProcess10FilesWhenPoolWithSingleReplicasRestarts() {
        givenSingleReplicasOnPool();
        givenADownStatusChangeFor(pool);
        givenARestartStatusChangeFor(pool);
        whenPoolOpScanIsRun();
        /*
         *  5 REPLICA ONLINE, 5 CUSTODIAL ONLINE
         */
        theResultingNumberOfFileOperationsSubmittedWas(10);
    }

    @Test
    public void shouldProcess6FilesWhenPoolWithExcessReplicasDown() {
        givenExcessReplicasOnPool();
        givenADownStatusChangeFor(pool);
        whenPoolOpScanIsRun();

        /*
         * 3 REPLICA ONLINE,  CUSTODIAL ONLINE
         */
        theResultingNumberOfFileOperationsSubmittedWas(6);
    }

    @Test
    public void shouldProcess4FilesWhenPoolWithMinReplicasDown() {
        givenMinimumReplicasOnPool();
        givenADownStatusChangeFor(pool);
        whenPoolOpScanIsRun();

        /*
         * 2 REPLICA ONLINE, 2 CUSTODIAL ONLINE
         */
        theResultingNumberOfFileOperationsSubmittedWas(4);
    }

    @Test
    public void shouldProcess6FilesWhenPoolWithExcessReplicasRestarts() {
        givenExcessReplicasOnPool();
        givenADownStatusChangeFor(pool);
        givenARestartStatusChangeFor(pool);
        whenPoolOpScanIsRun();

        /*
         * 3 REPLICA ONLINE, 3 CUSTODIAL ONLINE
         */
        theResultingNumberOfFileOperationsSubmittedWas(6);
    }

    @Test
    public void shouldSubmitUpdateWhenDownIsReceivedOnResilientPool() {
        givenADownStatusChangeFor("qos_pool-0");
        assertEquals(PoolQoSStatus.DOWN,
                        poolOperationMap.getCurrentStatus(pool));
        assertEquals("WAITING", poolOperationMap.getState(pool));
    }

    @Test
    public void shouldSubmitUpdateWhenRestartIsReceivedOnResilientPool() {
        givenADownStatusChangeFor("qos_pool-0");
        givenARestartStatusChangeFor("qos_pool-0");
        assertEquals(PoolQoSStatus.ENABLED,
                        poolOperationMap.getCurrentStatus(pool));
        assertEquals("WAITING", poolOperationMap.getState(pool));
    }

    @Test
    public void shouldSubmitUpdateWhenWriteDisabled() {
        givenADisabledStrictStatusChangeFor("qos_pool-0");
        assertEquals(PoolQoSStatus.DOWN,
                        poolOperationMap.getCurrentStatus(pool));
        assertEquals("WAITING", poolOperationMap.getState(pool));
    }

    @Test
    public void shouldSetOperationToIdleWhenAdjustmentTasksComplete() {
        givenExcessReplicasOnPool();
        givenADownStatusChangeFor(pool);
        whenPoolOpScanIsRun();
        whenVerifyScanIsRun();
        /*
         *  There are six files, max running = 2.
         */
        for (int i = 0; i < 5; ++i) {
            whenAdjusterTasksForPoolSucceed();
            whenVerifyTasksForPoolAreVoided();
            whenVerifyScanIsRun();
        }
        assertEquals("IDLE", poolOperationMap.getState(pool));
    }

    @Test
    public void shouldSetOperationToIdleWhenAdjustmentTasksFail() {
        givenExcessReplicasOnPool();
        givenADownStatusChangeFor(pool);
        whenPoolOpScanIsRun();
        whenVerifyScanIsRun();
        /*
         *  There are six files, max running = 2.
         */
        for (int i = 0; i < 5; ++i) {
            whenAdjusterTasksForPoolFail();
            whenVerifyTasksForPoolAreVoided();
            whenVerifyScanIsRun();
        }
        assertEquals("IDLE", poolOperationMap.getState(pool));
    }

    @Test
    public void shouldNotLaunchSecondOperationUntilFirstHasCompleted() {
        givenExcessReplicasOnPool();
        givenMaxConcurrentScanIs(1);
        givenADownStatusChangeFor("qos_pool-5");
        whenPoolOpScanIsRun();
        assertEquals("RUNNING", poolOperationMap.getState(pool));
        givenADownStatusChangeFor("qos_pool-7");
        whenPoolOpScanIsRun();
        assertEquals("WAITING", poolOperationMap.getState("qos_pool-7"));
        whenVerifyScanIsRun();
        /*
         *  There are six files, max running = 2.  Since a second pool is down,
         *  the excess copies are no longer excess, and these operations will
         *  end up being VOID.
         */
        for (int i = 0; i < 5; ++i) {
            whenAdjusterTasksForPoolFail();
            whenVerifyTasksForPoolAreVoided();
            whenVerifyScanIsRun();
        }
        assertEquals("IDLE", poolOperationMap.getState("qos_pool-5"));
        whenPoolOpScanIsRun();
        /*
         *  There are four files, max running = 2.
         */
        for (int i = 0; i < 4; ++i) {
            whenAdjusterTasksForPoolFail();
            whenVerifyTasksForPoolAreVoided();
            whenVerifyScanIsRun();
        }
        assertEquals("IDLE", poolOperationMap.getState("qos_pool-7"));
    }

    private void givenADownStatusChangeFor(String pool) {
        this.pool = pool;
        PoolV2Mode mode = new PoolV2Mode(PoolV2Mode.DISABLED_DEAD);
        poolInfoMap.updatePoolMode(pool, mode);
        poolOperationHandler.handlePoolStatusChange(pool, PoolQoSStatus.valueOf(mode));
    }

    private void givenAReadOnlyDownStatusChangeFor(String pool) {
        this.pool = pool;
        PoolV2Mode mode = new PoolV2Mode(PoolV2Mode.DISABLED_RDONLY);
        poolInfoMap.updatePoolMode(pool, mode);
        poolOperationHandler.handlePoolStatusChange(pool, PoolQoSStatus.valueOf(mode));
    }

    private void givenARestartStatusChangeFor(String pool) {
        this.pool = pool;
        PoolV2Mode mode = new PoolV2Mode(PoolV2Mode.ENABLED);
        poolInfoMap.updatePoolMode(pool, mode);
        poolOperationHandler.handlePoolStatusChange(pool, PoolQoSStatus.valueOf(mode));
    }

    private void givenADisabledStrictStatusChangeFor(String pool) {
        this.pool = pool;
        PoolV2Mode mode = new PoolV2Mode(PoolV2Mode.DISABLED_STRICT);
        poolInfoMap.updatePoolMode(pool, mode);
        poolOperationHandler.handlePoolStatusChange(pool, PoolQoSStatus.valueOf(mode));
    }

    private void givenAnUpStatusChangeFor(String pool) {
        this.pool = pool;
        PoolV2Mode mode = new PoolV2Mode(PoolV2Mode.DISABLED_RDONLY);
        poolInfoMap.updatePoolMode(pool, mode);
        poolOperationHandler.handlePoolStatusChange(pool, PoolQoSStatus.valueOf(mode));
    }

    private void givenExcessReplicasOnPool() {
        loadFilesWithExcessLocations();
        pool = "qos_pool-5";
    }

    private void givenMaxConcurrentScanIs(int max) {
        poolOperationMap.setMaxConcurrentRunning(max);
    }

    private void givenMinimumReplicasOnPool() {
        loadFilesWithRequiredLocations();
        pool = "qos_pool-13";
    }

    private void givenSingleReplicasOnPool() {
        loadNewFilesOnPoolsWithHostAndRackTags();
        pool = "qos_pool-10";
    }

    private void theResultingNumberOfFileOperationsSubmittedWas(int submitted) {
        FileQoSFilter filter = new FileQoSFilter();
        /*
         *  The operation map is not actually running, so the state is READY.
         */
        filter.setState(ImmutableSet.of("READY"));
        assertEquals(submitted, fileOperationMap.count(filter, new StringBuilder()));
    }

    private void whenPoolOpScanIsRun() {
        poolOperationMap.scan();
        poolOperationMap.runNow();
    }

    private void whenAdjusterTasksForPoolSucceed() {
        adjusterTaskMap.scan();
        Predicate<QoSAdjusterTask> predicate = t->true;
        taskPnfsids = adjusterTaskMap.getTasks(predicate, Integer.MAX_VALUE)
                                     .stream()
                                     .map(QoSAdjusterTask::getPnfsId)
                                     .collect(Collectors.toList());
        taskPnfsids.stream().forEach(id->adjusterTaskMap.updateTask(id, Optional.empty(), null));
        adjusterTaskMap.scan();
    }

    private void whenAdjusterTasksForPoolFail() {
        adjusterTaskMap.scan();
        Predicate<QoSAdjusterTask> predicate = t->true;
        taskPnfsids = adjusterTaskMap.getTasks(predicate, Integer.MAX_VALUE)
            .stream()
            .map(QoSAdjusterTask::getPnfsId)
            .collect(Collectors.toList());
        CacheException unexpected = new CacheException(CacheException.UNEXPECTED_SYSTEM_EXCEPTION, "Forced failure");
        taskPnfsids.stream().forEach(id->adjusterTaskMap.updateTask(id, Optional.empty(), unexpected));
        adjusterTaskMap.scan();
    }

    private void whenVerifyTasksForPoolAreVoided() {
        taskPnfsids.stream().forEach(fileOperationMap::voidOperation);
    }

    private void whenVerifyScanIsRun() {
        fileOperationMap.scan();
    }
}
