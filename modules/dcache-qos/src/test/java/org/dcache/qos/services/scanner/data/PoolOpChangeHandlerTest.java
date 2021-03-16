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
package org.dcache.qos.services.scanner.data;

import com.google.common.collect.ImmutableList;
import diskCacheV111.poolManager.PoolSelectionUnit.SelectionPool;
import diskCacheV111.util.CacheException;
import org.dcache.qos.TestBase;
import org.dcache.qos.TestSynchronousExecutor.Mode;
import org.dcache.qos.data.PoolQoSStatus;
import org.dcache.qos.listeners.QoSVerificationListener;
import org.dcache.qos.services.scanner.data.PoolOperation.State;
import org.dcache.qos.services.scanner.handlers.PoolOpDiff;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

/**
 * <p>Tests the application of the pool monitor diffs.  Note that
 *      for the purposes of these tests, the transition from uninitialized pool
 *      to initialized is ignored.</p>
 */
public class PoolOpChangeHandlerTest extends TestBase {
    PoolOpDiff diff;
    Integer    removedPoolIndex;

    @Before
    public void setUp() throws CacheException {
        setUpBase();
        setShortExecutionMode(Mode.RUN);
        setLongExecutionMode(Mode.NOP);
        createPoolOperationHandler();
        createPoolOperationMap();
        createChangeHandlers();
        initializeCounters();
        wirePoolOperationMap();
        wirePoolOperationHandler();
        wireChangeHandlers();
        poolOperationHandler.setVerificationListener(mock(QoSVerificationListener.class));
        poolOperationMap.loadPools();

        setAllPoolsOperationsToEnabled();
    }

    @After
    public void tearDown() {
        clearInMemory();
    }

    @Test
    public void shouldScanWhenPoolAddedToPrimaryGroup() {
        givenPsuUpdate();
        whenPsuUpdateContainsNewPool("new-pool");
        whenPsuUpdateContainsNewPoolForPoolGroup("new-pool", "qos-group");
        whenPsuChangeHelperIsCalled();
        assertThatPoolIsBeingScanned("new-pool");
    }

    @Test
    public void shouldNotScanWhenPoolAddedToOperationMap() {
        givenPsuUpdate();
        whenPsuUpdateContainsNewPool("new-pool");
        whenPsuChangeHelperIsCalled();
        assertThatPoolOperationHasBeenAddedFor("new-pool");
        assertThatNoScanCalledFor("new-pool");
    }

    @Test
    public void shouldScanWhenPoolAddedToNonPrimaryGroup() {
        givenPsuUpdate();
        whenPsuUpdateContainsNewPool("new-pool");
        whenPsuUpdateContainsNewPoolForPoolGroup("new-pool", "standard-group");
        whenPsuChangeHelperIsCalled();
        assertThatPoolIsBeingScanned("new-pool");
    }

    @Test
    public void shouldNotScanWhenGroupIsAddedWithNoPools() {
        givenPsuUpdate();
        whenPsuUpdateContainsNewNonQosPoolGroup("new-standard-group");
        whenPsuChangeHelperIsCalled();
        assertThatScanIsNotCalled();
    }

    @Test
    public void shouldNotScanWhenStorageUnitAddedWithNoPoolsLinked() {
        givenPsuUpdate();
        whenPsuUpdateContainsNewStorageUnit("new-default-unit");
        whenPsuChangeHelperIsCalled();
        assertThatScanIsNotCalled();
    }

    @Test
    public void shouldScanWhenNewPoolAndNewGroupIsAdded() {
        givenPsuUpdate();
        whenPsuUpdateContainsNewPool("new-pool");
        whenPsuUpdateContainsNewQosPoolGroup("new-qos-group");
        whenPsuUpdateContainsNewPoolForPoolGroup("new-pool", "new-qos-group");
        whenPsuChangeHelperIsCalled();
        assertThatPoolIsBeingScanned("new-pool");
    }

    @Test
    public void shouldCancelAndRemovePoolFromPoolGroupAndScan() {
        givenPsuUpdate();
        whenPoolIsWaitingToBeScanned("qos_pool-1");
        whenPsuUpdateNoLongerContainsPoolForPoolGroup("qos_pool-1",
                                                      "qos-group");
        whenPsuChangeHelperIsCalled();
        assertThatPoolIsBeingScanned("qos_pool-1");
    }

    @Test
    public void shouldRemovePoolFromMap() {
        givenPsuUpdate();
        whenPsuUpdateNoLongerContainsPool("qos_pool-2");
        whenPsuChangeHelperIsCalled();
        assertThatNoScanCalledFor("qos_pool-2");
    }

    @Test
    public void shouldRemovePoolGroupFromMap() {
        givenPsuUpdate();
        whenPsuUpdateNoLongerContainsPoolGroup("qos-group");
        whenPsuChangeHelperIsCalled();
        /*
         * This essentially 'orphans' the pools.  Should not be
         * done without removing the pools first.
         */
        assertThatScanIsNotCalled();
    }

    @Test
    public void shouldRemoveStorageUnitFromMap() {
        givenPsuUpdate();
        whenPsuUpdateNoLongerContainsStorageUnit("qos-0.dcache-devel-test@enstore");
        whenPsuChangeHelperIsCalled();
        /*
         * This essentially 'orphans' files belonging to it.  Should not be
         * done without removing the files first.
         */
        assertThatScanIsNotCalled();
    }

    @Test
    public void shouldScanGroupWhenStorageConstraintsAreModified() {
        givenPsuUpdate();
        whenPsuUpdateContainsStorageUnitWithNewConstraints("qos-0.dcache-devel-test@enstore");
        whenPsuChangeHelperIsCalled();
        assertThatPoolsInPoolGroupAreBeingScanned("qos-group");
    }

    private void assertThatNoScanCalledFor(String pool) {
        if (poolOperationMap.idle.containsKey(pool)) {
            assertEquals("IDLE", poolOperationMap.getState(pool));
        } else {
            assertFalse(poolOperationMap.waiting.containsKey(pool));
        }
    }

    private void assertThatPoolIsBeingScanned(String pool) {
        assertTrue(poolOperationMap.waiting.containsKey(pool));
        assertEquals("WAITING", poolOperationMap.getState(pool));
    }

    private void assertThatPoolOperationHasBeenAddedFor(String pool) {
        assertTrue(poolOperationMap.idle.containsKey(pool));
    }

    private void assertThatPoolsInPoolGroupAreBeingScanned(String group) {
        testPoolMonitor.getPoolSelectionUnit().getPoolsByPoolGroup(group)
                       .stream().map(SelectionPool::getName)
                                .forEach(this::assertThatPoolIsBeingScanned);
    }

    private void assertThatScanIsNotCalled() {
        assertEquals(0, poolOperationMap.waiting.size());
    }

    private void assertThatScanIsCalled() {
        assertTrue(0 < poolOperationMap.waiting.size());
    }

    private void givenPsuUpdate() {
        createNewPoolMonitor();
    }

    private void whenPoolIsWaitingToBeScanned(String pool) {
        PoolOperation operation = new PoolOperation(0L);
        operation.state = State.WAITING;
        operation.currStatus = PoolQoSStatus.ENABLED;
        poolOperationMap.waiting.put(pool, operation);
    }

    private void whenPsuChangeHelperIsCalled() {
        diff = poolOpChangeHandler.reloadAndScan(newPoolMonitor);
    }

    private void whenPsuUpdateContainsNewLinkToqosUnitGroup(String link) {
        getUpdatedPsu().createLink(link, ImmutableList.of("qos-storage"));
    }

    private void whenPsuUpdateContainsNewNonQosPoolGroup(String group) {
        getUpdatedPsu().createPoolGroup(group, false);
    }

    private void whenPsuUpdateContainsNewPool(String pool) {
        createNewPool(pool);
    }

    private void whenPsuUpdateContainsNewPoolForPoolGroup(String pool, String group) {
        getUpdatedPsu().addToPoolGroup(group, pool);
    }

    private void whenPsuUpdateContainsNewQosPoolGroup(String group) {
        getUpdatedPsu().createPoolGroup(group, true);
    }

    private void whenPsuUpdateContainsNewStorageUnit(String unit) {
        getUpdatedPsu().createUnit(unit, false, true, false, false);
    }

    private void whenPsuUpdateContainsPoolGroupAddedToNewLink(String group,
                    String link) {
        getUpdatedPsu().addLink(link, group);
    }

    private void whenPsuUpdateContainsStorageUnitWithNewConstraints(String unit) {
        Integer required = getUpdatedPsu().getStorageUnit(unit).getRequiredCopies();
        getUpdatedPsu().setStorageUnit(unit, required, new String[] { "subnet" });
    }

    private void whenPsuUpdateNoLongerContainsLink(String link) {
        getUpdatedPsu().removeLink(link);
    }

    private void whenPsuUpdateNoLongerContainsPool(String pool) {
        removedPoolIndex = poolInfoMap.getPoolIndex(pool);
        getUpdatedPsu().removePool(pool);
    }

    private void whenPsuUpdateNoLongerContainsPoolForPoolGroup(String pool,
                    String group) {
        getUpdatedPsu().removeFromPoolGroup(group, pool);
    }

    private void whenPsuUpdateNoLongerContainsPoolGroup(String group) {
        getUpdatedPsu().getPoolsByPoolGroup(group).stream().forEach(
                        (p) -> getUpdatedPsu().removeFromPoolGroup(group,
                                        p.getName()));
        getUpdatedPsu().removePoolGroup(group);
    }

    private void whenPsuUpdateNoLongerContainsStorageUnit(String unit) {
        getUpdatedPsu().removeUnit(unit, false);
    }
}
