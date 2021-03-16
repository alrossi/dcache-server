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

import com.google.common.collect.ImmutableList;
import diskCacheV111.util.CacheException;
import java.util.NoSuchElementException;
import java.util.Set;
import org.dcache.qos.TestBase;
import org.dcache.qos.TestSynchronousExecutor.Mode;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * <p>Tests the application of the pool monitor diffs.  Note that
 *      for the purposes of these tests, the transition from uninitialized pool
 *      to initialized is ignored.</p>
 */
public class PoolInfoChangeHandlerTest extends TestBase {
    PoolInfoDiff          diff;
    Integer               removedPoolIndex;

    @Before
    public void setUp() throws CacheException {
        setUpBase();
        setShortExecutionMode(Mode.RUN);
        setLongExecutionMode(Mode.NOP);
        createChangeHandlers();
        fileOperationMap = mock(FileQoSOperationMap.class);
        initializeCounters();
        wireChangeHandlers();
        setAllPoolsToEnabled();
    }

    @After
    public void tearDown() {
        clearInMemory();
    }

    @Test
    public void shouldAddStorageUnitToGroupOnLinkChange() {
        givenPsuUpdate();
        whenPsuUpdateContainsNewLinkToQosUnitGroup("new-link");
        whenPsuUpdateContainsPoolGroupAddedToNewLink("standard-group","new-link");
        whenPsuChangeHelperIsCalled();
        assertThatPoolInfoContainsQosUnitsForPoolGroup("standard-group");
    }

    @Test
    public void shouldRemoveStorageUnitsFromGroupOnLinkChange() {
        givenPsuUpdate();
        whenPsuUpdateNoLongerContainsLink("qos-link");
        whenPsuChangeHelperIsCalled();
        assertThatPoolInfoDoesNotContainQosUnitsForPoolGroup("qos-group");
    }

    @Test
    public void shouldAddOrphanedPoolToOperationMapWhenAddedToQosGroup() {
        shouldAddPoolToInfoAndOperationMap();
        whenPsuUpdateContainsNewPoolForPoolGroup("new-pool", "qos-group");
        whenPsuChangeHelperIsCalled();
        assertThatPoolInfoContainsPoolForPoolGroup("new-pool", "qos-group");
    }

    @Test
    public void shouldAddPoolToInfoAndOperationMap() {
        givenPsuUpdate();
        whenPsuUpdateContainsNewPool("new-pool");
        whenPsuChangeHelperIsCalled();
        assertThatPoolInfoContainsPool("new-pool");
    }

    @Test
    public void shouldAddPoolToPoolGroupAndScan() {
        givenPsuUpdate();
        whenPsuUpdateContainsNewPool("new-pool");
        whenPsuUpdateContainsNewPoolForPoolGroup("new-pool", "qos-group");
        whenPsuChangeHelperIsCalled();
        assertThatPoolInfoContainsPoolForPoolGroup("new-pool", "qos-group");
    }

    @Test
    public void shouldAddNonPrimaryPoolToPoolGroupAndScan() {
        givenPsuUpdate();
        whenPsuUpdateContainsNewPool("new-pool");
        whenPsuUpdateContainsNewPoolForPoolGroup("new-pool", "standard-group");
        whenPsuChangeHelperIsCalled();
        assertThatPoolInfoContainsPoolForPoolGroup("new-pool","standard-group");
    }

    @Test
    public void shouldAddNonQosGroupToMap() {
        givenPsuUpdate();
        whenPsuUpdateContainsNewNonQosPoolGroup("new-standard-group");
        whenPsuChangeHelperIsCalled();
        assertThatPoolInfoContainsPoolGroup("new-standard-group");
    }

    @Test
    public void shouldAddStorageUnitToMap() {
        givenPsuUpdate();
        whenPsuUpdateContainsNewStorageUnit("new-default-unit");
        whenPsuChangeHelperIsCalled();
        assertThatPoolInfoContainsStorageUnit("new-default-unit");
    }

    @Test
    public void shouldAddQosGroupToMap() {
        givenPsuUpdate();
        whenPsuUpdateContainsNewQosPoolGroup("new-qos-group");
        whenPsuChangeHelperIsCalled();
        assertThatPoolInfoContainsPoolGroup("new-qos-group");
    }

    @Test
    public void shouldRemovePoolFromMap() {
        givenPsuUpdate();
        whenPsuUpdateNoLongerContainsPool("qos_pool-2");
        whenPsuChangeHelperIsCalled();
        assertThatPoolInfoDoesNotContainPool("qos_pool-2");
        assertThatPoolInfoDoesNotContainPoolForPoolGroup(removedPoolIndex,"qos-group");
    }

    @Test
    public void shouldRemovePoolGroupFromMap() {
        givenPsuUpdate();
        whenPsuUpdateNoLongerContainsPoolGroup("qos-group");
        whenPsuChangeHelperIsCalled();
        /*
         * This essentially 'orphans' the pools.  Would not be
         * done without removing the pools first.
         */
        assertThatPoolInfoDoesNotContainPoolGroup("qos-group");
    }

    @Test
    public void shouldRemoveStorageUnitFromMap() {
        givenPsuUpdate();
        whenPsuUpdateNoLongerContainsStorageUnit("qos-0.dcache-devel-test@enstore");
        whenPsuChangeHelperIsCalled();
        /*
         * This essentially 'orphans' files belonging to it.  Would not be
         * done without removing the files first.
         */
        assertThatPoolInfoDoesNotContainStorageUnit("qos-0.dcache-devel-test@enstore");
    }

    @Test
    public void shouldCancelOperationsForRemovedPools() {
        givenPsuUpdate();
        whenPsuUpdateNoLongerContainsPool("qos_pool-2");
        whenPsuUpdateNoLongerContainsPool("qos_pool-1");
        whenPsuChangeHelperIsCalled();
        verify(fileOperationMap, times(2))
            .cancelFileOpForPool(any(String.class), eq(false));
    }

    @Test
    public void shouldCancelOperationsForPoolRemovedFromGroup() {
        givenPsuUpdate();
        whenPsuUpdateNoLongerContainsPoolForPoolGroup("qos_pool-2", "qos-group");
        whenPsuChangeHelperIsCalled();
        verify(fileOperationMap, times(1))
            .cancelFileOpForPool(any(String.class), eq(false));
    }

    private void assertThatPoolInfoContainsPool(String pool) {
        assertNotNull(poolInfoMap.getPoolIndex(pool));
    }

    private void assertThatPoolInfoContainsPoolForPoolGroup(String pool,
                    String group) {
        assertTrue(poolInfoMap.getPoolsOfGroup(poolInfoMap.getGroupIndex(group))
                                                          .contains(poolInfoMap.getPoolIndex(pool)));
    }

    private void assertThatPoolInfoContainsPoolGroup(String group) {
        assertNotNull(poolInfoMap.getGroupIndex(group));
    }

    private void assertThatPoolInfoContainsQosUnitsForPoolGroup(String group) {
        Set<String> units = poolInfoMap.getStorageUnitsForGroup(group);
        assertTrue(units.contains("qos-0.dcache-devel-test@enstore"));
        assertTrue(units.contains("qos-1.dcache-devel-test@enstore"));
        assertTrue(units.contains("qos-2.dcache-devel-test@enstore"));
        assertTrue(units.contains("qos-3.dcache-devel-test@enstore"));
        assertTrue(units.contains("qos-4.dcache-devel-test@enstore"));
    }

    private void assertThatPoolInfoContainsStorageUnit(String unit) {
        assertNotNull(poolInfoMap.getUnitIndex(unit));
    }

    private void assertThatPoolInfoDoesNotContainPool(String pool) {
        Integer index = null;
        try {
            index = poolInfoMap.getPoolIndex(pool);
        } catch (NoSuchElementException e) {
            assertNull(index);
        }
    }

    private void assertThatPoolInfoDoesNotContainPoolForPoolGroup(Integer pool,
                    String group) {
        assertFalse(poolInfoMap.getPoolsOfGroup(poolInfoMap.getGroupIndex(group)).contains(pool));
    }

    private void assertThatPoolInfoDoesNotContainPoolGroup(String group) {
        Integer index = null;
        try {
            index = poolInfoMap.getGroupIndex(group);
        } catch (NoSuchElementException e) {
            assertNull(index);
        }
    }

    private void assertThatPoolInfoDoesNotContainQosUnitsForPoolGroup(String group) {
        Set<String> units = poolInfoMap.getStorageUnitsForGroup(group);
        assertFalse(units.contains("qos-0.dcache-devel-test@enstore"));
        assertFalse(units.contains("qos-1.dcache-devel-test@enstore"));
        assertFalse(units.contains("qos-2.dcache-devel-test@enstore"));
        assertFalse(units.contains("qos-3.dcache-devel-test@enstore"));
        assertFalse(units.contains("qos-4.dcache-devel-test@enstore"));
    }

    private void assertThatPoolInfoDoesNotContainStorageUnit(String unit) {
        Integer index = null;
        try {
            index = poolInfoMap.getUnitIndex(unit);
        } catch (NoSuchElementException e) {
            assertNull(index);
        }
    }

    private void givenPsuUpdate() {
        createNewPoolMonitor();
    }

    private void whenPsuChangeHelperIsCalled() {
        diff = poolInfoChangeHandler.reloadAndScan(newPoolMonitor);
    }

    private void whenPsuUpdateContainsNewLinkToQosUnitGroup(String link) {
        getUpdatedPsu().createLink(link, ImmutableList.of("qos-storage"));
    }

    private void whenPsuUpdateContainsNewNonQosPoolGroup(String group) {
        getUpdatedPsu().createPoolGroup(group, false);
    }

    private void whenPsuUpdateContainsNewPool(String pool) {
        createNewPool(pool);
    }

    private void whenPsuUpdateContainsNewPoolForPoolGroup(String pool,
                    String group) {
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

    private void whenPsuUpdateNoLongerContainsLink(String link) {
        getUpdatedPsu().removeLink(link);
    }

    private void whenPsuUpdateNoLongerContainsPool(String pool) {
        removedPoolIndex = poolInfoMap.getPoolIndex(pool);
        getUpdatedPsu().removePool(pool);
    }

    private void whenPsuUpdateNoLongerContainsPoolForPoolGroup(String pool, String group) {
        getUpdatedPsu().removeFromPoolGroup(group, pool);
    }

    private void whenPsuUpdateNoLongerContainsPoolGroup(String group) {
        getUpdatedPsu().getPoolsByPoolGroup(group)
                       .stream()
                       .forEach(p->getUpdatedPsu().removeFromPoolGroup(group, p.getName()));
        getUpdatedPsu().removePoolGroup(group);
    }

    private void whenPsuUpdateNoLongerContainsStorageUnit(String unit) {
        getUpdatedPsu().removeUnit(unit, false);
    }
}
