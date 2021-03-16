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
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import diskCacheV111.poolManager.CostModule;
import diskCacheV111.poolManager.PoolSelectionUnit;
import diskCacheV111.poolManager.PoolSelectionUnit.SelectionLink;
import diskCacheV111.poolManager.PoolSelectionUnit.SelectionPool;
import diskCacheV111.poolManager.PoolSelectionUnit.SelectionPoolGroup;
import diskCacheV111.poolManager.PoolSelectionUnit.SelectionUnit;
import diskCacheV111.poolManager.StorageUnit;
import diskCacheV111.poolManager.StorageUnitInfoExtractor;
import diskCacheV111.pools.PoolCostInfo;
import diskCacheV111.pools.PoolV2Mode;
import diskCacheV111.vehicles.PoolManagerPoolInformation;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.dcache.poolmanager.PoolInfo;
import org.dcache.poolmanager.PoolMonitor;
import org.dcache.qos.services.verifier.handlers.PoolInfoChangeHandler;
import org.dcache.qos.services.verifier.util.AbstractLocationExtractor;
import org.dcache.qos.services.verifier.util.RandomSelectionStrategy;
import org.dcache.util.NonReindexableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.dcache.util.NonReindexableList.MISSING_INDEX;
import static org.dcache.util.NonReindexableList.safeGet;
import static org.dcache.util.NonReindexableList.safeIndexOf;

/**
 *  Serves as an index of PoolSelectionUnit-related information.
 *  <p/>
 *  The internal data structures hold a list of pool names and pool groups
 *  which will always assign a new index number to a new member even if some
 *  of the current members happen to be deleted via a PoolSelectionUnit operation.
 *  <p/>
 *  The reason for doing this is to be able to store most of the pool info
 *  associated with a given operation in progress as index references
 *  (see {@link FileQoSOperation}).
 *  <p/>
 *  The relational tables represented by multimaps of indices capture pool, pool group,
 *  storage unit and hsm membership. There are also maps which define the constraints
 *  for a given storage unit, the tags for a pool, and the live mode and cost
 *  information for a pool.
 *  <p/>
 *  This class also provides methods for determining the primary group of a given pool,
 *  the storage groups connected to a given pool group, and whether a pool group can
 *  satisfy the constraints defined by the storage units bound to it.
 *  <p/>
 *  {@link #apply(PoolInfoDiff)} empties and rebuilds pool-related information based
 *  on a diff obtained from {@link #compare(PoolMonitor)}.  An empty diff is equivalent
 *  to a load operation (at initialization). These methods are called by the map
 *  initializer at startup, and thereafter by {@link PoolInfoChangeHandler}.
 *  <p/>
 *  This class benefits from read-write synchronization, since there will be many
 *  more reads of what is for the most part stable information (note that the periodically
 *  refreshed information is synchronized within the PoolInformation object itself;
 *  hence changes to those values do not require a write lock; e.g., #updatePoolStatus.)
 *  <p/>
 *  Class is not marked final for stubbing/mocking purposes.
 */
public class PoolInfoMap {
    /*
     *  When there are no preferential groups to which a pool belongs, qos will
     *  use the entire set of pools to choose from.  In this case, the group index
     *  returned is this magic number.
     */
    public static final int SYSTEM_PGROUP = -1729;

    /*
     *  Custodial online scan placeholders;
     */
    public static final int ONLINE_CUSTODIAL_SCAN_INDEX = -9999;

    /*
     *  Caller does not need to know this name.  Randomized to make sure it does not
     *  clash with a real pool group name.
     */
    private static final String SYSTEM_PGROUP_NAME = UUID.randomUUID().toString();

    private static final Logger LOGGER = LoggerFactory.getLogger(PoolInfoMap.class);

    /*
     *  Uses pool tags as discriminator keys.
     */
    class LocationExtractor extends AbstractLocationExtractor {
        LocationExtractor(Collection<String> onlyOneCopyPer) {
            super(onlyOneCopyPer);
        }

        @Override
        protected Map<String, String> getKeyValuesFor(String location) {
            return getTags(getPoolIndex(location));
        }
    }

    /*
     *  The NonReindexableList semantics is different on get() and indexOf() in that the former
     *  will throw a NoSuchElementException if the list is set not to reference any nulls
     *  it may have as placeholders for invalidated indices, and to throw a NoSuchElementException
     *  when the element is not in the list.
     *
     *  Referencing this map under lock does not, unfortunately, guarantee consistency in this
     *  regard, as the operation map could carry stale references (e.g., after operation cancel).
     *  Not catching the NoSuchElementException then becomes problematic.
     *
     *  In the interest of safety, all references to the three NonReindexableLists use the static
     *  safe methods of the list so that these failures will not provoke uncaught exceptions.
     */
    private final NonReindexableList<String>            pools               = new NonReindexableList<>();
    private final NonReindexableList<String>            groups              = new NonReindexableList<>();
    private final NonReindexableList<String>            sunits              = new NonReindexableList<>();
    private final Map<Integer, PrimaryGroupMarker>      markers             = new HashMap<>();
    private final Map<Integer, StorageUnitConstraints>  constraints         = new HashMap<>();
    private final Map<Integer, PoolInformation>         poolInfo            = new HashMap<>();
    private final Multimap<Integer, Integer>            poolGroupToPool     = HashMultimap.create();
    private final Multimap<Integer, Integer>            poolToPoolGroup     = HashMultimap.create();
    private final Multimap<Integer, Integer>            storageToPoolGroup  = HashMultimap.create();
    private final Multimap<Integer, Integer>            poolGroupToStorage  = HashMultimap.create();
    private final Multimap<Integer, String>             poolToHsm           = HashMultimap.create();
    private final Set<Integer>                          readPref0Pools      = new HashSet<>();

    private final ReadWriteLock lock  = new ReentrantReadWriteLock(true);
    private final Lock          write = lock.writeLock();
    private final Lock          read  = lock.readLock();

    /**
     *  Called on a dedicated thread.
     *  <p/>
     *  Applies a diff under write lock.
     *  <p/>
     *  Will not clear the NonReindexable lists (pools, groups). This is to maintain
     *  the same indices for the duration of the lifetime of the JVM, since the other
     *  maps may contain live references to pools or groups.
     */
    public void apply(PoolInfoDiff diff) {
        write.lock();
        try {
            /*
             *  -- Remove stale pools, pool groups and storage units.
             *
             *     Pool removal assumes that the pool has been properly drained
             *     of files first; this is not taken care of here.  Similarly
             *     for the removal of pool groups.
             *
             *     If an old pool group has a changed marker, it is also removed.
             */
            LOGGER.trace("removing stale pools, pool groups and storage units");
            diff.getOldPools().stream().forEach(this::removePool);
            diff.getOldGroups().stream().forEach(this::removeGroup);
            diff.getOldUnits().stream().forEach(this::removeUnit);

            /*
             *  -- Remove pools from current groups.
             *
             *     If an old group has a changed marker, it is readded as new.
             */
            LOGGER.trace("removing pools from pool groups");
            diff.getPoolsRemovedFromPoolGroup().entries()
                .forEach(this::removeFromPoolGroup);

            /*
             *  -- Remove units from current groups.
             */
            LOGGER.trace("removing units from pool groups");
            diff.getUnitsRemovedFromPoolGroup().entries()
                .forEach(this::removeStorageUnit);

            /*
             *  -- Add new storage units, pool groups and pools.
             */
            LOGGER.trace("adding new storage units, pool groups and pools");
            diff.getNewUnits().stream().forEach(this::addStorageUnit);
            diff.getNewGroups().stream().forEach(this::addPoolGroup);
            diff.getNewPools().stream().forEach(this::addPool);

            /*
             *  -- Add units to pool groups.
             */
            LOGGER.trace("adding storage units to pool groups");
            diff.getUnitsAddedToPoolGroup().entries().stream()
                .forEach(this::addUnitToPoolGroup);

            /*
             *  -- Add pools to pool groups.
             */
            LOGGER.trace("adding pools to pool groups");
            diff.getPoolsAddedToPoolGroup().entries().stream()
                .forEach(this::addPoolToPoolGroups);

            /*
             *  -- Modify constraints.
             */
            LOGGER.trace("modifying storage unit constraints");
            diff.getConstraints().entrySet().stream()
                .forEach(this::updateConstraints);

            /*
             *  -- Add pool information for the new pools.
             *
             *     The new pools are the only ones to have
             *     entries in the cost info map.
             */
            LOGGER.trace("adding live pool information");
            diff.getPoolCost().keySet().stream()
                              .forEach(p -> setPoolInfo(p, diff.getModeChanged().get(p),
                                                           diff.getTagsChanged().get(p),
                                                           diff.poolCost.get(p)));

            /*
             *  -- Add HSMs.
             */
            LOGGER.trace("adding hsm pool information");
            diff.getHsmsChanged().entrySet().stream()
                                 .forEach(e->updateHsms(e.getKey(), e.getValue()));

            /*
             *  -- Update the readPref0 pools.
             */
            LOGGER.trace("updating set of pools for which there are no links with read pref = 0");
            readPref0Pools.clear();
            diff.getReadPref0().stream().forEach(p->readPref0Pools.add(pools.indexOf(p)));
        } finally {
            write.unlock();
        }
    }

    /**
     *  Called on dedicated thread.
     *  <p/>
     *  Does a diff under read lock.
     *  <p/>
     *  Gathers new pools, removed pools, new pool groups, removed pool groups,
     *  new storage units, removed storage units, modified groups and storage units,
     *  and pool information changes.
     *
     *  @param poolMonitor received from pool manager message.
     *  @return the diff (parsed out into relevant maps and collections).
     */
    public PoolInfoDiff compare(PoolMonitor poolMonitor) {
        read.lock();
        PoolSelectionUnit psu = poolMonitor.getPoolSelectionUnit();
        PoolInfoDiff diff = new PoolInfoDiff();
        try {
            LOGGER.trace("Searching for currently uninitialized pools.");
            getUninitializedPools(diff);

            LOGGER.trace("comparing pools");
            Set<String> commonPools = comparePools(diff, psu);

            LOGGER.trace("comparing pool groups");
            Set<String> commonGroups = comparePoolGroups(diff, psu);

            LOGGER.trace("adding pools and units to new pool groups");
            addPoolsAndUnitsToNewPoolGroups(diff, psu);

            LOGGER.trace("comparing pools in pool groups");
            comparePoolsInPoolGroups(diff, commonGroups, psu);

            LOGGER.trace("find pool group marker changes");
            comparePoolGroupMarkers(diff, commonGroups, psu);

            LOGGER.trace("comparing storage units");
            commonGroups = compareStorageUnits(diff, psu);

            LOGGER.trace("adding pool groups for new storage units");
            addPoolGroupsForNewUnits(diff, psu);

            LOGGER.trace("comparing storage unit links and constraints");
            compareStorageUnitLinksAndConstraints(diff, commonGroups, psu);

            LOGGER.trace("comparing pool info");
            comparePoolInfo(diff, commonPools, poolMonitor);
        } finally {
            read.unlock();
        }

        LOGGER.trace("Diff:\n{}", diff.toString());
        return diff;
    }

    /**
     *  Only for testing.
     */
    @VisibleForTesting
    public StorageUnitConstraints getConstraints(String unit) {
        read.lock();
        try {
            return constraints.get(safeIndexOf(unit, sunits));
        } finally {
            read.unlock();
        }
    }

    public Set<String> getExcludedLocationNames(Collection<String> members) {
        read.lock();
        try {
            return members.stream()
                          .map(l -> poolInfo.get(safeIndexOf(l, pools)))
                          .filter(Objects::nonNull)
                          .filter(PoolInformation::isInitialized)
                          .filter(PoolInformation::isExcluded)
                          .map(PoolInformation::getName)
                          .collect(Collectors.toSet());
        } finally {
            read.unlock();
        }
    }

    public String getGroup(Integer group) {
        read.lock();
        try {
            if (group != null && group == SYSTEM_PGROUP) {
                return SYSTEM_PGROUP_NAME;
            }
            return safeGet(group, groups);
        } finally {
            read.unlock();
        }
    }

    public Integer getGroupIndex(String name) {
        read.lock();
        try {
            if (name == null || SYSTEM_PGROUP_NAME.equals(name)) {
                return SYSTEM_PGROUP;
            }
            return safeIndexOf(name, groups);
        } finally {
            read.unlock();
        }
    }

    public Set<String> getHsmPoolsForStorageUnit(Integer sunit, Set<String> hsms) {
        read.lock();
        try {
            Predicate<Integer> hasHsm = p -> poolToHsm.get(p)
                                                      .stream()
                                                      .filter(hsms::contains)
                                                      .count() != 0;

            Stream<Integer> hsmPools;

            if (sunit == null) {
                hsmPools = pools.stream().map(pools::indexOf).filter(hasHsm);
            } else {
                hsmPools = storageToPoolGroup.get(sunit)
                                             .stream()
                                             .map(poolGroupToPool::get)
                                             .flatMap(pools -> pools.stream())
                                             .filter(hasHsm);
            }

            return hsmPools.filter(pool -> isPoolViable(pool, true))
                            .map(i->safeGet(i, pools))
                            .collect(Collectors.toSet());
        } finally {
            read.unlock();
        }
    }

    /**
     *  Uses pool tags as discriminator keys.
     */
    public AbstractLocationExtractor getLocationExtractor(Collection<String> oneCopyPer) {
        return new LocationExtractor(oneCopyPer);
    }

    public Set<String> getMemberLocations(Integer gindex, Collection<String> locations) {
        if (gindex == null || gindex == SYSTEM_PGROUP) {
            return ImmutableSet.copyOf(locations);
        }

        read.lock();
        try {
            Set<String> ofGroup = poolGroupToPool.get(gindex)
                                                 .stream()
                                                 .map(i -> safeGet(i, pools))
                                                 .collect(Collectors.toSet());
            return locations.stream().filter(ofGroup::contains).collect(Collectors.toSet());
        } finally {
            read.unlock();
        }
    }

    /**
     *  @param writable location is writable and readable if true, only readable if false.
     *  @return all pool group pools which qualify.
     */
    public Set<String> getMemberPools(Integer gindex, boolean writable) {
        read.lock();
        try {
            Stream<Integer> members;
            if (gindex == null || gindex == SYSTEM_PGROUP) {
                members = pools.stream().map(pools::indexOf);
            } else {
                members = poolGroupToPool.get(gindex).stream();
            }
            return ImmutableSet.copyOf(members.filter(p->viable(p, writable))
                                              .map(p->safeGet(p, pools))
                                              .collect(Collectors.toSet()));
        } finally {
            read.unlock();
        }
    }

    public String getPool(Integer pool) {
        read.lock();
        try {
            return safeGet(pool, pools);
        } finally {
            read.unlock();
        }
    }

    public Integer getPoolIndex(String name) {
        read.lock();
        try {
            return safeIndexOf(name, pools);
        } finally {
            read.unlock();
        }
    }

    public Set<Integer> getPoolIndices(Collection<String> locations) {
        read.lock();
        try {
            return locations.stream()
                            .map(p -> safeIndexOf(p, pools))
                            .collect(Collectors.toSet());
        } finally {
            read.unlock();
        }
    }

    public PoolManagerPoolInformation getPoolManagerInfo(Integer pool) {
        read.lock();
        try {
            return new PoolManagerPoolInformation(safeGet(pool, pools),
                                                  poolInfo.get(pool).getCostInfo());
        } finally {
            read.unlock();
        }
    }

    public Set<String> getPools(Collection<Integer> indices) {
        read.lock();
        try {
            return indices.stream().map(i -> safeGet(i, pools)).collect(Collectors.toSet());
        } finally {
            read.unlock();
        }
    }

    public Collection<Integer> getPoolsOfGroup(Integer group) {
        read.lock();
        try {
            if (group == null || group == SYSTEM_PGROUP) {
                return pools.stream().map(pools::indexOf).collect(Collectors.toList());
            }
            return poolGroupToPool.get(group);
        } finally {
            read.unlock();
        }
    }

    public Set<String> getReadableLocations(Collection<String> locations) {
        read.lock();
        try {
            return locations.stream()
                            .filter(location -> viable(safeIndexOf(location, pools), false))
                            .collect(Collectors.toSet());
        } finally {
            read.unlock();
        }
    }

    /**
     *  The effective pool group for an operation has to do with whether its source
     *  or the parent pool belongs to a primary group or not.
     *  <p/>
     *  If the pool belongs to a set of pool groups, but among them there is a single
     *  primary group, then that primary group must be used.  Otherwise, we always select
     *  from among all available pools, designated SYSTEM_GROUP. This
     *  is true even if the location belongs to a single pool group.
     *  <p/>
     *  More simply, selection of targets for copying replicas ignores pool groups except
     *  in the case when the file is linked to one and only one primary group.
     *
     *  @param pool either source of the operation or the pool being scanned.
     *  @return the effective pool group for that operation.
     */
    public Integer getEffectivePoolGroup(Integer pool) throws IllegalStateException {
        read.lock();
        try {
            Collection<Integer> groups = poolToPoolGroup.get(pool);
            Optional<Integer> primary = getPrimaryGroup(pool, groups);
            if (primary.isPresent()) {
                return primary.get();
            }
            return SYSTEM_PGROUP;
        } finally {
            read.unlock();
        }
    }

    public Set<String> getStorageUnitsForGroup(String group) {
        read.lock();
        try {
            return poolGroupToStorage.get(safeIndexOf(group, groups))
                                     .stream().map(u->safeGet(u, sunits))
                                     .collect(Collectors.toSet());
        } finally {
            read.unlock();
        }
    }

    public Map<String, String> getTags(Integer pool) {
        PoolInformation info;

        read.lock();
        try {
            info = poolInfo.get(pool);
        } finally {
            read.unlock();
        }

        if (info == null) {
            return ImmutableMap.of();
        }

        Map<String, String> tags = info.getTags();
        if (tags == null) {
            return ImmutableMap.of();
        }

        return tags;
    }

    public String getUnit(Integer index) {
        read.lock();
        try {
            return safeGet(index, sunits);
        } finally {
            read.unlock();
        }
    }

    public Integer getUnitIndex(String name) {
        read.lock();
        try {
            return safeIndexOf(name, sunits);
        } finally {
            read.unlock();
        }
    }

    public Set<Integer> getValidLocations(Collection<Integer> locations, boolean writable) {
        read.lock();
        try {
            return locations.stream()
                            .filter((i) -> viable(i, writable))
                            .collect(Collectors.toSet());
        } finally {
            read.unlock();
        }
    }

    public boolean hasPool(String pool) {
        read.lock();
        try {
            return pools.contains(pool);
        } finally {
            read.unlock();
        }
    }

    public boolean isReadPref0(String pool) {
        read.lock();
        try {
            return readPref0Pools.contains(safeIndexOf(pool, pools));
        } finally {
            read.unlock();
        }
    }

    public boolean isValidPoolIndex(Integer index) {
        read.lock();
        try {
            return pools.get(index) != null;
        } catch (NoSuchElementException e) {
            return false;
        } finally {
            read.unlock();
        }
    }

    public boolean isPoolViable(Integer pool, boolean writable) {
        read.lock();
        try {
            return viable(pool, writable);
        } finally {
            read.unlock();
        }
    }

    public boolean isInitialized(String pool) {
        read.lock();
        try {
            PoolInformation info = poolInfo.get(safeIndexOf(pool, pools));
            return info != null && info.isInitialized();
        } catch (NoSuchElementException e) {
            return false;
        } finally {
            read.unlock();
        }
    }

    public boolean isEnabled(String pool) {
        read.lock();
        try {
            PoolInformation info = poolInfo.get(safeIndexOf(pool, pools));
            return info != null && info.getMode().isEnabled();
        } catch (NoSuchElementException e) {
            return false;
        } finally {
            read.unlock();
        }
    }

    /**
     * @Admin
     */
    public String listPoolInfo(PoolInfoFilter poolInfoFilter) {
        final StringBuilder builder = new StringBuilder();
        read.lock();
        try {
            pools.stream()
                .map(i->safeIndexOf(i, pools))
                .map(poolInfo::get)
                .filter(poolInfoFilter::matches)
                .forEach((i) -> builder.append(i).append("\n"));
        } finally {
            read.unlock();
        }
        return builder.toString();
    }

    /**
     *  All unit tests are synchronous, so there is no need to lock the map here.
     */
    @VisibleForTesting
    public void setUnitConstraints(String group, Integer required,
                                   Collection<String> oneCopyPer) {
        constraints.put(sunits.indexOf(group),
                        new StorageUnitConstraints(required, oneCopyPer));
    }

    public void updatePoolInfo(String pool, boolean excluded) {
        write.lock();
        try {
            PoolInformation info = poolInfo.get(safeIndexOf(pool, pools));
            if (info != null) {
                info.setExcluded(excluded);
            }
        } finally {
            write.unlock();
        }
    }

    /*
     *  Used in testing only.
     */
    @VisibleForTesting
    public void updatePoolMode(String pool, PoolV2Mode mode) {
        write.lock();
        try {
            Integer key = safeIndexOf(pool, pools);
            PoolInformation info = poolInfo.get(key);
            if (info == null) {
                info = new PoolInformation(pool, key);
                poolInfo.put(key, info);
            }
            info.update(mode, null, null);
        } finally {
            write.unlock();
        }
    }

    /**
     *  A coarse-grained verification that the required and tag constraints of the pool group
     *  and its associated storage groups can be met. For the default and each storage unit,
     *  it attempts to fulfill the  max independent location requirement via the LocationExtractor.
     *
     *  @throws IllegalStateException upon encountering the first set of
     *                               constraints which cannot be met.
     */
    public void verifyConstraints(Integer pgindex) throws IllegalStateException {
        Collection<Integer> storageGroups;
        AbstractLocationExtractor extractor;

        read.lock();
        try {
            storageGroups = poolGroupToStorage.get(pgindex);
            for (Integer index : storageGroups) {
                StorageUnitConstraints unitConstraints = constraints.get(index);
                int required = unitConstraints.getRequired();
                extractor = getLocationExtractor(unitConstraints.getOneCopyPer());
                verify(pgindex, extractor, required);
            }
        } finally {
            read.unlock();
        }
    }

    @VisibleForTesting
    /** Called under write lock **/
    void removeGroup(String group) {
        int index = safeIndexOf(group, groups);
        groups.remove(index);
        markers.remove(index);
        poolGroupToPool.removeAll(index)
            .stream()
            .forEach((pindex) -> poolToPoolGroup.remove(pindex,
                index));
        poolGroupToStorage.removeAll(index)
            .stream()
            .forEach((gindex) -> storageToPoolGroup.remove(
                gindex, index));
    }

    @VisibleForTesting
    /** Called under write lock except during unit test**/
    void removePool(String pool) {
        int pindex = safeIndexOf(pool, pools);
        pools.remove(pindex);
        poolToPoolGroup.removeAll(pindex).stream().forEach((g) ->poolGroupToPool.remove(g, pindex));
        poolInfo.remove(pindex);
        poolToHsm.removeAll(pindex);
    }

    @VisibleForTesting
    /** Called under write lock except during unit test **/
    void removeUnit(String unit) {
        int index = safeIndexOf(unit, sunits);
        sunits.remove(index);
        constraints.remove(index);
        storageToPoolGroup.removeAll(index).stream()
            .forEach((gindex) -> poolGroupToStorage.remove(gindex, index));
    }

    /** Called under write lock **/
    private void addPool(SelectionPool pool) {
        String name = pool.getName();
        pools.add(name);
    }

    /** Called under write lock **/
    private void addPoolGroup(SelectionPoolGroup group) {
        String name = group.getName();
        groups.add(name);
        markers.put(groups.indexOf(name), new PrimaryGroupMarker(group.isPrimary()));
    }

    /** Called under write lock **/
    private void addPoolGroupsForNewUnits(PoolInfoDiff diff,
                                          PoolSelectionUnit psu) {
        Collection<StorageUnit> newUnits = diff.getNewUnits();
        for (StorageUnit unit : newUnits) {
            String name = unit.getName();
            StorageUnitInfoExtractor.getPrimaryGroupsFor(name, psu)
                                    .stream()
                                    .forEach((g) -> diff.unitsAdded.put(g, name));
        }
    }

    /** Called under write lock **/
    private void addPoolToPoolGroups(Entry<String, String> entry) {
        Integer pindex = safeIndexOf(entry.getKey(), pools);
        Integer gindex = safeIndexOf(entry.getValue(), groups);
        poolGroupToPool.put(gindex, pindex);
        poolToPoolGroup.put(pindex, gindex);
    }

    /** Called under write lock **/
    private void addPoolsAndUnitsToNewPoolGroups(PoolInfoDiff diff,
                                                 PoolSelectionUnit psu) {
        Collection<SelectionPoolGroup> newGroups = diff.getNewGroups();
        for (SelectionPoolGroup group : newGroups) {
            String name = group.getName();
            psu.getPoolsByPoolGroup(name)
               .stream()
               .map(SelectionPool::getName)
               .forEach((p) -> diff.poolsAdded.put(p, name));
            StorageUnitInfoExtractor.getStorageUnitsInGroup(name, psu)
               .stream()
               .forEach((u) -> diff.unitsAdded.put(name, u.getName()));
        }
    }

    /** Called under write lock **/
    private void addStorageUnit(StorageUnit unit) {
        String name = unit.getName();
        sunits.add(name);
        constraints.put(sunits.indexOf(name),
                        new StorageUnitConstraints(unit.getRequiredCopies(),
                                                   unit.getOnlyOneCopyPer()));
    }

    /** Called under write lock **/
    private void addUnitToPoolGroup(Entry<String, String> entry) {
        Integer gindex = safeIndexOf(entry.getKey(), groups);
        Integer sindex = safeIndexOf(entry.getValue(), sunits);
        storageToPoolGroup.put(sindex, gindex);
        poolGroupToStorage.put(gindex, sindex);
    }

    /** Called under read lock **/
    private Set<String> comparePoolGroups(PoolInfoDiff diff,
                                          PoolSelectionUnit psu) {
        Set<String> next = psu.getPoolGroups().values()
                                    .stream()
                                    .map(SelectionPoolGroup::getName)
                                    .collect(Collectors.toSet());
        Set<String> curr = poolGroupToPool.keySet()
                                    .stream()
                                    .map(this::getGroup)
                                    .collect(Collectors.toSet());
        Sets.difference(next, curr) .stream()
                                    .map((name) -> psu.getPoolGroups().get(name))
                                    .forEach(diff.newGroups::add);
        Sets.difference(curr, next) .stream()
                                    .forEach(diff.oldGroups::add);
        return Sets.intersection(next, curr);
    }

    /** Called under read lock **/
    private void comparePoolInfo(PoolInfoDiff diff,
                                 Set<String> commonPools,
                                 PoolMonitor poolMonitor) {
        PoolSelectionUnit psu = poolMonitor.getPoolSelectionUnit();
        CostModule costModule = poolMonitor.getCostModule();

        /*
         *  First add the info for all new pools to the diff.
         */
        diff.getNewPools().stream()
            .map(SelectionPool::getName)
            .forEach((p) -> {
                diff.getModeChanged().put(p, getPoolMode(psu.getPool(p)));
                diff.getTagsChanged().put(p, getPoolTags(p, costModule));
                diff.getPoolCost().put(p, getPoolCostInfo(p, costModule));
                diff.getHsmsChanged().put(p, psu.getPool(p).getHsmInstances());
                checkReadPrefs(p, diff.getReadPref0(), psu);
            });

        /*
         *  Now check for differences with current pools that are still valid.
         */
        commonPools.stream()
                   .forEach((p) -> {
                       PoolInformation info = poolInfo.get(getPoolIndex(p));
                       PoolV2Mode newMode = getPoolMode(psu.getPool(p));
                       PoolV2Mode oldMode = info.getMode();
                       if (oldMode == null || (newMode != null
                                       && !oldMode.equals(newMode))) {
                           diff.getModeChanged().put(p, newMode);
                       }

                       ImmutableMap<String, String> newTags
                                       = getPoolTags(p, costModule);
                       ImmutableMap<String, String> oldTags = info.getTags();
                       if (oldTags == null || (newTags != null
                                        && !oldTags.equals(newTags))) {
                           diff.getTagsChanged().put(p, newTags);
                       }

                       /*
                        *  Since we are not altering the actual collections inside
                        *  the PoolInfoMap, but are simply modifying the PoolInformation
                        *  object, and since its own update method is synchronized,
                        *  we can take care of the update here while holding a read lock.
                        */
                       info.update(newMode, newTags, getPoolCostInfo(p, costModule));

                       /*
                        *  HSM info may not be present when the pool was added, so
                        *  we readd it here.
                        */
                       diff.getHsmsChanged().put(p, psu.getPool(p).getHsmInstances());

                       /*
                        *  Pool could have been relinked.
                        */
                       checkReadPrefs(p, diff.getReadPref0(), psu);
                   });

    }

    /** Called under read lock **/
    private void checkReadPrefs(String pool, Set<String> readPref0, PoolSelectionUnit psu) {
        List<SelectionLink> links = psu.getPoolGroupsOfPool(pool).stream()
                                       .map(g -> psu.getLinksPointingToPoolGroup(g.getName()))
                                       .flatMap(c -> c.stream())
                                       .collect(Collectors.toList());
        if (links.isEmpty()) {
            return;
        }

        for (SelectionLink link: links) {
            if (link.getPreferences().getReadPref() != 0) {
                return;
            }
        }

        readPref0.add(pool);
    }

    /** Called under read lock **/
    private Set<String> comparePools(PoolInfoDiff diff, PoolSelectionUnit psu) {
        Set<String> next = psu.getPools().values().stream()
                                                  .map(SelectionPool::getName)
                                                  .collect(Collectors.toSet());
        Set<String> curr = ImmutableSet.copyOf(pools);
        Sets.difference(next, curr).stream().map(psu::getPool).forEach(diff.newPools::add);
        Sets.difference(curr, next).stream().forEach(diff.oldPools::add);
        return Sets.intersection(curr, next);
    }

    /** Called under read lock **/
    private void comparePoolGroupMarkers(PoolInfoDiff diff,
        Set<String> common,
        PoolSelectionUnit psu) {
        for (String group : common) {
            SelectionPoolGroup selectionPoolGroup = psu.getPoolGroups().get(group);
            PrimaryGroupMarker marker = markers.get(groups.indexOf(group));
            if (selectionPoolGroup.isPrimary() != marker.isPrimary()) {
                diff.getOldGroups().add(group);
                diff.getNewGroups().add(selectionPoolGroup);

                /*
                 * Only rescan groups whose marker changed.
                 */
                diff.getMarkerChanged().add(group);
            }
        }
    }

    /** Called under read lock **/
    private void comparePoolsInPoolGroups(PoolInfoDiff diff,
                                          Set<String> common,
                                          PoolSelectionUnit psu) {
        for (String group : common) {
            Set<String> next = psu.getPoolsByPoolGroup(group)
                                  .stream()
                                  .map(SelectionPool::getName)
                                  .collect(Collectors.toSet());
            Set<String> curr = poolGroupToPool.get(safeIndexOf(group, groups))
                                              .stream()
                                              .map(i->safeGet(i, pools))
                                              .collect(Collectors.toSet());
            Sets.difference(next, curr)
                .stream()
                .forEach((p) -> diff.poolsAdded.put(p, group));
            Sets.difference(curr, next)
                .stream()
                .filter((p) -> !diff.oldPools.contains(p))
                .forEach((p) -> diff.poolsRmved.put(p, group));
        }
    }

    /** Called under read lock **/
    private void compareStorageUnitLinksAndConstraints(PoolInfoDiff diff,
                                                       Set<String> common,
                                                       PoolSelectionUnit psu) {
        for (String unit : common) {
            StorageUnit storageUnit = psu.getStorageUnit(unit);
            int index = safeIndexOf(unit, sunits);
            Set<String> next
                = ImmutableSet.copyOf(StorageUnitInfoExtractor.getPoolGroupsFor(unit, psu,
                                                                    false));
            Set<String> curr = storageToPoolGroup.get(index)
                                                 .stream()
                                                 .map(i->safeGet(i, groups))
                                                 .collect(Collectors.toSet());
            Sets.difference(next, curr)
                .stream()
                .forEach((group) -> diff.unitsAdded.put(group, unit));
            Sets.difference(curr, next)
                .stream()
                .filter((group) -> !diff.oldGroups.contains(group))
                .forEach((group) -> diff.unitsRmved.put(group, unit));

            Integer required = storageUnit.getRequiredCopies();
            int newRequired = required == null ? -1 : required;

            StorageUnitConstraints constraints = this.constraints.get(index);
            int oldRequired = !constraints.hasRequirement() ? -1 : constraints.getRequired();

            Set<String> oneCopyPer = ImmutableSet.copyOf(storageUnit.getOnlyOneCopyPer());

            if (newRequired != oldRequired || !oneCopyPer.equals(constraints.getOneCopyPer())) {
                diff.constraints.put(unit, new StorageUnitConstraints(required, oneCopyPer));
            }
        }
    }

    /** Called under read lock **/
    private Set<String> compareStorageUnits(PoolInfoDiff diff,
                                            PoolSelectionUnit psu) {
        Set<String> next = psu.getSelectionUnits().values()
                                .stream()
                                .filter(StorageUnit.class::isInstance)
                                .map(SelectionUnit::getName)
                                .collect(Collectors.toSet());
        Set<String> curr = storageToPoolGroup.keySet()
                                .stream()
                                .map(i->safeGet(i, sunits))
                                .collect(Collectors.toSet());
        Sets.difference(next, curr)
                                .stream()
                                .map(psu::getStorageUnit)
                                .forEach(diff.newUnits::add);
        Sets.difference(curr, next).stream().forEach( diff.oldUnits::add);
        return Sets.intersection(next, curr);
    }

    private PoolV2Mode getPoolMode(SelectionPool pool) {
        /*
         *  Allow a NULL value
         */
        return pool.getPoolMode();
    }

    private ImmutableMap<String, String> getPoolTags(String pool,
                                                     CostModule costModule) {
        PoolInfo poolInfo = costModule.getPoolInfo(pool);
        if (poolInfo == null) {
            return null;
        }
        return poolInfo.getTags();
    }

    private PoolCostInfo getPoolCostInfo(String pool, CostModule costModule) {
        /*
         *  Allow a NULL value
         */
        return costModule.getPoolCostInfo(pool);
    }

    /** called under read lock **/
    private Optional<Integer> getPrimaryGroup(Integer pool, Collection<Integer> groups)
            throws IllegalStateException {
        Collection<Integer> primary = groups.stream()
                                            .filter(gindex -> markers.get(gindex).isPrimary())
                                            .collect(Collectors.toList());

        if (primary.size() > 1) {
                throw new IllegalStateException(String.format(
                    "Pool map is inconsistent; pool %s belongs to "
                        + "more than one primary "
                        + "group: %s.",
                    safeGet(pool, pools),
                    primary.stream().map(this::getGroup).collect(Collectors.toList())));
        }

        LOGGER.trace("number of primary pool groups for pool {}: {}.", pool, primary.size());

        if (primary.size() == 1) {
            return primary.stream().findFirst();
        }

        return Optional.empty();
    }

    private void getUninitializedPools(PoolInfoDiff diff) {
        pools.stream()
             .filter((p) -> !isInitialized(p))
             .forEach(diff.uninitPools::add);
    }

    /** Called under write lock **/
    private void removeFromPoolGroup(Entry<String, String> entry) {
        Integer pindex = safeIndexOf(entry.getKey(), pools);
        Integer gindex = safeIndexOf(entry.getValue(), groups);
        poolGroupToPool.remove(gindex, pindex);
        poolToPoolGroup.remove(pindex, gindex);
    }

    private void removeStorageUnit(Entry<String, String> entry) {
        Integer sindex = safeIndexOf(entry.getValue(), sunits);
        Integer pindex = safeIndexOf(entry.getKey(), groups);
        storageToPoolGroup.remove(sindex, pindex);
        poolGroupToStorage.remove(pindex, sindex);
    }

    /** Called under write lock **/
    private PoolInformation setPoolInfo(String pool,
                                        PoolV2Mode mode,
                                        ImmutableMap<String, String> tags,
                                        PoolCostInfo cost) {
        Integer pindex = safeIndexOf(pool, pools);
        PoolInformation entry = poolInfo.getOrDefault(pindex, new PoolInformation(pool, pindex));
        entry.update(mode, tags, cost);
        poolInfo.put(pindex, entry);
        return entry;
    }

    /** Called under write lock **/
    private void updateConstraints(Entry<String, StorageUnitConstraints> entry) {
        constraints.put(safeIndexOf(entry.getKey(), sunits), entry.getValue());
    }

    /** Called under write lock **/
    private void updateHsms(String pool, Set<String> hsms) {
        Integer index = pools.indexOf(pool);
        if (hsms.isEmpty()) {
            poolToHsm.removeAll(index);
        } else {
            hsms.stream().forEach(hsm -> poolToHsm.put(index, hsm));
        }
    }

    /**
     *  Called under read lock
     *
     *  @param index     of pool group.
     *  @param extractor configured for the specific tag constraints.
     *  @param required  specific to this group or storage unit.
     *  @throws IllegalStateException upon encountering the first set of
     *                                constraints which cannot be met.
     */
    private void verify(Integer index, AbstractLocationExtractor extractor, int required)
            throws IllegalStateException {
        Stream<Integer> indices;

        if (index == SYSTEM_PGROUP) {
            indices = pools.stream().map(p->safeIndexOf(p, pools)).filter(i->i != MISSING_INDEX);
        } else {
            indices = poolGroupToPool.get(index).stream();
        }

        Set<String> members = indices.map(i->safeGet(i, pools)).collect(Collectors.toSet());

        for (int i = 0; i < required; i++) {
            Collection<String> candidates = extractor.getCandidateLocations(members);
            if (candidates.isEmpty()) {
                throw new IllegalStateException("No candidate locations for "
                                                 + (index == SYSTEM_PGROUP ? "any pool" :
                                                 safeGet(index, groups)));
            }
            String selected = RandomSelectionStrategy.SELECTOR.apply(candidates);
            members.remove(selected);
            extractor.addSeenTagsFor(selected);
        }
    }

    private boolean viable(Integer pool, boolean writable) {
        PoolInformation info = poolInfo.get(pool);
        return info != null && info.isInitialized()
            && (writable ? info.canRead() && info.canWrite() : info.canRead());
    }
}