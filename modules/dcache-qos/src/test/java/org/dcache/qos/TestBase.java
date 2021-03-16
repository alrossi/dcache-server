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
package org.dcache.qos;

import com.google.common.collect.ImmutableList;
import diskCacheV111.poolManager.Pool;
import diskCacheV111.poolManager.PoolSelectionUnitV2;
import diskCacheV111.pools.PoolV2Mode;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.Message;
import java.io.File;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.dcache.pool.classic.Cancellable;
import org.dcache.pool.migration.ProportionalPoolSelectionStrategy;
import org.dcache.poolmanager.PoolMonitor;
import org.dcache.qos.data.PoolQoSStatus;
import org.dcache.qos.local.clients.LocalQoSAdjustmentClient;
import org.dcache.qos.local.clients.LocalQoSRequirementsClient;
import org.dcache.qos.local.clients.LocalQoSScannerClient;
import org.dcache.qos.local.clients.LocalQoSVerificationClient;
import org.dcache.qos.services.adjuster.adjusters.QoSAdjusterFactory;
import org.dcache.qos.services.adjuster.data.QoSAdjusterTaskMap;
import org.dcache.qos.services.adjuster.handlers.QoSAdjustTaskCompletionHandler;
import org.dcache.qos.services.adjuster.handlers.QoSAdjusterTaskHandler;
import org.dcache.qos.services.adjuster.util.QoSAdjusterCounters;
import org.dcache.qos.services.engine.handler.FileQoSStatusHandler;
import org.dcache.qos.services.engine.provider.ALRPStorageUnitQoSProvider;
import org.dcache.qos.services.engine.util.QoSEngineCounters;
import org.dcache.qos.services.scanner.data.PoolFilter;
import org.dcache.qos.services.scanner.data.PoolOperationMap;
import org.dcache.qos.services.scanner.handlers.PoolOpChangeHandler;
import org.dcache.qos.services.scanner.handlers.PoolOperationHandler;
import org.dcache.qos.services.scanner.handlers.PoolTaskCompletionHandler;
import org.dcache.qos.services.scanner.util.QoSScannerCounters;
import org.dcache.qos.services.scanner.util.ScannerMapInitializer;
import org.dcache.qos.services.verifier.data.FileQoSOperationMap;
import org.dcache.qos.services.verifier.data.PoolInfoDiff;
import org.dcache.qos.services.verifier.data.PoolInfoMap;
import org.dcache.qos.services.verifier.handlers.CheckpointHandler;
import org.dcache.qos.services.verifier.handlers.FileQoSOperationHandler;
import org.dcache.qos.services.verifier.handlers.PoolGroupAndTagsQoSVerifier;
import org.dcache.qos.services.verifier.handlers.PoolInfoChangeHandler;
import org.dcache.qos.services.verifier.util.PoolInfoLocationSelector;
import org.dcache.qos.services.verifier.util.QoSVerifierCounters;
import org.dcache.qos.util.QoSHistory;
import org.dcache.vehicles.FileAttributes;
import org.junit.After;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class TestBase implements Cancellable {
    protected static final Logger    LOGGER = LoggerFactory.getLogger(TestBase.class);
    protected static final Exception FORCED_FAILURE = new Exception("Forced failure for test purposes");
    protected static final String    CHKPTFILE      = "/tmp/checkpoint-file";
    protected static final String    POOLSFILE      = "/tmp/excluded-pools";

    protected ALRPStorageUnitQoSProvider requirementsProvider;

    protected FileQoSOperationHandler operationHandler;
    protected PoolOperationHandler poolOperationHandler;
    protected FileQoSOperationMap fileOperationMap;
    protected PoolOperationMap poolOperationMap;
    protected PoolInfoMap poolInfoMap;
    protected PoolInfoLocationSelector locationSelector;

    protected QoSAdjusterTaskHandler adjusterTaskHandler;
    protected QoSAdjusterTaskMap adjusterTaskMap;
    protected QoSAdjusterFactory adjusterFactory;

    protected QoSAdjustTaskCompletionHandler taskCompletionHandler;
    protected PoolTaskCompletionHandler poolTaskCompletionHandler;

    protected PoolOpChangeHandler poolOpChangeHandler;
    protected PoolInfoChangeHandler poolInfoChangeHandler;

    protected LocalQoSAdjustmentClient adjustmentListener;
    protected LocalQoSRequirementsClient requirementsListener;
    protected LocalQoSScannerClient poolScanResponseListener;
    protected LocalQoSVerificationClient verificationListener;
    protected PoolGroupAndTagsQoSVerifier statusVerifier;

    protected QoSVerifierCounters verifierCounters;
    protected QoSAdjusterCounters adjusterCounters;
    protected QoSEngineCounters engineCounters;
    protected QoSScannerCounters scannerCounters;

    protected QoSHistory verifierHistory;
    protected QoSHistory adjusterHistory;

    protected TestSynchronousExecutor shortJobExecutor;
    protected TestSynchronousExecutor longJobExecutor;
    protected TestSynchronousExecutor scheduledExecutorService;

    protected TestStub              testPnfsManagerStub;
    protected TestStub              testPinManagerStub;
    protected TestSelectionUnit     testSelectionUnit;
    protected TestCostModule        testCostModule;
    protected TestPoolMonitor       testPoolMonitor;
    protected TestNamespaceAccess   testNamespaceAccess;

    protected TestSelectionUnit   newSelectionUnit;
    protected TestCostModule      newCostModule;
    protected TestPoolMonitor     newPoolMonitor;
    protected TestStub testPoolStub;

    private boolean isCancelled = false;
    private boolean isDone      = false;

    /*
     * Whether the tasks should fail or cancel.
     */
    private TestSynchronousExecutor.Mode shortTaskExecutionMode = TestSynchronousExecutor.Mode.NOP;
    private TestSynchronousExecutor.Mode longTaskExecutionMode  = TestSynchronousExecutor.Mode.NOP;
    private TestSynchronousExecutor.Mode scheduledExecutionMode = TestSynchronousExecutor.Mode.NOP;

    private CheckpointHandler checkpointHandler;

    @Override
    public void cancel(String explanation) {
        isCancelled = true;
    }

    @After
    public void shutDown() {
        clearAll();
    }

    private void clearAll() {
        clearInMemory();
        if (testNamespaceAccess != null) {
            testNamespaceAccess.clear();
            testNamespaceAccess = null;
        }
        File file = new File(CHKPTFILE);
        if (file.exists()) {
            file.delete();
        }
    }

    protected FileAttributes aCustodialNearlineFile() throws CacheException {
        return testNamespaceAccess.getRequiredAttributes(
                        TestData.CUSTODIAL_NEARLINE[0]);
    }

    protected FileAttributes aCustodialOnlineFile() throws CacheException {
        return testNamespaceAccess.getRequiredAttributes(
                        TestData.CUSTODIAL_ONLINE[0]);
    }

    protected FileAttributes aDeletedReplicaOnlineFileWithBothTags()
                    throws CacheException {
        FileAttributes attributes = testNamespaceAccess.getRequiredAttributes(
                        TestData.REPLICA_ONLINE[3]);
        testNamespaceAccess.delete(attributes.getPnfsId(), false);
        attributes.getLocations().clear();
        return attributes;
    }

    protected FileAttributes aFileWithAReplicaOnAllResilientPools()
                    throws CacheException {
        FileAttributes attributes = aReplicaOnlineFileWithNoTags();
        attributes.setLocations(testCostModule.pools.stream().filter(
                        (p) -> p.contains("qos")).collect(
                        Collectors.toList()));
        return attributes;
    }

    protected FileAttributes aFileWithThreeReplicasInsteadOfTwo()
                    throws CacheException {
        return testNamespaceAccess.getRequiredAttributes(
                        TestData.REPLICA_ONLINE[1]);
    }

    protected FileAttributes aNonResilientFile() throws CacheException {
        return testNamespaceAccess.getRequiredAttributes(
                        TestData.CUSTODIAL_NEARLINE[0]);
    }

    protected FileAttributes aReplicaOnlineFileWithBothTags()
                    throws CacheException {
        return testNamespaceAccess.getRequiredAttributes(
                        TestData.REPLICA_ONLINE[3]);
    }

    protected FileAttributes aReplicaOnlineFileWithBothTagsButNoLocations()
                    throws CacheException {
        FileAttributes attributes = testNamespaceAccess.getRequiredAttributes(
                        TestData.REPLICA_ONLINE[3]);
        testNamespaceAccess.delete(attributes.getPnfsId(), true);
        return testNamespaceAccess.getRequiredAttributes(
                        TestData.REPLICA_ONLINE[3]);
    }

    protected FileAttributes aReplicaOnlineFileWithHostTag()
                    throws CacheException {
        return testNamespaceAccess.getRequiredAttributes(
                        TestData.REPLICA_ONLINE[0]);
    }

    protected FileAttributes aReplicaOnlineFileWithNoTags()
                    throws CacheException {
        return testNamespaceAccess.getRequiredAttributes(
                        TestData.REPLICA_ONLINE[4]);
    }

    protected FileAttributes aReplicaOnlineFileWithRackTag()
                    throws CacheException {
        return testNamespaceAccess.getRequiredAttributes(
                        TestData.REPLICA_ONLINE[2]);
    }

    protected void clearInMemory() {
        verifierCounters = null;
        if (fileOperationMap != null) {
            fileOperationMap.shutdown();
            fileOperationMap = null;
        }
        if (poolOperationMap != null) {
            poolOperationMap.shutdown();
            poolOperationMap = null;
        }
        poolInfoMap = null;
        if (shortJobExecutor != null) {
            shortJobExecutor.shutdown();
            shortJobExecutor = null;
        }
        if (longJobExecutor != null) {
            longJobExecutor.shutdown();
            longJobExecutor = null;
        }
        testSelectionUnit = null;
        testCostModule = null;
        testPoolMonitor = null;
    }

    protected void createAccess() {
        /**
         * Some tests may try to simulate restart against a persistent namespace
         */
        if (testNamespaceAccess == null) {
            testNamespaceAccess = new TestNamespaceAccess();
        }
    }

    protected void createAdjusterTaskMap() {
        adjusterTaskMap = new QoSAdjusterTaskMap();
        adjusterFactory = new QoSAdjusterFactory();
        adjusterCounters = new QoSAdjusterCounters();
    }

    protected void createAdjusterTaskHandler() {
        adjusterTaskHandler = new QoSAdjusterTaskHandler();
    }

    protected void createCellStubs() {
        testPnfsManagerStub = new TestStub();
        testPinManagerStub = new TestStub();
        testPoolStub = new TestStub();
    }

    protected void createChangeHandlers() {
        poolOpChangeHandler = new PoolOpChangeHandler();
        poolInfoChangeHandler = new PoolInfoChangeHandler();
    }

    protected void createCostModule() {
        testCostModule = new TestCostModule();
    }

    protected void createCounters() {
        verifierCounters = new QoSVerifierCounters();
        adjusterCounters = new QoSAdjusterCounters();
        engineCounters = new QoSEngineCounters();
        scannerCounters = new QoSScannerCounters();
    }

    protected void createLocationSelector() {
        locationSelector = new PoolInfoLocationSelector();
    }

    protected void createFileOperationHandler() {
        operationHandler = new FileQoSOperationHandler();
        taskCompletionHandler = new QoSAdjustTaskCompletionHandler();
    }

    protected void createFileOperationMap() {
        fileOperationMap = new FileQoSOperationMap();
        checkpointHandler = new CheckpointHandler();
    }

    protected void createLocalClients() {
        verificationListener = new LocalQoSVerificationClient();
        adjustmentListener = new LocalQoSAdjustmentClient();
        requirementsListener = new LocalQoSRequirementsClient();
        poolScanResponseListener = new LocalQoSScannerClient();
        statusVerifier = new PoolGroupAndTagsQoSVerifier();
    }

    protected void createPoolInfoMap() {
        poolInfoMap = new PoolInfoMap() {
            /*
             * For the purposes of testing, we ignore the difference
             * concerning uninitialized pools.
             */
            @Override
            public PoolInfoDiff compare(PoolMonitor poolMonitor) {
                PoolInfoDiff diff = super.compare(poolMonitor);
                diff.getUninitializedPools().clear();
                return diff;
            }
        };
    }

    protected void createPoolMonitor() {
        testPoolMonitor = new TestPoolMonitor();
    }

    protected void createPoolOperationHandler() {
        poolOperationHandler = new PoolOperationHandler();
        poolTaskCompletionHandler = new PoolTaskCompletionHandler();
    }

    protected void createPoolOperationMap() {
        poolOperationMap = new PoolOperationMap();
    }

    protected void createRequirementsProvider() {
        requirementsProvider = new ALRPStorageUnitQoSProvider() {
          protected FileAttributes fetchAttributes(PnfsId pnfsId) throws QoSException {
              try {
                  return testNamespaceAccess.getRequiredAttributes(pnfsId);
              } catch (CacheException e) {
                  throw new QoSException(e);
              }
          }
        };
        requirementsProvider.setPoolMonitor(testPoolMonitor);
    }

    protected void createSelectionUnit() {
        testSelectionUnit = new TestSelectionUnit();
    }

    protected void createNewPool(String name) {
        if (newPoolMonitor != null) {
            newSelectionUnit.psu.createPool(name, false, false, false);
            newSelectionUnit.psu.setPoolEnabled(name);
            Pool pool = (Pool)newSelectionUnit.getPool(name);
            pool.setPoolMode(new PoolV2Mode(PoolV2Mode.ENABLED));
            newCostModule.addPool(name, 2);
        }
    }

    protected void createNewPoolMonitor() {
        newSelectionUnit = new TestSelectionUnit();
        newSelectionUnit.load();
        newCostModule = new TestCostModule();
        newCostModule.load();
        newPoolMonitor = new TestPoolMonitor();
        newPoolMonitor.setCostModule(newCostModule);
        newPoolMonitor.setSelectionUnit(newSelectionUnit);
    }

    protected void deleteAllLocationsForFile(PnfsId pnfsid) {
        testNamespaceAccess.delete(pnfsid, true);
    }

    protected void deleteFileFromNamespace(PnfsId pnfsid) {
        testNamespaceAccess.delete(pnfsid, false);
    }

    protected PoolSelectionUnitV2 getUpdatedPsu() {
        if (newSelectionUnit == null) {
            return null;
        }
        return newSelectionUnit.psu;
    }

    protected void initializeCostModule() {
        testCostModule.load();
    }

    protected void initializeCounters() {
        verifierCounters.initialize();
        engineCounters.initialize();
        scannerCounters.initialize();
        adjusterCounters.initialize();
    }

    protected void initializePoolInfoMap() {
        poolInfoMap.apply(poolInfoMap.compare(testPoolMonitor));
    }

    protected void initializeSelectionUnit() {
        testSelectionUnit.load();
    }

    protected void loadFilesWithAllLocationsMissing() {
        testNamespaceAccess.loadMissingResilientLocations();
    }

    protected void loadFilesWithExcessLocations() {
        testNamespaceAccess.loadExcessResilient();
    }

    protected void loadFilesWithNonTaggedExcessLocations() {
        testNamespaceAccess.loadNonTaggedExcessResilient();
    }

    protected void loadFilesWithRequiredLocations() {
        testNamespaceAccess.loadRequiredResilient();
    }

    protected void loadNewFilesOnPoolsWithHostAndRackTags() {
        testNamespaceAccess.loadNewResilientOnHostAndRackTagsDefined();
    }

    protected void loadNewFilesOnPoolsWithHostTags() {
        testNamespaceAccess.loadNewResilientOnHostTagDefined();
    }

    protected void loadNewFilesWithUnmappedStorageUnit() {
        testNamespaceAccess.loadNewResilientWithUnmappedStorageUnit();
    }

    protected void loadNewFilesWithStorageUnitMatchingPattern(){
        testNamespaceAccess.loadNewFilesWithStorageUnitMatchingPattern();
    }

    protected void loadNewFilesOnPoolsWithNoTags() {
        testNamespaceAccess.loadNewResilient();
    }

    protected void loadNonResilientFiles() {
        testNamespaceAccess.loadNonResilient();
    }

    protected void makeNonResilient(String unit) {
        testSelectionUnit.makeStorageUnitNonResilient(unit);
        poolInfoMap.setUnitConstraints(unit, 1, ImmutableList.of());
    }

    protected void offlinePools(String... pool) {
        testSelectionUnit.setOffline(pool);
        for (String p : pool) {
            poolInfoMap.updatePoolMode(p, new PoolV2Mode(PoolV2Mode.DISABLED_STRICT));
        }
    }

    protected void printHistory() {
        if (verifierHistory != null) {
            System.out.println(verifierHistory.ascending(false));
        }

        if (adjusterHistory != null) {
            System.out.println(adjusterHistory.ascending(false));
        }
    }

    protected void setAllPoolsToEnabled() {
        Arrays.stream(testPoolMonitor.getPoolSelectionUnit()
            .getDefinedPools(false))
            .forEach(p -> poolInfoMap.updatePoolMode(p, new PoolV2Mode(PoolV2Mode.ENABLED)));
    }

    protected void setAllPoolsOperationsToEnabled() {
        PoolQoSStatus status = PoolQoSStatus.valueOf(new PoolV2Mode(PoolV2Mode.ENABLED));
        Arrays.stream(testPoolMonitor.getPoolSelectionUnit()
            .getDefinedPools(false))
            .forEach(p -> poolOperationMap.updateStatus(p, status));
    }

    protected void setExcluded(String pool) {
        PoolFilter filter = new PoolFilter();
        filter.setPools(pool);
        poolOperationMap.setIncluded(filter, false);
    }

    protected void setLongExecutionMode(TestSynchronousExecutor.Mode mode) {
        longTaskExecutionMode = mode;
        setLongTaskExecutor();
    }

    protected void setScheduledExecutionMode(
                    TestSynchronousExecutor.Mode mode) {
        scheduledExecutionMode = mode;
        setScheduledExecutor();
    }

    protected <T extends Message> void setPoolMessageProcessor(
                    TestMessageProcessor<T> processor) {
        testPoolStub.setProcessor(processor);
    }

    protected void setShortExecutionMode(TestSynchronousExecutor.Mode mode) {
        shortTaskExecutionMode = mode;
        setShortTaskExecutor();
    }

    protected void setUpBase() throws CacheException {
        createAccess();
        createCellStubs();
        createCostModule();
        createSelectionUnit();
        createPoolMonitor();
        createCounters();
        createPoolInfoMap();
        createLocationSelector();

        wirePoolMonitor();
        wireLocationSelector();

        initializeCostModule();
        initializeSelectionUnit();
        initializePoolInfoMap();

        /*
         * Leave out other initializations here; taken care of in
         * the specific test case.
         */
    }

    protected void turnOnRegex() {
        testSelectionUnit.setUseRegex();
        poolInfoMap.apply(poolInfoMap.compare(testPoolMonitor));
    }

    protected void wireAdjusterTaskHandler() {
        adjusterTaskHandler.setTaskMap(adjusterTaskMap);
        adjusterTaskHandler.setTaskService(longJobExecutor);
        adjusterTaskHandler.setVerificationListener(verificationListener);
    }

    protected void wireAdjusterTaskMap() {
        adjusterFactory.setCompletionHandler(taskCompletionHandler);
        adjusterFactory.setScheduledExecutor(scheduledExecutorService);
        adjusterFactory.setPinManager(testPinManagerStub);
        adjusterFactory.setPools(testPoolStub);
        adjusterTaskMap.setFactory(adjusterFactory);
        adjusterTaskMap.setCounters(adjusterCounters);
        adjusterTaskMap.setExecutorService(longJobExecutor);
        adjusterTaskMap.setHandler(adjusterTaskHandler);
        adjusterTaskMap.setMaxRetries(0);
        adjusterHistory = new QoSHistory();
        adjusterHistory.setCapacity(100);
        adjusterHistory.initialize();
        adjusterTaskMap.setHistory(adjusterHistory);
    }

    protected void wireChangeHandlers() {
        poolInfoChangeHandler.setPoolInfoMap(poolInfoMap);
        poolInfoChangeHandler.setFileOperationMap(fileOperationMap);
        poolOpChangeHandler.setPoolOperationHandler(poolOperationHandler);
        poolOpChangeHandler.setPoolOperationMap(poolOperationMap);
        poolOpChangeHandler.setMapInitializer(new ScannerMapInitializer());
        poolOpChangeHandler.setPoolMonitor(testPoolMonitor);
        poolOpChangeHandler.setRefreshService(scheduledExecutorService);
    }

    protected void wireFileOperationHandler() {
        operationHandler.setFileOpMap(fileOperationMap);
        operationHandler.setPoolInfoMap(poolInfoMap);
        operationHandler.setTaskExecutor(scheduledExecutorService);
        operationHandler.setUpdateExecutor(longJobExecutor);
        operationHandler.setBulkExecutor(longJobExecutor);
        operationHandler.setStatusVerifier(statusVerifier);
        operationHandler.setScanResponseListener(poolScanResponseListener);
        operationHandler.setRequirementsListener(requirementsListener);
        operationHandler.setAdjustmentListener(adjustmentListener);
        FileQoSStatusHandler statusHandler = new FileQoSStatusHandler();
        statusHandler.setQosTransitionTopic(testPoolStub);
        statusHandler.setQoSEngineCounters(engineCounters);
        operationHandler.setActionCompletedListener(statusHandler);
        operationHandler.setCounters(verifierCounters);
    }

    protected void wireFileOperationMap() {
        fileOperationMap.setCheckpointExpiry(Long.MAX_VALUE);
        fileOperationMap.setCheckpointExpiryUnit(TimeUnit.MILLISECONDS);
        fileOperationMap.setCheckpointFilePath(CHKPTFILE);
        fileOperationMap.setCheckpointHandler(checkpointHandler);
        checkpointHandler.setFileQoSOperationMap(fileOperationMap);
        checkpointHandler.setPoolInfoMap(poolInfoMap);
        fileOperationMap.setCounters(verifierCounters);
        verifierHistory = new QoSHistory();
        verifierHistory.setCapacity(100);
        verifierHistory.initialize();
        fileOperationMap.setHistory(verifierHistory);
        fileOperationMap.setOperationHandler(operationHandler);
        fileOperationMap.setPoolInfoMap(poolInfoMap);
        fileOperationMap.setMaxRunning(2);
        fileOperationMap.setMaxRetries(2);
    }

    protected void wireLocalClients() {
        adjustmentListener.setTaskHandler(adjusterTaskHandler);
        requirementsListener.setProvider(requirementsProvider);
        statusVerifier.setPoolInfoMap(poolInfoMap);
        statusVerifier.setPools(testPoolStub);
        statusVerifier.setLocationSelector(locationSelector);
        verificationListener.setFileOpHandler(operationHandler);

        if (testNamespaceAccess != null) {
            testNamespaceAccess.setPools(testPoolStub);
            testNamespaceAccess.setVerificationListener(verificationListener);
        }

        poolScanResponseListener.setCompletionHandler(poolTaskCompletionHandler);
    }

    protected void wireLocationSelector() {
        locationSelector.setPoolInfoMap(poolInfoMap);
        locationSelector.setPoolSelectionStrategy(
                        new ProportionalPoolSelectionStrategy());
    }

    protected void wirePoolMonitor() {
        testPoolMonitor.setCostModule(testCostModule);
        testPoolMonitor.setSelectionUnit(testSelectionUnit);
    }

    protected void wirePoolOperationHandler() {
        poolTaskCompletionHandler.setMap(poolOperationMap);
        poolOperationHandler.setCompletionHandler(poolTaskCompletionHandler);
        poolOperationHandler.setNamespace(testNamespaceAccess);
        poolOperationHandler.setOperationMap(poolOperationMap);
        poolOperationHandler.setScanResponseListener(poolScanResponseListener);
        poolOperationHandler.setTaskService(longJobExecutor);
        poolOperationHandler.setVerificationListener(verificationListener);
        poolOperationHandler.setUpdateService(shortJobExecutor);
    }

    protected void wirePoolOperationMap() {
        poolOperationMap.setDownGracePeriod(0);
        poolOperationMap.setDownGracePeriodUnit(TimeUnit.MINUTES);
        poolOperationMap.setRestartGracePeriod(0);
        poolOperationMap.setRestartGracePeriodUnit(TimeUnit.MINUTES);
        poolOperationMap.setMaxConcurrentRunning(2);
        poolOperationMap.setRescanWindow(0);
        poolOperationMap.setRescanWindowUnit(TimeUnit.HOURS);
        poolOperationMap.setHandler(poolOperationHandler);
        poolOperationMap.setExcludedPoolsFile(POOLSFILE);
        poolOperationMap.setChangeHandler(poolOpChangeHandler);
        poolOperationMap.setCounters(scannerCounters);
    }

    private void setLongTaskExecutor() {
        longJobExecutor = new TestSynchronousExecutor(longTaskExecutionMode);
    }

    private void setScheduledExecutor() {
        scheduledExecutorService = new TestSynchronousExecutor(scheduledExecutionMode);
    }

    private void setShortTaskExecutor() {
        shortJobExecutor = new TestSynchronousExecutor(shortTaskExecutionMode);
    }
}
