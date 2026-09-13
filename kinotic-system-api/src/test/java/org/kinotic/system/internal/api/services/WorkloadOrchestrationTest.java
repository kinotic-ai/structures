package org.kinotic.system.internal.api.services;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import org.kinotic.core.api.event.CRI;
import org.mockito.ArgumentCaptor;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.mock;
import static org.mockito.ArgumentMatchers.any;
import static org.junit.jupiter.api.Assertions.assertThrows;
import java.util.Date;
import reactor.core.publisher.Flux;
import org.kinotic.core.api.exceptions.RpcServiceUnavailableException;
import org.kinotic.core.api.exceptions.RpcMissingServiceException;
import org.kinotic.core.api.event.ListenerStatus;
import org.kinotic.core.api.event.EventBusService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.kinotic.system.api.config.KinoticSystemApiProperties;
import org.kinotic.system.api.model.workload.VmNode;
import org.kinotic.system.api.model.workload.VmNodeStatus;
import org.kinotic.system.api.model.workload.VmNodeStatusType;
import org.kinotic.system.api.workload.VmNodeRegistration;
import org.kinotic.management.api.model.workload.Workload;
import org.kinotic.management.api.model.workload.WorkloadStatus;
import org.kinotic.system.api.workload.WorkloadStatusReport;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Behavior of the detached contract in workload orchestration: a detached deploy returns at
 * start while a non-detached one returns at run end, exercised through the real orchestration
 * services against in-memory stubs for persistence and the node's vm-manager.
 */
public class WorkloadOrchestrationTest {

    private static final String NODE_ID = "node-1";

    private StubWorkloadService workloads;
    private StubVmNodeService nodes;
    private StubVmManagerProxy vmManager;
    private EventBusService eventBus;
    private Vertx vertx;
    private KinoticSystemApiProperties properties;
    private DefaultVmNodeOrchestrationService nodeOrchestration;
    private DefaultWorkloadOrchestrationService orchestration;

    @BeforeEach
    void setUp() {
        workloads = new StubWorkloadService();
        nodes = new StubVmNodeService();
        nodes.availableNode = new VmNode(NODE_ID, "node-1", "host-1");
        nodes.saveSync(nodes.availableNode);
        vmManager = new StubVmManagerProxy();
        // the node's vm-manager registration, as the cluster reports it: absent unless a test says otherwise
        eventBus = mock(EventBusService.class);
        when(eventBus.monitorListenerStatus(any())).thenReturn(Flux.just(ListenerStatus.INACTIVE));
        properties = new KinoticSystemApiProperties();
        vertx = Vertx.vertx();
        nodeOrchestration = new DefaultVmNodeOrchestrationService(properties, nodes, workloads, eventBus, vertx);
        orchestration = new DefaultWorkloadOrchestrationService(nodeOrchestration, vmManager,
                                                                nodes, workloads);
    }

    @AfterEach
    void tearDown() throws Exception {
        await(vertx.close());
    }

    @Test
    public void unreachableVmManagerMarksItsNodeUnreachableAtOnce() throws Exception {
        vmManager.failStartWith = new RpcMissingServiceException("no vm-manager registered for node-1");

        Exception failure = assertThrows(Exception.class, () -> await(orchestration.deployWorkload(newWorkload())));
        assertInstanceOf(RpcMissingServiceException.class, failure.getCause());

        // the registration checked is the vm-manager scoped to this node, the address the proxy sends to
        ArgumentCaptor<CRI> watched = ArgumentCaptor.forClass(CRI.class);
        verify(eventBus).monitorListenerStatus(watched.capture());
        assertEquals(NODE_ID, watched.getValue().scope());
        assertTrue(watched.getValue().raw().contains("VmManager"), watched.getValue().raw());
        assertEquals(VmNodeStatusType.UNREACHABLE, awaitNodeStatus(VmNodeStatusType.UNREACHABLE));
    }

    @Test
    public void markingUnreachableLeavesLastSeenAlone() throws Exception {
        // The placement read (availableNode) predates this heartbeat, so a full save of it would
        // put the older lastSeen back
        Date heartbeatAt = await(nodeOrchestration.heartbeat(NODE_ID, List.of())).getLastSeen();
        vmManager.failStartWith = new RpcMissingServiceException("gone");

        assertThrows(Exception.class, () -> await(orchestration.deployWorkload(newWorkload())));

        assertEquals(VmNodeStatusType.UNREACHABLE, awaitNodeStatus(VmNodeStatusType.UNREACHABLE));
        assertEquals(heartbeatAt, nodes.saved.get(NODE_ID).getLastSeen());
    }

    @Test
    public void registeringANewNodeStampsLastSeen() throws Exception {
        Date before = new Date();

        await(nodeOrchestration.registerNode(new VmNodeRegistration("node-2", "node-2", "host-2")));

        assertFalse(nodes.saved.get("node-2").getLastSeen().before(before));
    }

    @Test
    public void reRegisteringANodeStampsLastSeen() throws Exception {
        VmNode node = nodes.saved.get(NODE_ID);
        node.setLastSeen(new Date(System.currentTimeMillis() - 10_000));
        nodes.saveSync(node);
        Date before = new Date();

        await(nodeOrchestration.registerNode(new VmNodeRegistration(NODE_ID, "node-1", "host-1")));

        assertFalse(nodes.saved.get(NODE_ID).getLastSeen().before(before));
    }

    @Test
    public void ordinaryStartFailureDoesNotVerifyTheNode() throws Exception {
        vmManager.failStartWith = new RuntimeException("node exploded");

        assertThrows(Exception.class, () -> await(orchestration.deployWorkload(newWorkload())));

        verify(eventBus, never()).monitorListenerStatus(any());
        assertEquals(VmNodeStatusType.ONLINE, nodes.saved.get(NODE_ID).getStatus().getType());
    }

    @Test
    public void heartbeatBringsAnUnreachableNodeBackOnline() throws Exception {
        vmManager.failStartWith = new RpcMissingServiceException("gone");
        assertThrows(Exception.class, () -> await(orchestration.deployWorkload(newWorkload())));
        assertEquals(VmNodeStatusType.UNREACHABLE, awaitNodeStatus(VmNodeStatusType.UNREACHABLE));

        await(nodeOrchestration.heartbeat(NODE_ID, List.of()));

        assertEquals(VmNodeStatusType.ONLINE, nodes.saved.get(NODE_ID).getStatus().getType());
    }

    @Test
    public void reachableVmManagerKeepsItsNodeOnlineWhenACallFails() throws Exception {
        when(eventBus.monitorListenerStatus(any())).thenReturn(Flux.just(ListenerStatus.ACTIVE));
        vmManager.failStartWith = new RpcServiceUnavailableException("the gateway serving it left mid-call");

        assertThrows(Exception.class, () -> await(orchestration.deployWorkload(newWorkload())));

        assertEquals(VmNodeStatusType.ONLINE, nodes.saved.get(NODE_ID).getStatus().getType());
    }

    @Test
    public void silentNodeGoesOfflineWhateverItReportedLast() throws Exception {
        properties.getSystemApi().getVmNode().setHealthCheckIntervalSeconds(1);
        properties.getSystemApi().getVmNode().setHeartbeatTimeoutSeconds(1);
        Workload deployed = await(orchestration.deployWorkload(newWorkload()));
        VmNode node = nodes.saved.get(NODE_ID);
        node.setStatus(new VmNodeStatus(VmNodeStatusType.DRAINING, "telemetry shipping lost"));
        node.setLastSeen(new Date(System.currentTimeMillis() - 10_000));
        nodes.saveSync(node);

        nodeOrchestration.init();
        try {
            assertEquals(VmNodeStatusType.OFFLINE, awaitNodeStatus(VmNodeStatusType.OFFLINE));
            assertEquals(WorkloadStatus.FAILED, workloads.saved.get(deployed.getId()).getStatus());
        } finally {
            nodeOrchestration.destroy();
        }
    }

    @Test
    public void silentUnreachableNodeGoesOffline() throws Exception {
        properties.getSystemApi().getVmNode().setHealthCheckIntervalSeconds(1);
        properties.getSystemApi().getVmNode().setHeartbeatTimeoutSeconds(1);
        VmNode node = nodes.saved.get(NODE_ID);
        node.setStatus(new VmNodeStatus(VmNodeStatusType.UNREACHABLE, null));
        node.setLastSeen(new Date(System.currentTimeMillis() - 10_000));
        nodes.saveSync(node);

        nodeOrchestration.init();
        try {
            assertEquals(VmNodeStatusType.OFFLINE, awaitNodeStatus(VmNodeStatusType.OFFLINE));
        } finally {
            nodeOrchestration.destroy();
        }
    }

    @Test
    public void silentNodeFailsItsStoppingWorkload() throws Exception {
        properties.getSystemApi().getVmNode().setHealthCheckIntervalSeconds(1);
        properties.getSystemApi().getVmNode().setHeartbeatTimeoutSeconds(1);
        // a stop whose reply never came back from the node leaves the record STOPPING
        Workload deployed = await(orchestration.deployWorkload(newWorkload()));
        workloads.saved.get(deployed.getId()).setStatus(WorkloadStatus.STOPPING);
        VmNode node = nodes.saved.get(NODE_ID);
        node.setLastSeen(new Date(System.currentTimeMillis() - 10_000));
        nodes.saveSync(node);

        nodeOrchestration.init();
        try {
            assertEquals(VmNodeStatusType.OFFLINE, awaitNodeStatus(VmNodeStatusType.OFFLINE));
            assertEquals(WorkloadStatus.FAILED, workloads.saved.get(deployed.getId()).getStatus());
        } finally {
            nodeOrchestration.destroy();
        }
    }

    @Test
    public void detachedDeployCompletesOnceStarted() throws Exception {
        Workload deployed = await(orchestration.deployWorkload(newWorkload()));

        assertEquals(WorkloadStatus.RUNNING, deployed.getStatus());
        assertEquals(NODE_ID, deployed.getNodeId());
    }

    @Test
    public void foregroundDeployCompletesAtRunEnd() throws Exception {
        Future<Workload> run = orchestration.deployWorkload(newForegroundWorkload());
        assertFalse(run.isComplete());

        // Mid-run the node pushes its RUNNING transition while the reply stays pending
        report(vmManager.lastStarted.getId(), WorkloadStatus.RUNNING, null);
        assertFalse(run.isComplete());
        assertEquals(WorkloadStatus.RUNNING, workloads.saved.get(vmManager.lastStarted.getId()).getStatus());

        vmManager.completeRun(WorkloadStatus.STOPPED, 0);

        Workload finished = await(run);
        assertEquals(WorkloadStatus.STOPPED, finished.getStatus());
        assertEquals(0, finished.getExitCode());
        assertEquals(WorkloadStatus.STOPPED, workloads.saved.get(finished.getId()).getStatus());
        assertEquals(0, workloads.saved.get(finished.getId()).getExitCode());
    }

    @Test
    public void foregroundDeployCompletesWithFailureExitCode() throws Exception {
        Future<Workload> run = orchestration.deployWorkload(newForegroundWorkload());

        vmManager.completeRun(WorkloadStatus.FAILED, 137);

        Workload finished = await(run);
        assertEquals(WorkloadStatus.FAILED, finished.getStatus());
        assertEquals(137, finished.getExitCode());
    }

    @Test
    public void detachedReplyDoesNotClobberTerminalReport() throws Exception {
        // A short-lived detached workload: the node's terminal report is applied before the
        // start reply is processed, so the RUNNING reply must not overwrite it
        vmManager.onStart = () -> report(vmManager.lastStarted.getId(), WorkloadStatus.STOPPED, 0);

        Workload deployed = await(orchestration.deployWorkload(newWorkload()));

        assertEquals(WorkloadStatus.STOPPED, deployed.getStatus());
        assertEquals(0, deployed.getExitCode());
        assertEquals(WorkloadStatus.STOPPED, workloads.saved.get(deployed.getId()).getStatus());
    }

    @Test
    public void detachedExitCodeArrivesViaStatusReport() throws Exception {
        Workload deployed = await(orchestration.deployWorkload(newWorkload()));

        report(deployed.getId(), WorkloadStatus.STOPPED, 0);

        Workload stored = workloads.saved.get(deployed.getId());
        assertEquals(WorkloadStatus.STOPPED, stored.getStatus());
        assertEquals(0, stored.getExitCode());
    }

    @Test
    public void foregroundRestartCompletesAtRunEnd() throws Exception {
        Future<Workload> firstRun = orchestration.deployWorkload(newForegroundWorkload());
        String workloadId = vmManager.lastStarted.getId();
        vmManager.completeRun(WorkloadStatus.STOPPED, 0);
        await(firstRun);

        Future<Workload> secondRun = orchestration.restartWorkload(workloadId);
        assertFalse(secondRun.isComplete());

        vmManager.completeRun(WorkloadStatus.STOPPED, 3);

        Workload finished = await(secondRun);
        assertEquals(WorkloadStatus.STOPPED, finished.getStatus());
        assertEquals(3, finished.getExitCode());
    }

    @Test
    public void pinnedDeployLandsOnRequestedNode() throws Exception {
        VmNode target = registeredNode("node-2", 4, 4096, 10240);

        Workload deployed = await(orchestration.deployWorkload(newWorkload().setNodeId("node-2")));

        assertEquals("node-2", deployed.getNodeId());
        assertEquals("node-2", vmManager.lastStarted.getNodeId());
        assertEquals(4 - deployed.getVcpus(), target.getAvailableCpus());
        assertEquals(4096 - deployed.getMemoryMb(), target.getAvailableMemoryMb());
        assertEquals(10240 - deployed.getDiskSizeMb(), target.getAvailableDiskMb());
    }

    @Test
    public void pinnedDeployFailsWhenNodeUnknown() {
        Future<Workload> run = orchestration.deployWorkload(newWorkload().setNodeId("ghost"));

        assertTrue(run.failed());
        assertNull(vmManager.lastStarted);
        assertTrue(workloads.saved.isEmpty());
    }

    @Test
    public void pinnedDeployFailsWhenNodeNotTakingWorkloads() {
        registeredNode("node-2", 4, 4096, 10240)
                .setStatus(new VmNodeStatus(VmNodeStatusType.DRAINING, "disk limits unenforced"));

        Future<Workload> run = orchestration.deployWorkload(newWorkload().setNodeId("node-2"));

        assertTrue(run.failed());
        assertNull(vmManager.lastStarted);
    }

    @Test
    public void pinnedDeployFailsWhenNodeLacksCapacity() {
        registeredNode("node-2", 1, 4096, 10240);

        Future<Workload> run = orchestration.deployWorkload(newWorkload().setNodeId("node-2").setVcpus(2));

        assertTrue(run.failed());
        assertNull(vmManager.lastStarted);
    }

    @Test
    public void secretValuesRedactedInRecordButRealOnNode() throws Exception {
        Workload deployed = await(orchestration.deployWorkload(
                newWorkload().setEnvironment(new LinkedHashMap<>(Map.of("LOG_LEVEL", "debug")))
                             .setSecrets(new LinkedHashMap<>(Map.of("GIT_TOKEN", "secret")))));

        // The node received the real secret; the record only ever held the mask, including
        // when the node's start reply (which echoes environment and secrets) was persisted.
        // Plain environment entries persist verbatim.
        assertEquals("secret", vmManager.lastStarted.getSecrets().get("GIT_TOKEN"));
        assertEquals("<redacted>", workloads.saved.get(deployed.getId()).getSecrets().get("GIT_TOKEN"));
        assertEquals("debug", workloads.saved.get(deployed.getId()).getEnvironment().get("LOG_LEVEL"));
        assertEquals("secret", deployed.getSecrets().get("GIT_TOKEN"));
    }

    @Test
    public void secretValuesRedactedWhenStartFails() {
        vmManager.failStartWith = new RuntimeException("node exploded");

        Future<Workload> run = orchestration.deployWorkload(
                newWorkload().setSecrets(new LinkedHashMap<>(Map.of("GIT_TOKEN", "secret"))));

        assertTrue(run.failed());
        Workload stored = workloads.saved.values().iterator().next();
        assertEquals(WorkloadStatus.FAILED, stored.getStatus());
        assertEquals("<redacted>", stored.getSecrets().get("GIT_TOKEN"));
    }

    private VmNode registeredNode(String nodeId, int cpus, int memoryMb, int diskMb) {
        VmNode node = new VmNode(nodeId, nodeId, "host-" + nodeId);
        node.setTotalCpus(cpus);
        node.setTotalMemoryMb(memoryMb);
        node.setTotalDiskMb(diskMb);
        node.setAvailableCpus(cpus);
        node.setAvailableMemoryMb(memoryMb);
        node.setAvailableDiskMb(diskMb);
        nodes.saved.put(nodeId, node);
        return node;
    }

    private static <T> T await(Future<T> future) throws Exception {
        return future.toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS);
    }

    /**
     * Returns the stored status type of {@link #NODE_ID} once it is {@code expected}, or whatever it is
     * when the wait runs out. Status writes made by verifyNode's continuation land on the Vertx
     * context and by the reaper on its scheduler thread, after the call the test awaited returned.
     */
    private VmNodeStatusType awaitNodeStatus(VmNodeStatusType expected) throws Exception {
        long deadline = System.currentTimeMillis() + 10_000;
        while (nodes.saved.get(NODE_ID).getStatus().getType() != expected && System.currentTimeMillis() < deadline) {
            Thread.sleep(50);
        }
        return nodes.saved.get(NODE_ID).getStatus().getType();
    }

    private static Workload newWorkload() {
        return new Workload("test-workload", "alpine:latest");
    }

    private static Workload newForegroundWorkload() {
        return newWorkload().setDetached(false);
    }

    /**
     * Simulates the node pushing a status report, stamped ahead of the record's last save so
     * the report applies even when both happen within the same millisecond.
     */
    private void report(String workloadId, WorkloadStatus status, Integer exitCode) {
        WorkloadStatusReport statusReport = new WorkloadStatusReport()
                .setWorkloadId(workloadId)
                .setStatus(status)
                .setExitCode(exitCode)
                .setUpdated(System.currentTimeMillis() + 1000);
        nodeOrchestration.reportWorkloadStatus(NODE_ID, List.of(statusReport));
    }
}
