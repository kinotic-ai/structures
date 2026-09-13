package org.kinotic.system.internal.api.services;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import org.kinotic.system.api.workload.VmManagerProxy;
import org.kinotic.core.api.utils.KinoticUtil;
import org.kinotic.core.api.service.ServiceIdentifier;
import org.kinotic.core.api.event.ListenerStatus;
import org.kinotic.core.api.event.EventBusService;
import org.kinotic.core.api.event.CRI;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.Validate;
import org.kinotic.core.api.crud.Pageable;
import org.kinotic.management.api.model.workload.WorkloadStatus;
import org.kinotic.system.api.config.KinoticSystemApiProperties;
import org.kinotic.system.api.config.VmNodeProperties;
import org.kinotic.system.api.services.VmNodeOrchestrationService;
import org.kinotic.system.api.model.workload.VmNode;
import org.kinotic.system.api.workload.VmNodeRegistration;
import org.kinotic.system.api.workload.WorkloadStatusReport;
import org.kinotic.system.api.model.workload.VmNodeStatus;
import org.kinotic.system.api.model.workload.VmNodeStatusType;
import org.kinotic.management.api.model.workload.Workload;
import org.kinotic.system.api.services.VmNodeService;
import org.kinotic.system.api.services.WorkloadService;
import org.springframework.stereotype.Component;

import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Slf4j
@Component
@RequiredArgsConstructor
public class DefaultVmNodeOrchestrationService implements VmNodeOrchestrationService {

    private final KinoticSystemApiProperties orchestratorProperties;
    private final VmNodeService vmNodeService;
    private final WorkloadService workloadService;
    private final EventBusService eventBusService;
    private final Vertx vertx;
    private ScheduledExecutorService scheduler;

    @PostConstruct
    public void init() {
        VmNodeProperties nodeProps = orchestratorProperties.getSystemApi().getVmNode();
        scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "node-health-check");
            t.setDaemon(true);
            return t;
        });
        scheduler.scheduleAtFixedRate(this::checkNodeHealth,
                                      nodeProps.getHealthCheckIntervalSeconds(),
                                      nodeProps.getHealthCheckIntervalSeconds(),
                                      TimeUnit.SECONDS);
        log.info("Node health check scheduled every {}s, timeout {}s",
                 nodeProps.getHealthCheckIntervalSeconds(), nodeProps.getHeartbeatTimeoutSeconds());
    }

    @PreDestroy
    public void destroy() {
        if (scheduler != null) {
            scheduler.shutdown();
        }
    }

    @Override
    public Future<VmNode> registerNode(VmNodeRegistration registration) {
        Validate.notNull(registration, "Registration cannot be null");
        Validate.notNull(registration.getId(), "Node id cannot be null");

        return vmNodeService.findById(registration.getId())
                .compose(existing -> {
                    Future<VmNode> ret;
                    if (existing != null) {
                        // Read the allocation off the old capacity before overwriting it, so a node
                        // that re-registers with different hardware keeps its running workloads
                        // accounted for rather than appearing wholly free.
                        int allocatedCpus = existing.getTotalCpus() - existing.getAvailableCpus();
                        int allocatedMemoryMb = existing.getTotalMemoryMb() - existing.getAvailableMemoryMb();
                        int allocatedDiskMb = existing.getTotalDiskMb() - existing.getAvailableDiskMb();
                        existing.setHostname(registration.getHostname())
                                .setName(registration.getName())
                                .setProviderType(registration.getProviderType())
                                .setTotalCpus(registration.getTotalCpus())
                                .setTotalMemoryMb(registration.getTotalMemoryMb())
                                .setTotalDiskMb(registration.getTotalDiskMb())
                                .setWorkloadDataDir(registration.getWorkloadDataDir())
                                .setAvailableCpus(registration.getTotalCpus() - allocatedCpus)
                                .setAvailableMemoryMb(registration.getTotalMemoryMb() - allocatedMemoryMb)
                                .setAvailableDiskMb(registration.getTotalDiskMb() - allocatedDiskMb)
                                .setStatus(new VmNodeStatus())
                                .setLastSeen(new Date());
                        log.info("Re-registering VmNode: {} ({})", existing.getName(), existing.getId());
                        ret = vmNodeService.saveSync(existing);
                    } else {
                        VmNode node = new VmNode(registration.getId(), registration.getName(), registration.getHostname());
                        node.setProviderType(registration.getProviderType());
                        node.setTotalCpus(registration.getTotalCpus());
                        node.setTotalMemoryMb(registration.getTotalMemoryMb());
                        node.setTotalDiskMb(registration.getTotalDiskMb());
                        node.setAvailableCpus(registration.getTotalCpus());
                        node.setAvailableMemoryMb(registration.getTotalMemoryMb());
                        node.setAvailableDiskMb(registration.getTotalDiskMb());
                        node.setWorkloadDataDir(registration.getWorkloadDataDir());
                        node.setStatus(new VmNodeStatus());
                        node.setLastSeen(new Date());
                        log.info("Registering new VmNode: {} ({})", node.getName(), node.getId());
                        ret = vmNodeService.saveSync(node);
                    }
                    return ret;
                });
    }

    @Override
    public Future<VmNode> heartbeat(String nodeId, List<String> problems) {
        Validate.notNull(nodeId, "Node id cannot be null");
        List<String> found = problems == null ? List.of() : problems;
        String message = found.isEmpty() ? null : String.join("; ", found);
        VmNodeStatusType reported = found.isEmpty() ? VmNodeStatusType.ONLINE : VmNodeStatusType.DRAINING;

        return vmNodeService.findById(nodeId)
                .compose(node -> {
                    Future<VmNode> ret;
                    if (node == null) {
                        ret = Future.failedFuture(
                                new IllegalArgumentException("Node not registered: " + nodeId));
                    } else if (node.getStatus().getType() != reported
                            || !Objects.equals(node.getStatus().getHealthMessage(), message)) {
                        // a heartbeat is the only evidence the node is alive; no other save stamps lastSeen
                        node.setLastSeen(new Date());
                        if (message != null) {
                            log.warn("VmNode {} is not fit for workloads: {}", nodeId, message);
                        } else {
                            log.info("VmNode {} is fit for workloads again", nodeId);
                        }
                        node.setStatus(new VmNodeStatus(reported, message));
                        // findAvailableNode selects on status.type with a search, so the change
                        // has to be in the index before the next placement reads it
                        ret = vmNodeService.saveSync(node);
                    } else {
                        node.setLastSeen(new Date());
                        // Only checkNodeHealth reads lastSeen, through a search against the
                        // heartbeatTimeoutSeconds cutoff, so waiting for the index refresh buys nothing
                        ret = vmNodeService.save(node);
                    }
                    return ret;
                });
    }

    @Override
    public Future<Void> reportWorkloadStatus(String nodeId, List<WorkloadStatusReport> reports) {
        Validate.notNull(nodeId, "Node id cannot be null");
        Validate.notNull(reports, "Reports cannot be null");

        return Future.all(reports.stream()
                                 .map(report -> applyStatusReport(nodeId, report))
                                 .toList())
                     .mapEmpty();
    }

    private Future<Workload> applyStatusReport(String nodeId, WorkloadStatusReport report) {
        return workloadService.findById(report.getWorkloadId())
                .compose(workload -> {
                    Future<Workload> ret;

                    if (workload == null) {
                        // Destroyed since the node recorded the status — stale report
                        ret = Future.succeededFuture();
                    } else if (!nodeId.equals(workload.getNodeId())) {
                        log.warn("Ignoring status report from node {} for workload {} deployed on node {}",
                                 nodeId, report.getWorkloadId(), workload.getNodeId());
                        ret = Future.succeededFuture(workload);
                    } else if (workload.getStatus() == report.getStatus()) {
                        // Same state; still adopt an exit code the record lacks — stopWorkload
                        // persists STOPPED before the node's exit-code-bearing report arrives
                        if (report.getExitCode() != null && workload.getExitCode() == null) {
                            workload.setExitCode(report.getExitCode());
                            ret = workloadService.saveSync(workload);
                        } else {
                            ret = Future.succeededFuture(workload);
                        }
                    } else if (workload.getUpdated() != null
                            && report.getUpdated() <= workload.getUpdated().getTime()) {
                        // A report older than the record's last transition must not clobber it
                        ret = Future.succeededFuture(workload);
                    } else {
                        log.info("Workload {} status {} -> {} per report from node {}",
                                 report.getWorkloadId(), workload.getStatus(), report.getStatus(), nodeId);

                        workload.setStatus(report.getStatus());
                        workload.setExitCode(report.getExitCode());
                        ret = workloadService.saveSync(workload);
                    }

                    return ret;
                });
    }

    @Override
    public Future<Void> deregisterNode(String nodeId) {
        Validate.notNull(nodeId, "Node id cannot be null");

        return workloadService.countForNode(nodeId)
                .compose(count -> {
                    if (count > 0) {
                        return Future.failedFuture(
                                new IllegalStateException("Cannot deregister node with active workloads. "
                                        + "Destroy all workloads on node " + nodeId + " first."));
                    }
                    log.info("Deregistering VmNode: {}", nodeId);
                    return vmNodeService.deleteById(nodeId);
                });
    }

    @Override
    public Future<VmNode> findAvailableNode(int requiredCpus, int requiredMemoryMb, int requiredDiskMb) {
        return vmNodeService.findAvailableNode(requiredCpus, requiredMemoryMb, requiredDiskMb);
    }

    @Override
    public Future<Void> verifyNode(String nodeId) {
        Validate.notNull(nodeId, "Node id cannot be null");
        ServiceIdentifier vmManager = KinoticUtil.serviceIdentifierOf(VmManagerProxy.class);
        CRI address = new ServiceIdentifier(vmManager.zone(), vmManager.namespace(), vmManager.name(), nodeId, vmManager.version()).cri();
        // the first emission is the current registration state; it arrives on the monitor's delivery
        // context, and the caller's context is restored before anything else runs
        return Future.fromCompletionStage(eventBusService.monitorListenerStatus(address).next().toFuture(),
                                          vertx.getOrCreateContext())
                           .compose(status -> status == ListenerStatus.ACTIVE
                                   ? Future.succeededFuture()
                                   : vmNodeService.findById(nodeId).compose(node -> markUnreachable(nodeId, node)));
    }

    private Future<Void> markUnreachable(String nodeId, VmNode node) {
        Future<Void> ret;
        if (node == null || node.getStatus().getType() == VmNodeStatusType.OFFLINE
                || node.getStatus().getType() == VmNodeStatusType.UNREACHABLE) {
            ret = Future.succeededFuture();
        } else {
            log.warn("VmNode {} ({}) is unreachable and holds no vm-manager registration, taking no workloads", node.getName(), nodeId);
            // Only the status is written, so a heartbeat that landed after the read keeps its lastSeen.
            // findAvailableNode selects on status.type with a search, so the change has to be indexed first.
            ret = vmNodeService.updateStatusSync(nodeId,
                                                 new VmNodeStatus(VmNodeStatusType.UNREACHABLE, node.getStatus().getHealthMessage()));
        }
        return ret;
    }

    /**
     * Periodically marks every node that has not sent a heartbeat within the timeout OFFLINE, whatever it
     * reported last, and marks every workload still on it FAILED.
     */
    private void checkNodeHealth() {
        try {
            long heartbeatTimeoutSeconds = orchestratorProperties.getSystemApi().getVmNode().getHeartbeatTimeoutSeconds();
            long cutoff = System.currentTimeMillis() - (heartbeatTimeoutSeconds * 1000);
            Date cutoffDate = new Date(cutoff);

            vmNodeService.findAll(Pageable.create(0, 500, null))
                    .onSuccess(page -> {
                        for (VmNode node : page.getContent()) {
                            // a DRAINING or UNREACHABLE node that falls silent is as dead as an ONLINE one
                            if (node.getStatus().getType() != VmNodeStatusType.OFFLINE
                                    && node.getLastSeen() != null
                                    && node.getLastSeen().before(cutoffDate)) {

                                log.warn("VmNode {} ({}) missed heartbeat, marking OFFLINE",
                                         node.getName(), node.getId());

                                // Only the status is written, so a heartbeat that landed after findAll read
                                // the node keeps its lastSeen
                                vmNodeService.updateStatusSync(node.getId(),
                                                               new VmNodeStatus(VmNodeStatusType.OFFLINE, node.getStatus().getHealthMessage()))
                                        .compose(v -> markNodeWorkloadsFailed(node.getId()))
                                        .onFailure(error -> log.error("Error handling offline node {}", node.getId(), error));
                            }
                        }
                    })
                    .onFailure(error -> log.error("Error during node health check", error));
        } catch (Exception e) {
            log.error("Unexpected error during node health check", e);
        }
    }

    private Future<Void> markNodeWorkloadsFailed(String nodeId) {
        return workloadService.findAllForNode(nodeId, Pageable.create(0, 500, null))
                .compose(page -> {
                    Future<Void> chain = Future.succeededFuture();
                    for (Workload workload : page.getContent()) {
                        // a STOPPING workload is one whose stop never got an answer from the node
                        if (workload.getStatus() == WorkloadStatus.RUNNING
                                || workload.getStatus() == WorkloadStatus.STARTING
                                || workload.getStatus() == WorkloadStatus.STOPPING) {
                            chain = chain.compose(v -> {
                                log.warn("Marking workload {} as FAILED due to node {} going offline",
                                         workload.getId(), nodeId);
                                workload.setStatus(WorkloadStatus.FAILED);
                                return workloadService.saveSync(workload)
                                                      .mapEmpty();
                            });
                        }
                    }
                    return chain;
                });
    }
}
