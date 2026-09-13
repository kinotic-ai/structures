package org.kinotic.system.api.services;

import io.vertx.core.Future;
import org.kinotic.core.api.annotations.Publish;
import org.kinotic.core.api.crud.IdentifiableCrudService;
import org.kinotic.system.api.model.workload.VmNode;
import org.kinotic.system.api.model.workload.VmNodeStatus;

/**
 * Service for managing {@link VmNode} entities.
 * Tracks available nodes in the cluster that can host workloads.
 */
@Publish
public interface VmNodeService extends IdentifiableCrudService<VmNode, String> {

    /**
     * Finds a node with sufficient resources to host a workload with the given requirements.
     * @param requiredCpus the number of vCPUs required
     * @param requiredMemoryMb the amount of memory required in megabytes
     * @param requiredDiskMb the amount of disk space required in megabytes
     * @return a future that will complete with a suitable node, or null if none available
     */
    Future<VmNode> findAvailableNode(int requiredCpus, int requiredMemoryMb, int requiredDiskMb);

    /**
     * Sets a node's {@link VmNode#getStatus() status}, leaving every other field of the record as it is,
     * and completes once the change is visible to {@link #findAvailableNode}.
     * @param nodeId the id of the node to update
     * @param status the node's new status
     * @return a future that will complete when the status is stored, or fail if the node is not registered
     */
    Future<Void> updateStatusSync(String nodeId, VmNodeStatus status);

    /**
     * Reserves resources on a node for a workload: takes them from the node's unallocated capacity if it has
     * them all, atomically against every other reservation and release on the node, and completes once the
     * change is visible to {@link #findAvailableNode}.
     * @param nodeId the id of the node to reserve on
     * @param cpus the vCPUs the workload needs
     * @param memoryMb the memory the workload needs, in megabytes
     * @param diskMb the disk space the workload needs, in megabytes
     * @return a future that will complete with true when the resources are reserved and false when the node
     * no longer has them, or fail if the node is not registered
     */
    Future<Boolean> reserveSync(String nodeId, int cpus, int memoryMb, int diskMb);

    /**
     * Returns a workload's resources to a node's unallocated capacity, never past the node's totals, atomically
     * against every other reservation and release on the node, and completes once the change is visible to
     * {@link #findAvailableNode}.
     * @param nodeId the id of the node to release on
     * @param cpus the vCPUs the workload held
     * @param memoryMb the memory the workload held, in megabytes
     * @param diskMb the disk space the workload held, in megabytes
     * @return a future that will complete when the resources are released, or fail if the node is not registered
     */
    Future<Void> releaseSync(String nodeId, int cpus, int memoryMb, int diskMb);

}
