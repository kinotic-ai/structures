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
     * Sets a node's unallocated resources, leaving every other field of the record as it is, and completes
     * once the change is visible to {@link #findAvailableNode}.
     * @param nodeId the id of the node to update
     * @param availableCpus the number of vCPUs not allocated to any workload
     * @param availableMemoryMb the memory not allocated to any workload, in megabytes
     * @param availableDiskMb the disk space not allocated to any workload, in megabytes
     * @return a future that will complete when the allocation is stored, or fail if the node is not registered
     */
    Future<Void> updateAllocationSync(String nodeId, int availableCpus, int availableMemoryMb, int availableDiskMb);

}
