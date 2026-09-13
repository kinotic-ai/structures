package org.kinotic.system.internal.api.repositories;

import org.kinotic.domain.internal.api.repositories.AbstractRepository;

import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import io.vertx.core.Future;
import org.kinotic.system.api.model.workload.VmNode;
import org.kinotic.system.api.model.workload.VmNodeStatus;
import org.kinotic.system.api.model.workload.VmNodeStatusType;
import org.kinotic.domain.internal.api.services.CrudServiceTemplate;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component
public class VmNodeRepository extends AbstractRepository<VmNode> {

    // declines with noop rather than going negative, so the caller learns the capacity was taken
    private static final String RESERVE_SCRIPT = """
            if (ctx._source.availableCpus < params.cpus
                    || ctx._source.availableMemoryMb < params.memoryMb
                    || ctx._source.availableDiskMb < params.diskMb) {
                ctx.op = 'noop';
            } else {
                ctx._source.availableCpus -= params.cpus;
                ctx._source.availableMemoryMb -= params.memoryMb;
                ctx._source.availableDiskMb -= params.diskMb;
            }
            """;
    private static final String RELEASE_SCRIPT = """
            ctx._source.availableCpus = Math.min(ctx._source.totalCpus, ctx._source.availableCpus + params.cpus);
            ctx._source.availableMemoryMb = Math.min(ctx._source.totalMemoryMb, ctx._source.availableMemoryMb + params.memoryMb);
            ctx._source.availableDiskMb = Math.min(ctx._source.totalDiskMb, ctx._source.availableDiskMb + params.diskMb);
            """;

    public VmNodeRepository(CrudServiceTemplate crudServiceTemplate) {
        super("kinotic_vm_node", VmNode.class, crudServiceTemplate);
    }

    /**
     * Returns an {@link VmNodeStatusType#ONLINE} node with at least the requested resources
     * unallocated, or {@code null} when the cluster has no node with room for them.
     */
    public Future<VmNode> findAvailableNode(int requiredCpus, int requiredMemoryMb, int requiredDiskMb) {
        return findFirst(b -> b.query(composeFilter(termFilter("status.type", VmNodeStatusType.ONLINE.name()),
                                                    atLeast("availableCpus", requiredCpus),
                                                    atLeast("availableMemoryMb", requiredMemoryMb),
                                                    atLeast("availableDiskMb", requiredDiskMb))));
    }

    /**
     * Sets a node's status through a partial update touching only {@code status}, visible to search on
     * completion.
     */
    public Future<Void> updateStatusSync(String nodeId, VmNodeStatus status) {
        return crudServiceTemplate.partialUpdateSync(indexName, nodeId, Map.of("status", status), false);
    }

    /**
     * Takes the resources from a node's unallocated {@code available*} fields in one shard operation, so two
     * reservations can never both be granted the same capacity, visible to search on completion.
     * @return true when the node had the capacity and it is now reserved, false when it did not
     */
    public Future<Boolean> reserveSync(String nodeId, int cpus, int memoryMb, int diskMb) {
        return crudServiceTemplate.scriptedUpdateSync(indexName, nodeId, RESERVE_SCRIPT, allocationParams(cpus, memoryMb, diskMb));
    }

    /**
     * Returns the resources to a node's unallocated {@code available*} fields in one shard operation, never
     * past the node's totals, visible to search on completion.
     */
    public Future<Void> releaseSync(String nodeId, int cpus, int memoryMb, int diskMb) {
        return crudServiceTemplate.scriptedUpdateSync(indexName, nodeId, RELEASE_SCRIPT, allocationParams(cpus, memoryMb, diskMb))
                                  .mapEmpty();
    }

    private static Map<String, Object> allocationParams(int cpus, int memoryMb, int diskMb) {
        return Map.of("cpus", cpus, "memoryMb", memoryMb, "diskMb", diskMb);
    }

    private static Query atLeast(String field, int required) {
        return Query.of(q -> q.range(r -> r.number(n -> n.field(field).gte((double) required))));
    }
}
