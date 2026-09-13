package org.kinotic.system.internal.api.services;

import io.vertx.core.Future;
import org.kinotic.core.api.crud.Page;
import org.kinotic.core.api.crud.Pageable;
import org.kinotic.system.api.model.workload.VmNode;
import org.kinotic.system.api.model.workload.VmNodeStatus;
import org.kinotic.system.api.services.VmNodeService;
import tools.jackson.databind.ObjectMapper;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

/**
 * In-memory stand-in for the Elasticsearch backed {@link VmNodeService}.
 * {@link #findAvailableNode} always places on {@link #availableNode}. Records are stored and
 * returned as serialization round-trips the way Elasticsearch documents are, so a caller's
 * mutation of an entity after a save never alters the stored record, and a full save writes
 * back exactly what the caller's copy holds. The partial updates change only their fields on
 * the stored record and fail when the node has no record, as the real update does.
 */
public class StubVmNodeService implements VmNodeService {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    public final Map<String, VmNode> saved = new ConcurrentHashMap<>();

    public VmNode availableNode;

    @Override
    public Future<VmNode> findAvailableNode(int requiredCpus, int requiredMemoryMb, int requiredDiskMb) {
        return Future.succeededFuture(availableNode);
    }

    @Override
    public Future<Void> updateStatusSync(String nodeId, VmNodeStatus status) {
        return update(nodeId, node -> node.setStatus(status));
    }

    @Override
    public Future<Void> updateAllocationSync(String nodeId, int availableCpus, int availableMemoryMb, int availableDiskMb) {
        return update(nodeId, node -> node.setAvailableCpus(availableCpus)
                                          .setAvailableMemoryMb(availableMemoryMb)
                                          .setAvailableDiskMb(availableDiskMb));
    }

    private Future<Void> update(String nodeId, Consumer<VmNode> partial) {
        Future<Void> ret;
        VmNode stored = saved.get(nodeId);
        if (stored == null) {
            ret = Future.failedFuture(new IllegalStateException("No VmNode record for " + nodeId));
        } else {
            partial.accept(stored);
            // re-put so a reader on another thread sees the mutation through the map's happens-before
            saved.put(nodeId, stored);
            ret = Future.succeededFuture();
        }
        return ret;
    }

    @Override
    public Future<VmNode> save(VmNode entity) {
        saved.put(entity.getId(), snapshot(entity));
        return Future.succeededFuture(entity);
    }

    @Override
    public Future<VmNode> saveSync(VmNode entity) {
        return save(entity);
    }

    @Override
    public Future<VmNode> create(VmNode entity) {
        return save(entity);
    }

    @Override
    public Future<VmNode> createSync(VmNode entity) {
        return save(entity);
    }

    @Override
    public Future<VmNode> findById(String id) {
        VmNode stored = saved.get(id);
        return Future.succeededFuture(stored == null ? null : snapshot(stored));
    }

    private static VmNode snapshot(VmNode entity) {
        return MAPPER.readValue(MAPPER.writeValueAsBytes(entity), VmNode.class);
    }

    @Override
    public Future<Long> count() {
        return Future.succeededFuture((long) saved.size());
    }

    @Override
    public Future<Void> deleteById(String id) {
        saved.remove(id);
        return Future.succeededFuture();
    }

    @Override
    public Future<Void> deleteByIdSync(String id) {
        return deleteById(id);
    }

    @Override
    public Future<Page<VmNode>> findAll(Pageable pageable) {
        List<VmNode> all = saved.values().stream().map(StubVmNodeService::snapshot).toList();
        return Future.succeededFuture(new Page<>(all, (long) all.size()));
    }

    @Override
    public Future<Page<VmNode>> search(String searchText, Pageable pageable) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Future<Void> syncIndex() {
        return Future.succeededFuture();
    }
}
