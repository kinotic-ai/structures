package org.kinotic.system.internal.api.services;

import io.vertx.core.Future;
import org.apache.commons.lang3.Validate;
import org.kinotic.domain.internal.api.services.AbstractCrudService;
import org.kinotic.system.api.model.workload.VmNode;
import org.kinotic.system.api.model.workload.VmNodeStatus;
import org.kinotic.system.api.services.VmNodeService;
import org.kinotic.system.internal.api.repositories.VmNodeRepository;
import org.springframework.stereotype.Component;


@Component
public class DefaultVmNodeService extends AbstractCrudService<VmNode> implements VmNodeService {

    private final VmNodeRepository vmNodeRepository;

    public DefaultVmNodeService(VmNodeRepository repository) {
        super(repository);
        this.vmNodeRepository = repository;
    }

    @Override
    public Future<VmNode> findAvailableNode(int requiredCpus, int requiredMemoryMb, int requiredDiskMb) {
        return vmNodeRepository.findAvailableNode(requiredCpus, requiredMemoryMb, requiredDiskMb);
    }

    @Override
    public Future<Void> updateStatusSync(String nodeId, VmNodeStatus status) {
        Validate.notNull(nodeId, "VmNode id cannot be null");
        Validate.notNull(status, "VmNode status cannot be null");
        return vmNodeRepository.updateStatusSync(nodeId, status);
    }

    @Override
    public Future<Boolean> reserveSync(String nodeId, int cpus, int memoryMb, int diskMb) {
        Validate.notNull(nodeId, "VmNode id cannot be null");
        return vmNodeRepository.reserveSync(nodeId, cpus, memoryMb, diskMb);
    }

    @Override
    public Future<Void> releaseSync(String nodeId, int cpus, int memoryMb, int diskMb) {
        Validate.notNull(nodeId, "VmNode id cannot be null");
        return vmNodeRepository.releaseSync(nodeId, cpus, memoryMb, diskMb);
    }

    @Override
    protected Future<Void> beforeSave(VmNode entity) {
        Validate.notNull(entity, "VmNode cannot be null");
        Validate.notNull(entity.getId(), "VmNode id cannot be null");
        return Future.succeededFuture();
    }

}
