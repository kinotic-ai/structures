package org.kinotic.core.internal.api.service;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.metrics.ObservableLongGauge;
import io.vertx.core.Context;
import io.vertx.core.Vertx;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.kinotic.core.api.event.EventBusService;
import org.kinotic.core.api.service.RequestLivenessWatcher;
import org.springframework.stereotype.Component;
import reactor.core.Disposable;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Pins requests to node ids and fails them from the cluster membership {@link EventBusService#monitorClusterNodes()}
 * reports. One membership subscription serves every request on the node; a membership change costs one
 * lookup per node that has requests pinned to it.
 *
 * Created by Navíd Mitchell 🤪 on 9/9/26.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class DefaultRequestLivenessWatcher implements RequestLivenessWatcher {

    private final EventBusService eventBusService;
    private final Vertx vertx;
    private final OpenTelemetry openTelemetry;
    private final ConcurrentHashMap<String, Lease> leases = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Set<String>> correlationIdsByNode = new ConcurrentHashMap<>();
    // null until the first membership snapshot arrives: a request pinned before that is judged by the snapshot
    private volatile Set<String> members;
    private Disposable membership;
    private ObservableLongGauge pendingGauge;

    @PostConstruct
    void start() {
        // A count that climbs on a healthy node is the only visible sign of a callee that is up but wedged
        pendingGauge = openTelemetry.getMeter("kinotic.rpc.liveness")
                                    .gaugeBuilder("rpc.pending.requests")
                                    .setDescription("The number of requests in flight")
                                    .setUnit("requests")
                                    .ofLongs()
                                    .buildWithCallback(measurement -> measurement.record(leases.size()));
        membership = eventBusService.monitorClusterNodes()
                                    .subscribe(this::membershipChanged,
                                               throwable -> log.error("Cluster membership monitoring failed, pending requests can no longer be failed on node departure", throwable));
    }

    @PreDestroy
    void stop() {
        pendingGauge.close();
        membership.dispose();
    }

    @Override
    public void watch(String correlationId, String nodeId, Runnable onLost) {
        leases.put(correlationId, new Lease(nodeId, onLost, vertx.getOrCreateContext()));
        // the add runs under the key's lock, so a concurrent settle() emptying and dropping this node's set
        // cannot leave the id in a set that is no longer in the map
        correlationIdsByNode.compute(nodeId, (_, correlationIds) -> {
            Set<String> ret = correlationIds != null ? correlationIds : ConcurrentHashMap.newKeySet();
            ret.add(correlationId);
            return ret;
        });
        // The node may have left between the acknowledgement and this call; the latest snapshot decides,
        // and membershipChanged catches a departure that lands while this method runs
        Set<String> current = members;
        if(current != null && !current.contains(nodeId)){
            lose(correlationId);
        }
    }

    @Override
    public void settle(String correlationId) {
        Lease lease = leases.remove(correlationId);
        if(lease != null){
            unindex(lease.nodeId(), correlationId);
        }
    }

    @Override
    public int pendingCount() {
        return leases.size();
    }

    private void membershipChanged(Set<String> nodes) {
        members = nodes;
        List<String> departed = new ArrayList<>();
        for(String nodeId : correlationIdsByNode.keySet()){
            if(!nodes.contains(nodeId)){
                departed.add(nodeId);
            }
        }
        for(String nodeId : departed){
            Set<String> correlationIds = correlationIdsByNode.remove(nodeId);
            if(correlationIds != null){
                correlationIds.forEach(this::lose);
            }
        }
    }

    // onLost is dispatched to the request's own context and never run on the membership delivery
    // context, so a death burst drains here as queue submissions rather than as caller code
    private void lose(String correlationId) {
        Lease lease = leases.remove(correlationId);
        if(lease != null){
            unindex(lease.nodeId(), correlationId);
            lease.context().runOnContext(_ -> lease.onLost().run());
        }
    }

    private void unindex(String nodeId, String correlationId) {
        correlationIdsByNode.computeIfPresent(nodeId, (_, correlationIds) -> {
            correlationIds.remove(correlationId);
            return correlationIds.isEmpty() ? null : correlationIds;
        });
    }

    private record Lease(String nodeId, Runnable onLost, Context context) {}

}
