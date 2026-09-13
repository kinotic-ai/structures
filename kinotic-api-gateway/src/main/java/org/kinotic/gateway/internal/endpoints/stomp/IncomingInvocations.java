package org.kinotic.gateway.internal.endpoints.stomp;

import lombok.extern.slf4j.Slf4j;
import org.kinotic.core.api.event.CRI;
import org.kinotic.core.api.event.Event;
import org.kinotic.core.api.event.EventConstants;
import org.kinotic.core.api.event.EventConsumer;
import org.kinotic.core.api.event.Metadata;
import org.kinotic.core.api.exceptions.RpcServiceUnavailableException;
import org.kinotic.core.internal.utils.EventUtil;
import org.kinotic.gateway.internal.endpoints.Services;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Invocations the client on this connection has sent to the cluster and is waiting on. Each one is pinned
 * to the node that acknowledged it; if that node leaves the cluster, the client receives an
 * {@link RpcServiceUnavailableException} reply instead. Also holds the client's reply subscriptions, which
 * is how replies reach it.
 *
 * Created by Navíd Mitchell 🤪 on 9/9/26.
 */
@Slf4j
public class IncomingInvocations {

    private final Services services;
    // correlation ids are the client's own, so the node-wide lease key is qualified per connection
    private final String leasePrefix = UUID.randomUUID() + ":";
    private final Map<String, EventConsumer> replySubscriptions = new HashMap<>();
    // pending invocations by correlation id; only ever touched on the connection's event loop
    private final Map<String, Metadata> invocations = new HashMap<>();

    public IncomingInvocations(Services services) {
        this.services = services;
    }

    /**
     * Subscribes the client to one of its reply destinations. A terminal reply settles its invocation on
     * the way to the client.
     */
    public void subscribe(CRI cri, String subscriptionIdentifier, StompSubscriptionHandler subscriptionHandler) {
        EventConsumer eventConsumer = services.eventBusService.listen(cri);
        eventConsumer.handler(event -> {
                         settleIfTerminal(event);
                         subscriptionHandler.handleEvent(event);
                     })
                     .exceptionHandler(subscriptionHandler::handleError);
        // a subscription id the client reuses ends the consumer it named before
        EventConsumer previous = replySubscriptions.put(subscriptionIdentifier, eventConsumer);
        if (previous != null) {
            previous.unregister();
        }
    }

    /**
     * @return true when the identifier was one of the client's reply subscriptions
     */
    public boolean unsubscribe(String subscriptionIdentifier) {
        EventConsumer consumer = replySubscriptions.remove(subscriptionIdentifier);
        if (consumer != null) {
            consumer.unregister();
        }
        return consumer != null;
    }

    /**
     * Records an invocation the client is sending. A cancel control settles the invocation it names. An
     * invocation without a correlation id cannot be matched to a reply and is not recorded.
     */
    public void track(Event<byte[]> request) {
        String correlationId = request.metadata().get(EventConstants.CORRELATION_ID_HEADER);
        if (correlationId != null) {
            String control = request.metadata().get(EventConstants.CONTROL_HEADER);
            if (control == null) {
                invocations.put(correlationId, EventUtil.replyMetadataOf(request.metadata()));
            } else if (EventConstants.CONTROL_VALUE_CANCEL.equals(control)) {
                settle(correlationId);
            }
        }
    }

    /**
     * Pins an invocation to the node that acknowledged it. An invocation that was already settled stays
     * settled.
     */
    public void pin(String correlationId, String nodeId, CRI destination) {
        if (correlationId != null) {
            // the reply can arrive before the ack; computeIfPresent then does nothing
            invocations.computeIfPresent(correlationId, (_, replyMetadata) -> {
                services.requestLivenessWatcher.watch(leasePrefix + correlationId, nodeId, () -> fail(correlationId, destination, nodeId));
                return replyMetadata;
            });
        }
    }

    /**
     * Forgets an invocation: its reply arrived, its send failed, or the client cancelled it.
     */
    public void settle(String correlationId) {
        if (correlationId != null && invocations.remove(correlationId) != null) {
            services.requestLivenessWatcher.settle(leasePrefix + correlationId);
        }
    }

    /**
     * Releases every pending invocation and unsubscribes every reply destination.
     */
    public void dispose() {
        invocations.keySet().forEach(correlationId -> services.requestLivenessWatcher.settle(leasePrefix + correlationId));
        invocations.clear();
        replySubscriptions.values().forEach(EventConsumer::unregister);
        replySubscriptions.clear();
    }

    private void settleIfTerminal(Event<byte[]> reply) {
        if (EventUtil.isTerminalReply(reply.metadata())) {
            settle(reply.metadata().get(EventConstants.CORRELATION_ID_HEADER));
        }
    }

    // the node that took the invocation left the cluster; if its reply got here first there is nothing to do
    private void fail(String correlationId, CRI destination, String nodeId) {
        Metadata replyMetadata = invocations.remove(correlationId);
        if (replyMetadata != null) {
            RpcServiceUnavailableException cause = new RpcServiceUnavailableException(
                    "Node " + nodeId + " left the cluster while serving the request to " + destination.raw());
            try {
                services.eventBusService.send(services.exceptionConverter.convert(replyMetadata, cause));
            } catch (Exception e) {
                log.error("Could not answer request {} to {} after node {} left", correlationId, destination.raw(), nodeId, e);
            }
        }
    }
}
