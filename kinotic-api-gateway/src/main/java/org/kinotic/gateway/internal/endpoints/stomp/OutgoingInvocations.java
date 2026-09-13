package org.kinotic.gateway.internal.endpoints.stomp;

import lombok.extern.slf4j.Slf4j;
import org.kinotic.core.api.event.CRI;
import org.kinotic.core.api.event.Event;
import org.kinotic.core.api.event.EventConstants;
import org.kinotic.core.api.event.ListenerStatus;
import org.kinotic.core.api.event.Metadata;
import org.kinotic.core.api.exceptions.RpcServiceUnavailableException;
import org.kinotic.core.internal.utils.EventUtil;
import org.kinotic.gateway.internal.endpoints.Services;
import reactor.core.Disposable;

import java.util.HashMap;
import java.util.Map;

/**
 * Invocations delivered to the services the client on this connection publishes, until the client sends
 * the terminal reply. When the connection closes, every invocation still pending is answered with an
 * {@link RpcServiceUnavailableException} on the requester's behalf. A streaming invocation is cancelled
 * on the client when its requester's reply destination is gone.
 *
 * Created by Navíd Mitchell 🤪 on 9/9/26.
 */
@Slf4j
public class OutgoingInvocations {

    private final Services services;
    // pending invocations by correlation id; only ever touched on the connection's event loop
    private final Map<String, OutgoingInvocation> invocations = new HashMap<>();
    // one watch per streaming invocation, on its requester's reply destination
    private final Map<String, Disposable> requesterMonitors = new HashMap<>();

    public OutgoingInvocations(Services services) {
        this.services = services;
    }

    /**
     * Records an invocation being delivered to the client. A cancel control forgets the invocation it
     * names. An invocation without a correlation id or reply-to expects no reply and is not recorded.
     * @param event the invocation
     * @param subscriptionHandler the subscription it is delivered through; a cancel goes back through the same one
     * @return true when the invocation is now pending, so its replies are allowed until the terminal one
     */
    public boolean deliver(Event<byte[]> event, StompSubscriptionHandler subscriptionHandler) {
        boolean ret = false;
        Metadata metadata = event.metadata();
        String correlationId = metadata.get(EventConstants.CORRELATION_ID_HEADER);
        if (correlationId != null) {
            String control = metadata.get(EventConstants.CONTROL_HEADER);
            if (control == null) {
                if (metadata.contains(EventConstants.REPLY_TO_HEADER)) {
                    invocations.put(correlationId, new OutgoingInvocation(event.cri(),
                                                                          EventUtil.replyMetadataOf(metadata),
                                                                          subscriptionHandler,
                                                                          services.vertx.getOrCreateContext()));
                    ret = true;
                }
            } else if (EventConstants.CONTROL_VALUE_CANCEL.equals(control)) {
                forget(correlationId);
            }
        }
        return ret;
    }

    /**
     * @return true when the reply answers a pending invocation and is addressed to that invocation's
     * reply destination
     */
    public boolean owesReply(Event<byte[]> reply) {
        OutgoingInvocation invocation = invocations.get(reply.metadata().get(EventConstants.CORRELATION_ID_HEADER));
        return invocation != null
                && reply.cri().raw().equals(CRI.create(invocation.replyMetadata().get(EventConstants.REPLY_TO_HEADER)).raw());
    }

    /**
     * Handles a reply the client sent. A terminal reply forgets its invocation. A stream value starts a
     * watch on the requester's reply destination, so the stream can be cancelled once nothing listens there.
     */
    public void observeReply(Event<byte[]> reply) {
        String correlationId = reply.metadata().get(EventConstants.CORRELATION_ID_HEADER);
        if (correlationId != null) {
            if (EventUtil.isTerminalReply(reply.metadata())) {
                forget(correlationId);
            } else {
                OutgoingInvocation invocation = invocations.get(correlationId);
                if (invocation != null) {
                    requesterMonitors.computeIfAbsent(correlationId, _ -> watchRequester(correlationId, invocation));
                }
            }
        }
    }

    /**
     * Stops every watch and answers every pending invocation with an {@link RpcServiceUnavailableException}.
     */
    public void dispose() {
        requesterMonitors.values().forEach(Disposable::dispose);
        requesterMonitors.clear();
        invocations.forEach((correlationId, invocation) -> {
            RpcServiceUnavailableException cause = new RpcServiceUnavailableException(
                    "The connection serving the request disconnected before replying");
            try {
                services.eventBusService.send(services.exceptionConverter.convert(invocation.replyMetadata(), cause));
            } catch (Exception e) {
                log.error("Could not answer invocation {} after its connection closed", correlationId, e);
            }
        });
        invocations.clear();
    }

    private Disposable watchRequester(String correlationId, OutgoingInvocation invocation) {
        CRI replyCri = CRI.create(invocation.replyMetadata().get(EventConstants.REPLY_TO_HEADER));
        return services.eventBusService
                       .monitorListenerStatus(replyCri)
                       .subscribe(status -> {
                                      // arrives on the cluster manager's thread; the cancel frame must be
                                      // written on the connection's event loop
                                      if (status == ListenerStatus.INACTIVE) {
                                          invocation.context().runOnContext(_ -> cancel(correlationId));
                                      }
                                  },
                                  throwable -> log.warn("Requester watch for invocation {} failed", correlationId, throwable));
    }

    // Nothing listens on the requester's reply destination: forget the invocation and tell the client to stop
    // the stream. The requester is answered too, so one whose registration this node had not seen yet gets
    // the error instead of silence; a requester that is gone drops it with the rest.
    private void cancel(String correlationId) {
        OutgoingInvocation invocation = invocations.get(correlationId);
        if (invocation != null) {
            forget(correlationId);
            Metadata metadata = Metadata.create(Map.of(EventConstants.CONTROL_HEADER, EventConstants.CONTROL_VALUE_CANCEL,
                                                       EventConstants.CORRELATION_ID_HEADER, correlationId));
            invocation.subscriptionHandler().handleEvent(Event.create(invocation.cri(), metadata, null));
            RpcServiceUnavailableException cause = new RpcServiceUnavailableException(
                    "No listener on the reply destination of the stream");
            try {
                services.eventBusService.send(services.exceptionConverter.convert(invocation.replyMetadata(), cause));
            } catch (Exception e) {
                log.error("Could not answer invocation {} after its requester stopped listening", correlationId, e);
            }
        }
    }

    private void forget(String correlationId) {
        invocations.remove(correlationId);
        Disposable monitor = requesterMonitors.remove(correlationId);
        if (monitor != null) {
            monitor.dispose();
        }
    }
}
