


package org.kinotic.gateway.internal.endpoints.stomp;

import io.vertx.core.Future;
import io.vertx.core.MultiMap;
import io.vertx.core.http.HttpHeaders;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.Session;
import org.apache.commons.lang3.Validate;
import org.kinotic.core.api.exceptions.AuthenticationException;
import org.kinotic.core.api.exceptions.AuthorizationException;
import org.kinotic.core.api.event.CRI;
import org.kinotic.core.api.event.Event;
import org.kinotic.core.api.event.EventConstants;
import org.kinotic.core.api.event.EventConsumer;
import org.kinotic.core.api.event.SessionKeepAliveMode;
import org.kinotic.core.api.security.ConnectedInfo;
import org.kinotic.core.api.security.SecurityService;
import org.kinotic.core.api.utils.KinoticUtil;
import org.kinotic.core.internal.utils.EventUtil;
import org.kinotic.gateway.internal.endpoints.Services;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tools.jackson.core.JacksonException;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;

/**
 * Created by Navid Mitchell on 11/3/20
 */
public class EndpointConnectionHandler {

    private static final Logger log = LoggerFactory.getLogger(EndpointConnectionHandler.class);
    /** Upgrade header carrying the client secret; also read by name in KinoticSecurityService. */
    private static final String CLIENT_SECRET_HEADER = "clientSecret";
    private final SecurityService securityService;
    private final Services services;
    private final Map<String, EventConsumer> subscriptions = new HashMap<>();
    private final IncomingInvocations incomingInvocations;
    private final OutgoingInvocations outgoingInvocations;
    private Session session;
    private long lastSessionFlush = 0;
    private ConnectedInfo connectedInfo;
    private StompAuthorizer stompAuthorizer;
    private SessionKeepAliveMode sessionKeepAliveMode = SessionKeepAliveMode.ACTIVITY;
    private boolean sessionCreatedHere;
    private long sessionTimer = -1;

    public EndpointConnectionHandler(Services services) {
        this.services = services;
        this.securityService = services.securityService;
        this.incomingInvocations = new IncomingInvocations(services);
        this.outgoingInvocations = new OutgoingInvocations(services);
    }

    public Future<MultiMap> handshake(RoutingContext routingContext) {
        session = routingContext.session();
        this.connectedInfo = connectedInfoFromSession();
        // a session with no login behind it was created for this upgrade
        sessionCreatedHere = connectedInfo == null;

        // vertx-stomp-lite upgrades the request only after this future completes, and the
        // ServerWebSocket it creates keeps the request's header MultiMap for the life of the
        // connection. Credentials left in it stay reachable through ServerWebSocket#headers() and in
        // any heap dump, so they are dropped here -- on both paths, since a client may present a
        // bearer token alongside a session that authenticates it. The Cookie header is deliberately
        // left alone: Vert.x also keeps the parsed jar on the response for the connection's life, so
        // removing the header halves the copies without keeping the session cookie out of a dump.
        Map<String, String> authenticationInfo = toCaseInsensitiveMap(routingContext.request().headers());
        routingContext.request().headers().remove(HttpHeaders.AUTHORIZATION);
        routingContext.request().headers().remove(CLIENT_SECRET_HEADER);

        if (connectedInfo != null && connectedInfo.getParticipant() != null) {
            return Future.succeededFuture(MultiMap.caseInsensitiveMultiMap());
        }

        return securityService.authenticate(authenticationInfo)
                              .recover(throwable -> {
                                  Throwable cause;
                                  if(throwable instanceof AuthenticationException){
                                      cause = throwable;
                                  }else{
                                      cause = new AuthenticationException("Could not authenticate with the given credentials", throwable);
                                  }
                                  return Future.failedFuture(cause);
                              })
                              .map(participant -> {
                                  connectedInfo = new ConnectedInfo();
                                  connectedInfo.setParticipant(participant);
                                  if (session != null) {
                                      session.put(ConnectedInfo.SESSION_KEY, connectedInfo);
                                  }
                                  return MultiMap.caseInsensitiveMultiMap();
                              });
    }

    public Future<Map<String, String>> connect(Map<String, String> connectHeaders) {
        if (connectedInfo == null || connectedInfo.getParticipant() == null) {
            return Future.failedFuture(new AuthenticationException("Client must authenticate before sending a CONNECT frame"));
        }

        Future<Map<String, String>> ret;
        try {
            sessionKeepAliveMode = SessionKeepAliveMode.fromHeader(connectHeaders.get(EventConstants.SESSION_KEEP_ALIVE_HEADER));
            if (sessionKeepAliveMode != SessionKeepAliveMode.NONE && session == null) {
                ret = Future.failedFuture(
                        new AuthenticationException("A Vert.x session is required unless session keep alive mode is NONE"));
            } else {
                // The replyToId is generated server side so the client cannot pick a guessable
                // or colliding value
                if (connectedInfo.getReplyToId() == null) {
                    connectedInfo.setReplyToId(UUID.randomUUID().toString());
                }
                if (session != null) {
                    session.put(ConnectedInfo.SESSION_KEY, connectedInfo);
                }
                stompAuthorizer = services.stompAuthorizerFactory.create(connectedInfo);

                signalActivity();
                if (sessionKeepAliveMode == SessionKeepAliveMode.CONNECTION) {
                    // the upgrade response never reaches the store, so the connection's first write is this one
                    touchSession();
                    startSessionTouchTimer();
                }
                ret = Future.succeededFuture(Map.of(EventConstants.CONNECTED_INFO_HEADER,
                                                    services.jsonMapper.writeValueAsString(connectedInfo)));
            }
        } catch (JacksonException e) {
            ret = Future.failedFuture(e);
        } catch (IllegalArgumentException e) {
            ret = Future.failedFuture(new AuthenticationException("Invalid CONNECT frame", e));
        }
        return ret;
    }

    public void removeSession() {
        // a login session the client presented belongs to the browser, not to this connection
        if (sessionKeepAliveMode == SessionKeepAliveMode.NONE && session != null && sessionCreatedHere) {
            // The Vert.x SessionHandler deletes a destroyed session from the store when the response
            // ends. A WebSocket's response ended at the upgrade, so the store entry is removed here,
            // which keeps a reconnect within the timeout from resuming the session and its replyToId.
            services.sessionStore.delete(session.id())
                                 .onFailure(throwable -> log.warn("Session {} could not be removed from the store", session.id(), throwable));
            session.destroy();
        }
    }

    public Future<Void> send(Event<byte[]> incomingEvent) {
        signalActivity();

        if (incomingEvent.cri().scheme().equals(EventConstants.REPLY_DESTINATION_SCHEME)) {
            // A reply is a one-way delivery to the requester's reply destination. It is never invoked and
            // never itself replies, so no ack and no reply-to validation apply. A reply to an invocation this
            // client is serving is allowed for as long as the invocation is pending; one for an invocation
            // this connection no longer holds, such as a value crossing a cancel or a reply finished after a
            // reconnect, is dropped rather than ending the connection.
            if (outgoingInvocations.owesReply(incomingEvent)) {
                outgoingInvocations.observeReply(incomingEvent);
                services.eventBusService.send(incomingEvent);
            } else if (stompAuthorizer.sendAllowed(incomingEvent.cri())) {
                services.eventBusService.send(incomingEvent);
            } else {
                log.debug("Dropping reply to {} for an invocation this connection does not owe", incomingEvent.cri());
            }
            return Future.succeededFuture();
        }

        if (!stompAuthorizer.sendAllowed(incomingEvent.cri())) {
            return Future.failedFuture(new AuthorizationException("Not Authorized to send to " + incomingEvent.cri()));
        }

        if (incomingEvent.cri().scheme().equals(EventConstants.SERVICE_DESTINATION_SCHEME)) {

            try {

                incomingEvent.setSender(connectedInfo.getParticipant());

                // make sure reply-to if present is scoped to sender
                validateReplyToForServiceRequest(incomingEvent);

                String correlationId = incomingEvent.metadata().get(EventConstants.CORRELATION_ID_HEADER);
                incomingInvocations.track(incomingEvent);

                // A control event names a stream by correlation id and is published: the instance producing
                // the stream is whichever one took the request, and the others ignore an id they do not hold
                if (incomingEvent.metadata().contains(EventConstants.CONTROL_HEADER)) {
                    services.eventBusService.publish(incomingEvent);
                    return Future.succeededFuture();
                }

                return services.eventBusService
                        .sendWithAck(incomingEvent)
                        .onSuccess(nodeId -> incomingInvocations.pin(correlationId, nodeId, incomingEvent.cri()))
                        .recover(throwable -> {
                            // no reply will come for a request that never left
                            incomingInvocations.settle(correlationId);
                            throwable = KinoticUtil.mapSendFailure(throwable,
                                                                   incomingEvent.cri(),
                                                                   services.serviceDirectoryProvider.getIfAvailable());
                            try {
                                Event<byte[]> convertedEvent = services.exceptionConverter.convert(incomingEvent.metadata(), throwable);
                                // since we don't know the subscription id used by the stomp client for this request we send through the eventbus
                                services.eventBusService.send(convertedEvent);
                                return Future.succeededFuture();
                            } catch (Exception ex) {
                                if(log.isDebugEnabled()){
                                    log.debug("Exception occurred converting exception\n{}",
                                              EventUtil.toString(incomingEvent, true),
                                              throwable);
                                }
                                return Future.failedFuture(ex);
                            }
                        })
                        .mapEmpty();

            } catch (Exception e) {
                return Future.failedFuture(e);
            }

        } else if (incomingEvent.cri().scheme().equals(EventConstants.STREAM_DESTINATION_SCHEME)) {

            return services.eventStreamService.send(incomingEvent);

        } else {
            return Future.failedFuture(new IllegalArgumentException("CRI scheme not supported"));
        }
    }

    public void shutdown() {
        if (sessionTimer != -1) {
            services.vertx.cancelTimer(sessionTimer);
            sessionTimer = -1;
        }
        subscriptions.forEach((s, eventConsumer) -> eventConsumer.unregister());
        subscriptions.clear();
        incomingInvocations.dispose();
        outgoingInvocations.dispose();
        // a NONE session ends with its connection however the connection ended
        removeSession();
        session = null;
        connectedInfo = null;
        stompAuthorizer = null;
    }

    public void subscribe(CRI cri,
                          String subscriptionIdentifier,
                          StompSubscriptionHandler subscriptionHandler) {
        Validate.notNull(cri, "CRI must not be null");
        Validate.notEmpty(subscriptionIdentifier, "subscriptionIdentifier must not be empty");
        Validate.notNull(subscriptionHandler, "subscriptionHandler must not be null");

        signalActivity();

        if (!stompAuthorizer.subscribeAllowed(cri)) {
            throw new AuthorizationException("Not Authorized to subscribe to " + cri);
        }

        if (cri.scheme().equals(EventConstants.SERVICE_DESTINATION_SCHEME)) {

            EventConsumer eventConsumer = services.eventBusService.listen(cri);
            eventConsumer.handler(event -> {
                        // An invocation with a correlation id is pending until its terminal reply, and its
                        // replies are allowed for that long. Any other event carrying a reply-to gets one
                        // send to it: the reply-to was verified against the sender's replyToId when the
                        // event entered through send(), so it can only name the sender's own destination.
                        boolean pending = outgoingInvocations.deliver(event, subscriptionHandler);
                        String replyTo = event.metadata().get(EventConstants.REPLY_TO_HEADER);
                        // a control event is never answered, so its reply-to earns no grant
                        if (!pending && replyTo != null && !event.metadata().contains(EventConstants.CONTROL_HEADER)) {
                            // a wildcard could match destinations beyond the sender's own
                            if (!replyTo.contains("*")) {
                                stompAuthorizer.addTemporarySendAllowed(replyTo);
                            } else {
                                log.warn("reply-to header contains * and will NOT be ALLOWED for message {}",
                                         event);
                            }
                        }
                        subscriptionHandler.handleEvent(event);
                    })
                    .exceptionHandler(subscriptionHandler::handleError);

            replaceSubscription(subscriptionIdentifier, eventConsumer);

            log.debug("New Service Subscription cri: {} id: {} for login: {}",
                      cri.raw(),
                      subscriptionIdentifier,
                      connectedInfo.getParticipant());


        } else if (cri.scheme().equals(EventConstants.STREAM_DESTINATION_SCHEME)) {

            EventConsumer eventConsumer = services.eventStreamService.listen(cri);
            eventConsumer.handler(subscriptionHandler::handleEvent)
                         .exceptionHandler(subscriptionHandler::handleError);

            replaceSubscription(subscriptionIdentifier, eventConsumer);

            log.debug("New Event Subscription cri: {} id: {} for login: {}",
                      cri.raw(),
                      subscriptionIdentifier,
                      connectedInfo.getParticipant());

        } else if (cri.scheme().equals(EventConstants.REPLY_DESTINATION_SCHEME)) {

            incomingInvocations.subscribe(cri, subscriptionIdentifier, subscriptionHandler);

            log.debug("New Reply Subscription cri: {} id: {} for login: {}",
                      cri.raw(),
                      subscriptionIdentifier,
                      connectedInfo.getParticipant());

        } else {
            throw new IllegalArgumentException("CRI scheme not supported");
        }
    }

    public void unsubscribe(String subscriptionIdentifier) {
        Validate.notEmpty(subscriptionIdentifier, "subscriptionIdentifier must not be empty");

        signalActivity();

        if (!incomingInvocations.unsubscribe(subscriptionIdentifier)) {
            EventConsumer consumer = subscriptions.remove(subscriptionIdentifier);
            if (consumer != null) {
                consumer.unregister();
            } else {
                log.debug("No subscription exists for subscriptionIdentifier: {}", subscriptionIdentifier);
            }
        }
    }

    // a subscription id the client reuses ends the consumer it named before
    private void replaceSubscription(String subscriptionIdentifier, EventConsumer eventConsumer) {
        EventConsumer previous = subscriptions.put(subscriptionIdentifier, eventConsumer);
        if (previous != null) {
            previous.unregister();
        }
    }

    private Map<String, String> toCaseInsensitiveMap(MultiMap headers) {
        Map<String, String> ret = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        for (Map.Entry<String, String> entry : headers) {
            ret.put(entry.getKey(), entry.getValue());
        }
        return ret;
    }

    private ConnectedInfo connectedInfoFromSession() {
        if (session == null) {
            return null;
        }
        Object value = session.get(ConnectedInfo.SESSION_KEY);
        if (value instanceof ConnectedInfo storedConnectedInfo) {
            return storedConnectedInfo;
        }
        return null;
    }

    private void signalActivity() {
        if (sessionKeepAliveMode == SessionKeepAliveMode.ACTIVITY) {
            if (session == null) {
                log.error("Session is null while sessionKeepAliveMode is ACTIVITY");
                throw new IllegalStateException("Internal server error");
            }
            touchSession();
        }
    }

    // SessionHandler flushes a session to the store when a response ends, which on a WebSocket happened
    // once, at the upgrade. Every touch after that has to reach the store itself or the clustered entry
    // expires under the open connection. The write is rate limited to a fraction of the timeout, which
    // bounds how far the stored expiry lags a busy connection.
    private void touchSession() {
        session.setAccessed();
        long now = System.currentTimeMillis();
        if (now - lastSessionFlush >= services.apiGatewayProperties.getSessionTimeout() / 4) {
            lastSessionFlush = now;
            flushSession(session);
        }
    }

    // The stored copy is refreshed rather than the local one: it carries the version the store expects and
    // whatever a request on the same cookie has put since. A session the store no longer has, deleted by a
    // logout or expired, is never written back, because a put with no stored entry would re-create it.
    private void flushSession(Session flushed) {
        services.sessionStore.get(flushed.id()).onComplete(ar -> {
            if (session != flushed) {
                return;
            }
            if (ar.failed()) {
                log.warn("Session {} could not be read from the store", flushed.id(), ar.cause());
            } else if (ar.result() == null) {
                log.debug("Session {} is no longer in the store and is not refreshed", flushed.id());
            } else {
                Session stored = ar.result();
                session = stored;
                stored.setAccessed();
                services.sessionStore.put(stored)
                                     .onFailure(throwable -> log.warn("Session {} could not be refreshed in the store", stored.id(), throwable));
            }
        });
    }

    private void startSessionTouchTimer() {
        if (sessionTimer != -1) {
            log.error("Session-touch timer already started");
            throw new IllegalStateException("Internal server error");
        }
        // a quarter of the timeout, so a flush that fails is retried before the stored copy expires
        long sessionUpdateInterval = services.apiGatewayProperties.getSessionTimeout() / 4;
        sessionTimer = services.vertx.setPeriodic(sessionUpdateInterval, event -> {
            if (session == null) {
                log.error("Session is null while session-touch timer is active");
                throw new IllegalStateException("Internal server error");
            }
            touchSession();
        });
    }

    private void validateReplyToForServiceRequest(Event<byte[]> event) {
        String replyTo = event.metadata().get(EventConstants.REPLY_TO_HEADER);
        if (replyTo != null) {
            // reply-to must not use any * characters and must be "scoped" to the participant replyToId
            if (replyTo.contains("*")) {
                throw new IllegalArgumentException("reply-to header invalid * are not allowed for service requests");
            }

            CRI replyCRI;
            try {
                replyCRI = CRI.create(replyTo);
            } catch (Exception e) {
                throw new IllegalArgumentException("reply-to header invalid " + e.getMessage());
            }

            String scheme = replyCRI.scheme();
            if (scheme == null || !scheme.equals(EventConstants.REPLY_DESTINATION_SCHEME)) {
                throw new IllegalArgumentException("reply-to header invalid, scheme: " + scheme + " is not valid for service requests");
            }

            String scope = replyCRI.scope();
            if (scope != null) {
                // valid scopes are PARTICIPANT-REPLY_TO_ID:UUID or PARTICIPANT-REPLY_TO_ID
                int idx = scope.indexOf(":");
                if (idx != -1) {
                    scope = scope.substring(0, idx);
                }

                if (!scope.equals(connectedInfo.getReplyToId())) {
                    throw new IllegalArgumentException("reply-to header invalid, scope: " + scope + " is not valid for service requests");
                }
            } else {
                throw new IllegalArgumentException(
                        "reply-to header invalid, scope: null is not valid for service requests");
            }
        } else {
            throw new IllegalArgumentException("reply-to header invalid, not provided for service request");
        }
    }

}
