package org.kinotic.gateway.internal.endpoints.stomp;

import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.MultiMap;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.Session;
import io.vertx.ext.web.sstore.ClusteredSessionStore;
import io.vertx.spi.cluster.ignite.IgniteClusterManager;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.kinotic.core.api.config.KinoticProperties;
import org.kinotic.core.api.event.CRI;
import org.kinotic.core.api.event.Event;
import org.kinotic.core.api.event.EventBusService;
import org.kinotic.core.api.event.EventConstants;
import org.kinotic.core.api.event.EventConsumer;
import org.kinotic.core.api.event.ListenerStatus;
import org.kinotic.core.api.event.Metadata;
import org.kinotic.core.api.security.SecurityService;
import org.kinotic.core.api.service.RequestLivenessWatcher;
import org.kinotic.core.internal.api.service.json.JacksonExceptionConverter;
import org.kinotic.domain.api.model.security.participant.DefaultOrganizationParticipant;
import org.kinotic.gateway.api.config.ApiGatewayProperties;
import org.kinotic.gateway.internal.endpoints.Services;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.ObjectProvider;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;
import tools.jackson.databind.json.JsonMapper;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.endsWith;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Pins what a STOMP connection's caller side owes its client: a request whose serving node leaves the
 * cluster is answered on the client's reply destination with the typed error, a reply that settles a
 * request releases it, a session under an open connection outlives its timeout, and a connection that closes
 * answers every invocation still outstanding on the services it published, and a stream whose requester is
 * gone is cancelled on the connection producing it.
 *
 * Created by Navíd Mitchell 🤪 on 9/9/26.
 */
public class EndpointConnectionHandlerTests {

    private static final long SESSION_TIMEOUT_MS = 1000;
    private static final String SERVICE_DESTINATION = "srv://app.acme-org.orders-app~OrderService/create#1.0.0";

    private static Ignite ignite;
    private static Vertx vertx;
    private Services services;
    private EventBusService eventBusService;
    private RequestLivenessWatcher requestLivenessWatcher;
    private EventConsumer replyConsumer;
    private final AtomicReference<Handler<Event<byte[]>>> replyDelivery = new AtomicReference<>();

    // The clustered session store needs a clustered Vert.x; one Ignite node on ports of its own, so a
    // core test JVM running alongside never joins it
    @BeforeAll
    public static void startCluster() throws Exception {
        IgniteConfiguration configuration = new IgniteConfiguration()
                .setIgniteInstanceName("gateway-endpoint-tests")
                .setMetricsLogFrequency(0)
                .setWorkDirectory("/tmp/ignite-gateway-tests")
                .setDiscoverySpi(new TcpDiscoverySpi().setLocalPort(48500)
                                                      .setIpFinder(new TcpDiscoveryVmIpFinder().setAddresses(List.of("127.0.0.1:48500"))))
                .setCommunicationSpi(new TcpCommunicationSpi().setLocalPort(48100));
        ignite = Ignition.start(configuration);
        vertx = Vertx.builder()
                     .withClusterManager(new IgniteClusterManager(ignite))
                     .buildClustered()
                     .toCompletionStage().toCompletableFuture().get(2, TimeUnit.MINUTES);
    }

    @AfterAll
    public static void stopCluster() throws Exception {
        vertx.close().toCompletionStage().toCompletableFuture().get(1, TimeUnit.MINUTES);
        ignite.close();
    }

    @BeforeEach
    @SuppressWarnings("unchecked")
    public void setUp() {
        eventBusService = mock(EventBusService.class);
        requestLivenessWatcher = mock(RequestLivenessWatcher.class);

        // the connection's reply subscription, with the handler the gateway installs captured for delivery
        replyConsumer = mock(EventConsumer.class);
        when(replyConsumer.handler(any())).thenAnswer(invocation -> {
            replyDelivery.set(invocation.getArgument(0));
            return replyConsumer;
        });
        when(replyConsumer.exceptionHandler(any())).thenReturn(replyConsumer);
        when(eventBusService.listen(any())).thenReturn(replyConsumer);

        JsonMapper jsonMapper = JsonMapper.builder().build();
        services = new Services();
        services.vertx = vertx;
        services.apiGatewayProperties = new ApiGatewayProperties();
        services.apiGatewayProperties.setSessionTimeout(SESSION_TIMEOUT_MS);
        services.jsonMapper = jsonMapper;
        services.exceptionConverter = new JacksonExceptionConverter(new KinoticProperties(), jsonMapper);
        services.stompAuthorizerFactory = new StompAuthorizerFactory();
        services.securityService = mock(SecurityService.class);
        when(services.securityService.authenticate(any())).thenReturn(Future.succeededFuture(participant()));
        services.eventBusService = eventBusService;
        services.requestLivenessWatcher = requestLivenessWatcher;
        services.serviceDirectoryProvider = mock(ObjectProvider.class);
        services.sessionStore = ClusteredSessionStore.create(vertx);
    }

    @Test
    public void testLostNodeFailsTheRequestOnTheReplyDestination() throws Exception {
        EndpointConnectionHandler handler = connect(Map.of());
        String replyTo = subscribeReplies(handler);

        handler.send(request(replyTo, "corr-1")).toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS);

        ArgumentCaptor<Runnable> onLost = ArgumentCaptor.forClass(Runnable.class);
        verify(requestLivenessWatcher).watch(endsWith(":corr-1"), eq("node-2"), onLost.capture());
        onLost.getValue().run();

        ArgumentCaptor<Event<byte[]>> sent = ArgumentCaptor.forClass(Event.class);
        verify(eventBusService).send(sent.capture());
        Event<byte[]> errorReply = sent.getValue();
        Assertions.assertEquals(replyTo, errorReply.cri().raw());
        Assertions.assertEquals("corr-1", errorReply.metadata().get(EventConstants.CORRELATION_ID_HEADER));
        Assertions.assertTrue(errorReply.metadata().get(EventConstants.ERROR_HEADER).contains("node-2"));
        Assertions.assertTrue(new String(errorReply.data(), StandardCharsets.UTF_8).contains("RpcServiceUnavailableException"));
    }

    @Test
    public void testTerminalReplySettlesTheRequest() throws Exception {
        EndpointConnectionHandler handler = connect(Map.of());
        String replyTo = subscribeReplies(handler);
        List<Event<byte[]>> delivered = new ArrayList<>();
        // the subscription handler the test installed receives what the reply consumer delivers
        handler.send(request(replyTo, "corr-2")).toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS);
        ArgumentCaptor<Runnable> onLost = ArgumentCaptor.forClass(Runnable.class);
        verify(requestLivenessWatcher).watch(endsWith(":corr-2"), eq("node-2"), onLost.capture());

        Metadata replyMetadata = Metadata.create(Map.of(EventConstants.CORRELATION_ID_HEADER, "corr-2",
                                                        EventConstants.CONTROL_HEADER, EventConstants.CONTROL_VALUE_COMPLETE));
        replyDelivery.get().handle(Event.create(CRI.create(replyTo), replyMetadata, new byte[0]));

        verify(requestLivenessWatcher).settle(endsWith(":corr-2"));
        // a node loss reported after the reply has nothing left to answer
        onLost.getValue().run();
        verify(eventBusService, never()).send(any());
    }

    @Test
    public void testShutdownSettlesEveryOutstandingRequest() throws Exception {
        EndpointConnectionHandler handler = connect(Map.of());
        String replyTo = subscribeReplies(handler);
        handler.send(request(replyTo, "corr-3")).toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS);

        handler.shutdown();

        verify(requestLivenessWatcher).settle(endsWith(":corr-3"));
        verify(replyConsumer).unregister();
    }

    @Test
    public void testSessionOutlivesItsTimeoutUnderAnOpenConnection() throws Exception {
        Session session = services.sessionStore.createSession(SESSION_TIMEOUT_MS);
        services.sessionStore.put(session).toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS);

        EndpointConnectionHandler handler = connect(session, Map.of(EventConstants.SESSION_KEEP_ALIVE_HEADER, "CONNECTION"));
        Thread.sleep(SESSION_TIMEOUT_MS * 5 / 2);
        Assertions.assertNotNull(storedSession(session.id()), "the session expired under an open connection");

        // the last store write happened at most one timeout before the connection closed
        handler.shutdown();
        Thread.sleep(SESSION_TIMEOUT_MS * 3 / 2);
        Assertions.assertNull(storedSession(session.id()), "the session outlived its timeout after the connection closed");
    }

    @Test
    public void testClosedConnectionFailsTheInvocationsItStillOwes() throws Exception {
        EndpointConnectionHandler handler = connect(Map.of());
        String requester = EventConstants.REPLY_DESTINATION_SCHEME + "://other:replies@kinotic.js.EventBus/replyHandler";
        deliverInvocation(handler, requester, "inv-1");

        handler.shutdown();

        ArgumentCaptor<Event<byte[]>> sent = ArgumentCaptor.forClass(Event.class);
        verify(eventBusService).send(sent.capture());
        Event<byte[]> errorReply = sent.getValue();
        Assertions.assertEquals(requester, errorReply.cri().raw());
        Assertions.assertEquals("inv-1", errorReply.metadata().get(EventConstants.CORRELATION_ID_HEADER));
        Assertions.assertTrue(new String(errorReply.data(), StandardCharsets.UTF_8).contains("RpcServiceUnavailableException"));
    }

    @Test
    public void testTerminalReplyFromTheConnectionSettlesTheInvocation() throws Exception {
        EndpointConnectionHandler handler = connect(Map.of());
        String requester = EventConstants.REPLY_DESTINATION_SCHEME + "://other:replies@kinotic.js.EventBus/replyHandler";
        deliverInvocation(handler, requester, "inv-2");

        // the service instance answers through the connection; the reply passes back on the reply scheme
        Metadata replyMetadata = Metadata.create(Map.of(EventConstants.CORRELATION_ID_HEADER, "inv-2",
                                                        EventConstants.CONTROL_HEADER, EventConstants.CONTROL_VALUE_COMPLETE));
        handler.send(Event.create(CRI.create(requester), replyMetadata, new byte[0])).toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS);
        handler.shutdown();

        // the forwarded reply is the only send; no error follows it
        ArgumentCaptor<Event<byte[]>> sent = ArgumentCaptor.forClass(Event.class);
        verify(eventBusService).send(sent.capture());
        Assertions.assertFalse(sent.getValue().metadata().contains(EventConstants.ERROR_HEADER));
    }

    @Test
    public void testRequesterGoneCancelsTheStreamOnTheConnection() throws Exception {
        Sinks.Many<ListenerStatus> requesterStatus = Sinks.many().replay().latest();
        requesterStatus.tryEmitNext(ListenerStatus.ACTIVE);
        when(eventBusService.monitorListenerStatus(any())).thenReturn(requesterStatus.asFlux());
        EndpointConnectionHandler handler = connect(Map.of());
        String requester = EventConstants.REPLY_DESTINATION_SCHEME + "://other:replies@kinotic.js.EventBus/replyHandler";
        List<Event<byte[]>> delivered = deliverInvocation(handler, requester, "inv-3");

        // a value without the completion marker is what makes the invocation a stream
        Metadata valueMetadata = Metadata.create(Map.of(EventConstants.CORRELATION_ID_HEADER, "inv-3"));
        handler.send(Event.create(CRI.create(requester), valueMetadata, new byte[0])).toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS);
        ArgumentCaptor<CRI> watched = ArgumentCaptor.forClass(CRI.class);
        verify(eventBusService).monitorListenerStatus(watched.capture());
        Assertions.assertEquals(requester, watched.getValue().raw());

        // the requester's answer is written on the connection's context, after the cancel frame
        CountDownLatch answered = new CountDownLatch(1);
        doAnswer(_ -> { answered.countDown(); return null; }).when(eventBusService).send(any());
        requesterStatus.tryEmitNext(ListenerStatus.INACTIVE);
        Assertions.assertTrue(answered.await(5, TimeUnit.SECONDS), "the requester was never answered");
        Assertions.assertEquals(2, delivered.size(), "no cancel reached the connection");
        Event<byte[]> cancel = delivered.get(1);
        Assertions.assertEquals(SERVICE_DESTINATION, cancel.cri().raw());
        Assertions.assertEquals(EventConstants.CONTROL_VALUE_CANCEL, cancel.metadata().get(EventConstants.CONTROL_HEADER));
        Assertions.assertEquals("inv-3", cancel.metadata().get(EventConstants.CORRELATION_ID_HEADER));

        // the requester is answered with the typed error, so one whose registration this node had not seen
        // yet learns the stream is over; the cancelled invocation is no longer owed, so the close adds nothing
        handler.shutdown();
        ArgumentCaptor<Event<byte[]>> sent = ArgumentCaptor.forClass(Event.class);
        verify(eventBusService, times(2)).send(sent.capture());
        Event<byte[]> answer = sent.getAllValues().get(1);
        Assertions.assertEquals(requester, answer.cri().raw());
        Assertions.assertEquals("inv-3", answer.metadata().get(EventConstants.CORRELATION_ID_HEADER));
        Assertions.assertNotNull(answer.metadata().get(EventConstants.ERROR_HEADER));
    }

    @Test
    public void testEveryReplyOfAPendingInvocationIsAllowed() throws Exception {
        when(eventBusService.monitorListenerStatus(any())).thenReturn(Flux.just(ListenerStatus.ACTIVE));
        EndpointConnectionHandler handler = connect(Map.of());
        String requester = EventConstants.REPLY_DESTINATION_SCHEME + "://other:replies@kinotic.js.EventBus/replyHandler";
        deliverInvocation(handler, requester, "inv-4");

        // two stream values, then the completion
        for (int i = 0; i < 2; i++) {
            Metadata valueMetadata = Metadata.create(Map.of(EventConstants.CORRELATION_ID_HEADER, "inv-4"));
            handler.send(Event.create(CRI.create(requester), valueMetadata, new byte[0])).toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS);
        }
        Metadata completion = Metadata.create(Map.of(EventConstants.CORRELATION_ID_HEADER, "inv-4",
                                                     EventConstants.CONTROL_HEADER, EventConstants.CONTROL_VALUE_COMPLETE));
        handler.send(Event.create(CRI.create(requester), completion, new byte[0])).toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS);
        verify(eventBusService, times(3)).send(any());

        // the invocation is over: a further reply to the requester is dropped, and the connection lives on
        Metadata late = Metadata.create(Map.of(EventConstants.CORRELATION_ID_HEADER, "inv-4"));
        Assertions.assertTrue(handler.send(Event.create(CRI.create(requester), late, new byte[0])).succeeded());
        verify(eventBusService, times(3)).send(any());
    }

    @Test
    public void testControlEventEarnsNoReplyGrant() throws Exception {
        EndpointConnectionHandler handler = connect(Map.of());
        String requester = EventConstants.REPLY_DESTINATION_SCHEME + "://other:replies@kinotic.js.EventBus/replyHandler";
        List<Event<byte[]>> delivered = new ArrayList<>();
        handler.subscribe(CRI.create("srv://app.acme-org.orders-app~OrderService#1.0.0"), "svc-1", new StompSubscriptionHandler() {
            @Override
            public void handleEvent(Event<byte[]> event) {
                delivered.add(event);
            }

            @Override
            public void handleError(Throwable throwable) {}
        });
        // a requester's cancel names a stream this connection never held; it carries a reply-to like every request
        Metadata cancel = Metadata.create(Map.of(EventConstants.REPLY_TO_HEADER, requester,
                                                 EventConstants.CORRELATION_ID_HEADER, "inv-5",
                                                 EventConstants.CONTROL_HEADER, EventConstants.CONTROL_VALUE_CANCEL));
        replyDelivery.get().handle(Event.create(CRI.create(SERVICE_DESTINATION), cancel, null));
        Assertions.assertEquals(1, delivered.size());

        // nothing is owed to that requester and the cancel granted nothing, so a reply to it goes nowhere
        Metadata reply = Metadata.create(Map.of(EventConstants.CORRELATION_ID_HEADER, "inv-5"));
        Assertions.assertTrue(handler.send(Event.create(CRI.create(requester), reply, new byte[0])).succeeded());
        verify(eventBusService, never()).send(any());
    }

    @Test
    public void testClientCancelIsPublishedToEveryInstance() throws Exception {
        EndpointConnectionHandler handler = connect(Map.of());
        String replyTo = subscribeReplies(handler);
        handler.send(request(replyTo, "inv-6")).toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS);

        Metadata metadata = Metadata.create(Map.of(EventConstants.REPLY_TO_HEADER, replyTo,
                                                   EventConstants.CORRELATION_ID_HEADER, "inv-6",
                                                   EventConstants.CONTROL_HEADER, EventConstants.CONTROL_VALUE_CANCEL));
        handler.send(Event.create(CRI.create(SERVICE_DESTINATION), metadata, null)).toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS);

        // the request went to one instance; the cancel reaches all of them and settles the lease
        verify(eventBusService, times(1)).sendWithAck(any());
        ArgumentCaptor<Event<byte[]>> published = ArgumentCaptor.forClass(Event.class);
        verify(eventBusService).publish(published.capture());
        Assertions.assertEquals(EventConstants.CONTROL_VALUE_CANCEL, published.getValue().metadata().get(EventConstants.CONTROL_HEADER));
        verify(requestLivenessWatcher).settle(endsWith(":inv-6"));
    }

    @Test
    public void testLeasesAreQualifiedPerConnection() throws Exception {
        EndpointConnectionHandler first = connect(Map.of());
        EndpointConnectionHandler second = connect(Map.of());
        String firstReplyTo = subscribeReplies(first);
        String secondReplyTo = subscribeReplies(second);

        // two clients on one gateway using the same correlation id
        first.send(request(firstReplyTo, "dup")).toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS);
        second.send(request(secondReplyTo, "dup")).toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS);
        ArgumentCaptor<String> leases = ArgumentCaptor.forClass(String.class);
        verify(requestLivenessWatcher, times(2)).watch(leases.capture(), eq("node-2"), any());
        Assertions.assertNotEquals(leases.getAllValues().get(0), leases.getAllValues().get(1));

        // the first client's terminal reply settles its own lease only
        Metadata completion = Metadata.create(Map.of(EventConstants.CORRELATION_ID_HEADER, "dup",
                                                     EventConstants.CONTROL_HEADER, EventConstants.CONTROL_VALUE_COMPLETE));
        replyDelivery.get().handle(Event.create(CRI.create(secondReplyTo), completion, new byte[0]));
        verify(requestLivenessWatcher, times(1)).settle(any());
        verify(requestLivenessWatcher).settle(leases.getAllValues().get(1));
    }

    @Test
    public void testReusedSubscriptionIdEndsThePreviousConsumer() throws Exception {
        EndpointConnectionHandler handler = connect(Map.of());
        subscribeReplies(handler);
        subscribeReplies(handler);
        verify(replyConsumer, times(1)).unregister();
    }

    @Test
    public void testConnectionKeepAliveTouchesTheSessionAtConnect() throws Exception {
        Session session = services.sessionStore.createSession(SESSION_TIMEOUT_MS * 10);
        services.sessionStore.put(session).toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS);
        long storedBefore = storedSession(session.id()).lastAccessed();
        Thread.sleep(20);

        connect(session, Map.of(EventConstants.SESSION_KEEP_ALIVE_HEADER, "CONNECTION"));
        long deadline = System.currentTimeMillis() + 5000;
        while (storedSession(session.id()).lastAccessed() == storedBefore && System.currentTimeMillis() < deadline) {
            Thread.sleep(50);
        }
        Assertions.assertTrue(storedSession(session.id()).lastAccessed() > storedBefore, "the connect never reached the store");
    }

    @Test
    public void testSessionDeletedFromTheStoreIsNotRecreatedByTheTouch() throws Exception {
        Session session = services.sessionStore.createSession(SESSION_TIMEOUT_MS * 10);
        services.sessionStore.put(session).toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS);
        EndpointConnectionHandler handler = connect(session, Map.of());
        // connect() touched the session; let that write land before the delete
        Thread.sleep(300);

        // a logout deletes the session; the connection's next touch must not put it back
        services.sessionStore.delete(session.id()).toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS);
        Thread.sleep(SESSION_TIMEOUT_MS / 4 + 50);
        handler.unsubscribe("none");
        Thread.sleep(500);
        Assertions.assertNull(storedSession(session.id()), "the touch re-created a deleted session");
    }

    @Test
    public void testNoneKeepAliveSessionEndsWithTheConnection() throws Exception {
        Session session = services.sessionStore.createSession(SESSION_TIMEOUT_MS * 10);
        services.sessionStore.put(session).toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS);

        EndpointConnectionHandler handler = connect(session, Map.of(EventConstants.SESSION_KEEP_ALIVE_HEADER, "NONE"));
        Assertions.assertNotNull(storedSession(session.id()));

        // a network close, not a DISCONNECT frame: the session must not outlive the connection either way
        handler.shutdown();
        long deadline = System.currentTimeMillis() + 5000;
        while (storedSession(session.id()) != null && System.currentTimeMillis() < deadline) {
            Thread.sleep(50);
        }
        Assertions.assertNull(storedSession(session.id()), "a NONE session survived its connection");
    }

    // Subscribes the connection to a service address it publishes and delivers one invocation to it; returns
    // everything the subscription delivers to the connection, the invocation first
    private List<Event<byte[]>> deliverInvocation(EndpointConnectionHandler handler, String replyTo, String correlationId) {
        List<Event<byte[]>> delivered = new ArrayList<>();
        handler.subscribe(CRI.create("srv://app.acme-org.orders-app~OrderService#1.0.0"), "svc-1", new StompSubscriptionHandler() {
            @Override
            public void handleEvent(Event<byte[]> event) {
                delivered.add(event);
            }

            @Override
            public void handleError(Throwable throwable) {}
        });
        Metadata metadata = Metadata.create(Map.of(EventConstants.REPLY_TO_HEADER, replyTo,
                                                   EventConstants.CORRELATION_ID_HEADER, correlationId));
        replyDelivery.get().handle(Event.create(CRI.create(SERVICE_DESTINATION), metadata, new byte[0]));
        return delivered;
    }

    private EndpointConnectionHandler connect(Map<String, String> connectHeaders) throws Exception {
        return connect(services.sessionStore.createSession(SESSION_TIMEOUT_MS), connectHeaders);
    }

    private EndpointConnectionHandler connect(Session session, Map<String, String> connectHeaders) throws Exception {
        HttpServerRequest request = mock(HttpServerRequest.class);
        when(request.headers()).thenReturn(MultiMap.caseInsensitiveMultiMap());
        RoutingContext routingContext = mock(RoutingContext.class);
        when(routingContext.session()).thenReturn(session);
        when(routingContext.request()).thenReturn(request);

        EndpointConnectionHandler handler = new EndpointConnectionHandler(services);
        handler.handshake(routingContext).toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS);
        handler.connect(connectHeaders).toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS);
        return handler;
    }

    // Subscribes the connection's reply destination and returns its CRI; requests carry it as reply-to
    private String subscribeReplies(EndpointConnectionHandler handler) throws Exception {
        String replyTo = EventConstants.REPLY_DESTINATION_SCHEME + "://" + replyToId(handler) + ":replies@kinotic.js.EventBus/replyHandler";
        handler.subscribe(CRI.create(replyTo), "sub-1", new StompSubscriptionHandler() {
            @Override
            public void handleEvent(Event<byte[]> event) {}

            @Override
            public void handleError(Throwable throwable) {}
        });
        return replyTo;
    }

    private String replyToId(EndpointConnectionHandler handler) throws Exception {
        // connect() answers the CONNECT frame with the connected info, whose replyToId scopes every reply destination
        Map<String, String> connected = handler.connect(Map.of()).toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS);
        return services.jsonMapper.readTree(connected.get(EventConstants.CONNECTED_INFO_HEADER)).get("replyToId").asString();
    }

    private Event<byte[]> request(String replyTo, String correlationId) {
        when(eventBusService.sendWithAck(any())).thenReturn(Future.succeededFuture("node-2"));
        Metadata metadata = Metadata.create(Map.of(EventConstants.REPLY_TO_HEADER, replyTo,
                                                   EventConstants.CORRELATION_ID_HEADER, correlationId));
        return Event.create(CRI.create(SERVICE_DESTINATION), metadata, new byte[0]);
    }

    private Session storedSession(String id) throws Exception {
        return services.sessionStore.get(id).toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS);
    }

    // An organization participant may both call and publish services in its own application zones
    private static DefaultOrganizationParticipant participant() {
        return DefaultOrganizationParticipant.builder()
                                             .id("org-user")
                                             .organizationId("acme-org")
                                             .metadata(Map.of())
                                             .roles(List.of())
                                             .build();
    }
}
