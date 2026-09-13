package org.kinotic.core.internal.api;

import io.vertx.core.Context;
import io.vertx.core.Vertx;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.logger.slf4j.Slf4jLogger;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.kinotic.core.api.RpcServiceProxyHandle;
import org.kinotic.core.api.ServiceRegistry;
import org.kinotic.core.api.event.Event;
import org.kinotic.core.api.event.EventConstants;
import org.kinotic.core.api.exceptions.RpcServiceUnavailableException;
import org.kinotic.core.api.security.Participant;
import org.kinotic.core.api.security.SecurityContext;
import org.kinotic.core.api.service.RequestLivenessWatcher;
import org.kinotic.core.api.service.ServiceIdentifier;
import org.kinotic.core.internal.KinoticIgniteClusterManager;
import org.kinotic.core.internal.api.event.EventMessageCodec;
import org.kinotic.core.internal.api.support.DrainTestService;
import org.kinotic.core.internal.api.support.NonExistentServiceProxy;
import org.kinotic.core.internal.utils.MetaUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.test.StepVerifier;
import tools.jackson.databind.json.JsonMapper;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * Pins the liveness contract of an in-cluster caller: a call pinned to a node that leaves the cluster fails,
 * a call in flight on a service that unregisters still replies, and a stream cut by its service's stop ends
 * with an error rather than silence.
 *
 * Created by Navíd Mitchell 🤪 on 9/9/26.
 */
@SpringBootTest
@ActiveProfiles({"test"})
public class RpcLivenessTests {

    private static final String PARTICIPANT_ID = "test-participant";

    @Autowired
    private ServiceRegistry serviceRegistry;
    @Autowired
    private RequestLivenessWatcher requestLivenessWatcher;
    @SuppressWarnings("SpringJavaInjectionPointsAutowiringInspection") // these are not detected because continuum wires them..
    @Autowired
    private NonExistentServiceProxy nonExistentServiceProxy;
    @Autowired
    private Vertx vertx;
    @Autowired
    private SecurityContext securityContext;
    @Autowired
    private JsonMapper jsonMapper;

    private Ignite secondIgnite;
    private Vertx secondVertx;

    @AfterEach
    public void stopSecondNode() throws Exception {
        if(secondVertx != null){
            secondVertx.close().toCompletionStage().toCompletableFuture().get(1, TimeUnit.MINUTES);
            secondVertx = null;
        }
        if(secondIgnite != null){
            secondIgnite.close();
            secondIgnite = null;
        }
    }

    @Test
    public void testCallPinnedToDepartedNodeFails() throws Exception {
        // The second node hosts the address nobody in this JVM serves: it acknowledges every request with
        // its node id, which pins the call to it, and never replies
        KinoticIgniteClusterManager secondClusterManager = startSecondNode();
        String address = new ServiceIdentifier(MetaUtil.getZone(NonExistentServiceProxy.class),
                                               "com.namespace",
                                               "NonExistentService",
                                               null,
                                               "1.1.0").cri().baseResource();
        CountDownLatch received = new CountDownLatch(1);
        secondVertx.eventBus().<Event<byte[]>>consumer(address, message -> {
                       message.reply(secondClusterManager.getNodeId());
                       received.countDown();
                   })
                   .completion().toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);

        CompletableFuture<Void> outcome = new CompletableFuture<>();
        withParticipant(() -> {
            nonExistentServiceProxy.probablyNotHome().subscribe(v -> {}, outcome::completeExceptionally, () -> outcome.complete(null));
            return null;
        });
        Assertions.assertTrue(received.await(10, TimeUnit.SECONDS), "the second node never received the request");
        awaitPendingCount(1);

        stopSecondNode();

        Exception failure = Assertions.assertThrows(Exception.class, () -> outcome.get(1, TimeUnit.MINUTES));
        Assertions.assertInstanceOf(RpcServiceUnavailableException.class, failure.getCause());
        awaitPendingCount(0);
    }

    @Test
    public void testLeaseSurvivesASiblingSettlingOnTheSameNode() throws Exception {
        KinoticIgniteClusterManager secondClusterManager = startSecondNode();
        String nodeId = secondClusterManager.getNodeId();
        CompletableFuture<Void> lost = new CompletableFuture<>();
        withParticipant(() -> {
            requestLivenessWatcher.watch("first", nodeId, () -> {});
            requestLivenessWatcher.watch("second", nodeId, () -> lost.complete(null));
            return null;
        });
        awaitPendingCount(2);

        // settling the first must not take the node's index entry away from the second
        requestLivenessWatcher.settle("first");
        awaitPendingCount(1);
        stopSecondNode();

        lost.get(1, TimeUnit.MINUTES);
        awaitPendingCount(0);
    }

    @Test
    public void testInFlightCallRepliesAfterUnregister() throws Exception {
        GatedDrainTestService service = new GatedDrainTestService();
        ServiceIdentifier serviceIdentifier = register(service);
        RpcServiceProxyHandle<DrainTestService> handle = serviceRegistry.serviceProxy(serviceIdentifier, DrainTestService.class);
        try {
            CompletableFuture<String> reply = withParticipant(() -> handle.getService().awaitGate().toFuture());
            Assertions.assertTrue(service.gateReached.await(10, TimeUnit.SECONDS), "the call never reached the service");

            // stop() holds its completion until the invocation in flight has replied
            CompletableFuture<Void> unregistered = serviceRegistry.unregister(serviceIdentifier).toCompletionStage().toCompletableFuture();
            Assertions.assertThrows(java.util.concurrent.TimeoutException.class, () -> unregistered.get(500, TimeUnit.MILLISECONDS));
            Assertions.assertFalse(reply.isDone());

            service.gate.complete("drained");
            Assertions.assertEquals("drained", reply.get(10, TimeUnit.SECONDS));
            unregistered.get(10, TimeUnit.SECONDS);
        } finally {
            handle.release();
        }
    }

    @Test
    public void testStreamEndsWithUnavailableWhenServiceStops() throws Exception {
        GatedDrainTestService service = new GatedDrainTestService();
        ServiceIdentifier serviceIdentifier = register(service);
        RpcServiceProxyHandle<DrainTestService> handle = serviceRegistry.serviceProxy(serviceIdentifier, DrainTestService.class);
        try {
            Flux<String> stream = withParticipant(() -> handle.getService().streamUntilStopped());
            StepVerifier.create(stream)
                        .expectNext("first")
                        .then(() -> serviceRegistry.unregister(serviceIdentifier))
                        .expectError(RpcServiceUnavailableException.class)
                        .verify(Duration.ofSeconds(30));
        } finally {
            handle.release();
        }
    }

    @Test
    public void testCancelReachesTheInstanceProducingTheStream() throws Exception {
        GatedDrainTestService service = new GatedDrainTestService();
        ServiceIdentifier serviceIdentifier = register(service);
        RpcServiceProxyHandle<DrainTestService> handle = serviceRegistry.serviceProxy(serviceIdentifier, DrainTestService.class);
        try {
            Flux<String> stream = withParticipant(() -> handle.getService().streamUntilStopped());
            CountDownLatch firstValue = new CountDownLatch(1);
            Disposable subscription = stream.subscribe(_ -> firstValue.countDown());
            Assertions.assertTrue(firstValue.await(10, TimeUnit.SECONDS), "the stream never produced");

            // a second instance of the service, joining after the stream started: a cancel sent to the shared
            // address could land here instead of on the instance producing the stream
            CountDownLatch cancelSeenBySibling = new CountDownLatch(1);
            vertx.eventBus().<Event<byte[]>>consumer(serviceIdentifier.cri().baseResource(), message -> {
                     if(EventConstants.CONTROL_VALUE_CANCEL.equals(message.body().metadata().get(EventConstants.CONTROL_HEADER))){
                         cancelSeenBySibling.countDown();
                     }
                 })
                 .completion().toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);

            subscription.dispose();
            Assertions.assertTrue(cancelSeenBySibling.await(10, TimeUnit.SECONDS), "the sibling instance never saw the cancel");
            Assertions.assertTrue(service.streamCancelled.await(10, TimeUnit.SECONDS), "the producing instance never saw the cancel");
        } finally {
            handle.release();
            serviceRegistry.unregister(serviceIdentifier).toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testLocalCallSurvivesUnrelatedNodeDeparture() throws Exception {
        startSecondNode();
        GatedDrainTestService service = new GatedDrainTestService();
        ServiceIdentifier serviceIdentifier = register(service);
        RpcServiceProxyHandle<DrainTestService> handle = serviceRegistry.serviceProxy(serviceIdentifier, DrainTestService.class);
        try {
            CompletableFuture<String> reply = withParticipant(() -> handle.getService().awaitGate().toFuture());
            Assertions.assertTrue(service.gateReached.await(10, TimeUnit.SECONDS), "the call never reached the service");
            awaitPendingCount(1);

            // a membership change that does not involve the node the call is pinned to leaves it pending
            stopSecondNode();
            Assertions.assertFalse(reply.isDone());
            Assertions.assertEquals(1, requestLivenessWatcher.pendingCount());

            service.gate.complete("survived");
            Assertions.assertEquals("survived", reply.get(10, TimeUnit.SECONDS));
            awaitPendingCount(0);
        } finally {
            handle.release();
            serviceRegistry.unregister(serviceIdentifier).toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
        }
    }

    private ServiceIdentifier register(DrainTestService service) throws Exception {
        ServiceIdentifier serviceIdentifier = new ServiceIdentifier(null,
                                                                    "org.kinotic.tests",
                                                                    "DrainTestService-" + System.nanoTime(),
                                                                    null,
                                                                    "1.0.0");
        serviceRegistry.register(serviceIdentifier, DrainTestService.class, service)
                       .toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
        return serviceIdentifier;
    }

    private KinoticIgniteClusterManager startSecondNode() throws Exception {
        // Joins the test node through its discovery port; every local port defaults to the next free one
        // in its range, so both nodes fit in one JVM
        IgniteConfiguration configuration = new IgniteConfiguration()
                .setIgniteInstanceName("rpc-liveness-second-node")
                .setGridLogger(new Slf4jLogger())
                .setMetricsLogFrequency(0)
                .setWorkDirectory("/tmp/ignite-second-node")
                .setDiscoverySpi(new TcpDiscoverySpi().setIpFinder(new TcpDiscoveryVmIpFinder().setAddresses(List.of("127.0.0.1:47500..47509"))))
                .setCommunicationSpi(new TcpCommunicationSpi());
        secondIgnite = Ignition.start(configuration);
        KinoticIgniteClusterManager clusterManager = new KinoticIgniteClusterManager(secondIgnite);
        secondVertx = Vertx.builder()
                           .withClusterManager(clusterManager)
                           .buildClustered()
                           .toCompletionStage().toCompletableFuture().get(2, TimeUnit.MINUTES);
        secondVertx.eventBus().registerCodec(new EventMessageCodec(jsonMapper));
        secondVertx.eventBus().codecSelector(body -> body instanceof Event ? EventMessageCodec.NAME : null);
        return clusterManager;
    }

    private void awaitPendingCount(int expected) throws Exception {
        long deadline = System.currentTimeMillis() + 10_000;
        while(requestLivenessWatcher.pendingCount() != expected && System.currentTimeMillis() < deadline){
            Thread.sleep(20);
        }
        Assertions.assertEquals(expected, requestLivenessWatcher.pendingCount());
    }

    // The proxy captures the sender from the current context, so the call is made on a context with a participant
    private <T> T withParticipant(Supplier<T> proxyCall) throws Exception {
        Context context = vertx.getOrCreateContext();
        securityContext.setParticipant(context, new Participant() {
            @Override
            public String getId() { return PARTICIPANT_ID; }
            @Override
            public Map<String, String> getMetadata() { return Map.of(); }
            @Override
            public List<String> getRoles() { return List.of(); }
        });
        CompletableFuture<T> future = new CompletableFuture<>();
        context.runOnContext(_ -> {
            try {
                future.complete(proxyCall.get());
            } catch (Throwable t) {
                future.completeExceptionally(t);
            }
        });
        return future.get(10, TimeUnit.SECONDS);
    }

    private static class GatedDrainTestService implements DrainTestService {
        final CompletableFuture<String> gate = new CompletableFuture<>();
        final CountDownLatch gateReached = new CountDownLatch(1);
        final CountDownLatch streamCancelled = new CountDownLatch(1);

        @Override
        public Mono<String> awaitGate() {
            gateReached.countDown();
            return Mono.fromFuture(gate);
        }

        @Override
        public Flux<String> streamUntilStopped() {
            Sinks.Many<String> sink = Sinks.many().unicast().onBackpressureBuffer();
            sink.tryEmitNext("first");
            return sink.asFlux().doOnCancel(streamCancelled::countDown);
        }
    }
}
