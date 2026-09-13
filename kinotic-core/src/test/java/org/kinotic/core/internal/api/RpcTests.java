

package org.kinotic.core.internal.api;

import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.MessageConsumer;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.kinotic.core.api.exceptions.RpcServiceUnavailableException;
import org.kinotic.core.internal.api.service.rpc.RpcReturnValueHandlerFactory;
import org.kinotic.core.internal.api.service.rpc.RpcRequest;
import org.kinotic.core.internal.api.service.rpc.RpcReturnValueHandler;
import org.kinotic.core.api.Kinotic;
import org.kinotic.core.api.event.CRI;
import org.kinotic.core.api.event.Event;
import org.kinotic.core.api.event.EventBusService;
import org.kinotic.core.api.event.EventConstants;
import org.kinotic.core.api.event.EventConsumer;
import org.kinotic.core.api.event.Metadata;
import org.kinotic.core.api.exceptions.AuthorizationException;
import org.kinotic.core.api.exceptions.RpcInvocationException;
import org.kinotic.core.api.exceptions.RpcMissingMethodException;
import org.kinotic.core.api.exceptions.RpcMissingServiceException;
import org.kinotic.core.api.security.Participant;
import org.kinotic.core.api.security.SecurityContext;
import org.kinotic.core.internal.api.support.*;
import org.kinotic.core.internal.utils.EventUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import reactor.util.function.Tuple2;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.util.TokenBuffer;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 *
 * Created by navid on 10/30/19
 */
@SpringBootTest
@ActiveProfiles({"test"})
public class RpcTests {

    @Autowired
    private Kinotic kinotic;
    @SuppressWarnings("SpringJavaInjectionPointsAutowiringInspection") // these are not detected because continuum wires them..
    @Autowired
    private NonExistentServiceProxy nonExistentServiceProxy;
    @SuppressWarnings("SpringJavaInjectionPointsAutowiringInspection") // these are not detected because continuum wires them..
    @Autowired
    private RpcTestServiceProxy rpcTestServiceProxy;
    @SuppressWarnings("SpringJavaInjectionPointsAutowiringInspection") // these are not detected because continuum wires them..
    @Autowired
    private TerminalReplyServiceProxy terminalReplyServiceProxy;

    @Autowired
    private EventBusService eventBusService;
    @Autowired
    private JsonMapper jsonMapper;
    @Autowired
    private Vertx vertx;
    @Autowired
    private SecurityContext securityContext;
    @Autowired
    private RpcReturnValueHandlerFactory rpcReturnValueHandlerFactory;

    private static final String PARTICIPANT_ID = "test-participant";

    /**
     * Runs {@code proxyCall} on a Vert.x context that has a participant bound, then returns its result.
     * The RPC proxy captures the sender from the current context when the method is invoked, so the
     * invocation must happen on a context (a JUnit thread has none).
     */
    private <T> T withParticipant(Supplier<T> proxyCall) {
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
        try {
            return future.get(10, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    // TODO: test to few arguments, and too many arguments, also a variation with the participant. Participant variant error message may be misleading?
    // See org.kinotic.core.internal.api.service.json.AbstractJacksonSupport Line 114, Line 180. Should we keep the number of participant args in mind.

    @Test
    public void testABunchOfArguments(){
        ABunchOfArgumentsHolder argumentsHolder = RpcTestService.BUNCH_OF_ARGUMENTS;
        Mono<ABunchOfArgumentsHolder> mono = rpcTestServiceProxy.acceptABunchOfArguments(argumentsHolder.getIntValue(),
                                                                                         argumentsHolder.getLongValue(),
                                                                                         argumentsHolder.getStringValue(),
                                                                                         argumentsHolder.isBoolValue(),
                                                                                         argumentsHolder.getSimpleObject(),
                                                                                         RpcTestService.LIST_OF_STRINGS);

        StepVerifier.create(mono)
                    .expectNext(argumentsHolder)
                    .expectComplete()
                    .verify();
    }

    @Test
    public void testSendTwoArgsReturnOne(){
        String lhs = "Hello ";
        String rhs = "Wat";
        Mono<String> mono = rpcTestServiceProxy.concatString(lhs, rhs);

        StepVerifier.create(mono)
                    .expectNext(lhs + rhs)
                    .expectComplete()
                    .verify();
    }

    @Test
    public void testObjectMethodsAreAnsweredWithoutRemoteInvocation(){
        Assertions.assertTrue(rpcTestServiceProxy.toString().contains("RpcTestService"));

        Assertions.assertEquals(rpcTestServiceProxy, rpcTestServiceProxy);
        Assertions.assertNotEquals(rpcTestServiceProxy, nonExistentServiceProxy);

        // exercises hashCode and equals together, and would hang or throw if either went to the remote end
        Assertions.assertTrue(Set.of(rpcTestServiceProxy, nonExistentServiceProxy).contains(rpcTestServiceProxy));
    }

    @Test
    public void testFirstArgParticipant(){
        String suffix = " Wat";
        Mono<String> mono = withParticipant(() -> rpcTestServiceProxy.firstArgParticipant(suffix));

        StepVerifier.create(mono)
                    .expectNext(PARTICIPANT_ID + suffix)
                    .expectComplete()
                    .verify();
    }

    @Test
    public void testGetByteArray(){
        Mono<byte[]> mono = rpcTestServiceProxy.getByteArray();

        StepVerifier.create(mono)
                    .expectNextMatches(bytes -> Arrays.equals(bytes, RpcTestService.BINARY_VALUE))
                    .expectComplete()
                    .verify();
    }

    @Test
    public void testGetBuffer(){
        Mono<Buffer> mono = rpcTestServiceProxy.getBuffer();

        StepVerifier.create(mono)
                    .expectNextMatches(buffer -> Arrays.equals(buffer.getBytes(), RpcTestService.BINARY_VALUE))
                    .expectComplete()
                    .verify();
    }

    @Test
    public void testGetMonoByteArray(){
        Mono<byte[]> mono = rpcTestServiceProxy.getMonoByteArray();

        StepVerifier.create(mono)
                    .expectNextMatches(bytes -> Arrays.equals(bytes, RpcTestService.BINARY_VALUE))
                    .expectComplete()
                    .verify();
    }

    @Test
    public void testGetByteArrayFlux(){
        Flux<byte[]> flux = rpcTestServiceProxy.getByteArrayFlux();

        StepVerifier.create(flux)
                    .expectNextMatches(bytes -> Arrays.equals(bytes, RpcTestService.BINARY_CHUNKS[0]))
                    .expectNextMatches(bytes -> Arrays.equals(bytes, RpcTestService.BINARY_CHUNKS[1]))
                    .expectNextMatches(bytes -> Arrays.equals(bytes, RpcTestService.BINARY_CHUNKS[2]))
                    .expectComplete()
                    .verify();
    }

    @Test
    public void testInfiniteFlux(){
        Flux<String> flux = rpcTestServiceProxy.getInfiniteFlux();

        StepVerifier.create(flux)
                    .expectNextMatches(s -> s.startsWith("Hello Sucka"))
                    .expectNextMatches(s -> s.startsWith("Hello Sucka"))
                    .expectNextMatches(s -> s.startsWith("Hello Sucka"))
                    .expectNextMatches(s -> s.startsWith("Hello Sucka"))
                    .thenCancel()
                    .verify();
    }

    /**
     * A streaming result monitors its reply listener so it can be terminated when the listener goes
     * away. Registration changes on other addresses must not affect the stream.
     */
    @Test
    public void testInfiniteFluxSurvivesUnrelatedConsumerChurn(){
        Flux<String> flux = rpcTestServiceProxy.getInfiniteFlux();

        StepVerifier.create(flux)
                    .expectNextMatches(s -> s.startsWith("Hello Sucka"))
                    .then(() -> {
                        try {
                            MessageConsumer<Object> unrelated =
                                    vertx.eventBus().consumer("some.completely.unrelated.address", msg -> {});
                            unrelated.completion().toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
                            unrelated.unregister().toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    })
                    .expectNextMatches(s -> s.startsWith("Hello Sucka"))
                    .expectNextMatches(s -> s.startsWith("Hello Sucka"))
                    .thenCancel()
                    .verify(Duration.ofSeconds(15));
    }

    @Test
    public void testLastArgParticipant(){
        String prefix = "Hello ";

        Mono<String> mono = withParticipant(() -> rpcTestServiceProxy.lastArgParticipant(prefix));

        StepVerifier.create(mono)
                    .expectNext(prefix + PARTICIPANT_ID)
                    .expectComplete()
                    .verify();
    }

    @Test
    public void testLimitedFlux(){
        Flux<Integer> flux = rpcTestServiceProxy.getLimitedFlux();

        StepVerifier.create(flux)
                    .expectNext(1, 2, 3, 4, 5)
                    .expectComplete()
                    .verify();
    }

    @Test
    public void testMiddleArgParticipant(){
        String prefix = "Hello ";
        String suffix = " Wat";

        Mono<String> mono = withParticipant(() -> rpcTestServiceProxy.middleArgParticipant(prefix, suffix));

        StepVerifier.create(mono)
                    .expectNext(prefix + PARTICIPANT_ID + suffix)
                    .expectComplete()
                    .verify();
    }

    @Test
    public void testNarrowParticipantRejectsWiderCaller(){
        // the bound participant is a plain Participant, so it is not the NarrowParticipant the
        // service declares
        Mono<String> mono = withParticipant(rpcTestServiceProxy::narrowParticipant);

        StepVerifier.create(mono)
                    .expectErrorMatches(throwable -> throwable instanceof AuthorizationException)
                    .verify();
    }

    @Test
    public void testMissingRemoteMethodFailure() {
        Mono<String> mono = rpcTestServiceProxy.getMissingRemoteMethodFailure();

        StepVerifier.create(mono)
                    .expectError(RpcMissingMethodException.class)
                    .verify();
    }

    @Test
    public void testMissingServiceFailure() {
        Mono<Void> mono = nonExistentServiceProxy.probablyNotHome();

        StepVerifier.create(mono)
                    .expectError(RpcMissingServiceException.class)
                    .verify();
    }

    @Test
    public void testMonoEmptyString() {
        Mono<String> mono = rpcTestServiceProxy.getMonoEmptyString();

        StepVerifier.create(mono)
                    .expectNext("")
                    .expectComplete()
                    .verify();
    }

    @Test
    public void testMonoIntegerNull() {
        AtomicBoolean hasNull = new AtomicBoolean();
        Mono<Integer> mono = rpcTestServiceProxy
                .getMonoIntegerNull()
                .doOnSuccess(v -> {
                    if (v == null) hasNull.set(true);
                });

        StepVerifier.create(mono)
                    .expectComplete()
                    .verify();

        Assertions.assertTrue(hasNull.get());
    }

    @Test
    public void testMonoStringLiterallyNull() {
        Mono<String> mono = rpcTestServiceProxy.getMonoStringLiterallyNull();

        StepVerifier.create(mono)
                    .expectNext("null")
                    .expectComplete()
                    .verify();
    }

    @Test
    public void testMonoStringNull() {
        AtomicBoolean hasNull = new AtomicBoolean();
        Mono<String> mono = rpcTestServiceProxy
                .getMonoStringNull()
                .doOnSuccess(v -> {
                    if (v == null) hasNull.set(true);
                });

        StepVerifier.create(mono)
                    .expectComplete()
                    .verify();

        Assertions.assertTrue(hasNull.get());
    }

    @Test
    public void testMonoWithValue() {
        Mono<String> mono = rpcTestServiceProxy.getMonoWithValue();

        StepVerifier.create(mono)
                    .expectNext("Hello Bob")
                    .expectComplete()
                    .verify();
    }

    @Test
    public void testMonoWithVoidFromEmpty() {
        Mono<Void> mono = rpcTestServiceProxy.getMonoWithVoidFromEmpty();
        StepVerifier.create(mono)
                    .expectComplete()
                    .verify();
    }

    @Test
    public void testMonoWithVoidFromNull() {
        Mono<Void> mono = rpcTestServiceProxy.getMonoWithVoidFromNull();
        StepVerifier.create(mono)
                    .expectComplete()
                    .verify();
    }

    /**
     * Pins the terminal reply contract: a single-value reply carries {@link EventConstants#CONTROL_VALUE_COMPLETE}
     * on the reply event itself, so any hop holding per-request state can release it on that one event without
     * knowing the invoked method's shape.
     */
    @Test
    public void testSingleValueReplyCarriesTerminalMarker() throws Exception {
        CRI replyCri = CRI.create(EventConstants.REPLY_DESTINATION_SCHEME,
                                  UUID.randomUUID().toString(),
                                  "org.kinotic.tests.TerminalMarkerProbe");
        EventConsumer replyConsumer = eventBusService.listen(replyCri);
        CompletableFuture<Event<byte[]>> replyFuture = new CompletableFuture<>();
        replyConsumer.handler(replyFuture::complete);
        replyConsumer.completion().toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
        try {
            Metadata metadata = Metadata.create();
            metadata.put(EventConstants.REPLY_TO_HEADER, replyCri.raw());
            metadata.put(EventConstants.CORRELATION_ID_HEADER, UUID.randomUUID().toString());
            metadata.put(EventConstants.CONTENT_TYPE_HEADER, "application/json");
            CRI requestCri = CRI.create(EventConstants.SERVICE_DESTINATION_SCHEME,
                                        null,
                                        "org.kinotic.core.internal.api.support.RpcTestService",
                                        "/getMonoEmptyString",
                                        null);
            eventBusService.send(Event.create(requestCri, metadata, new byte[0]));

            Event<byte[]> reply = replyFuture.get(10, TimeUnit.SECONDS);
            Assertions.assertFalse(reply.metadata().contains(EventConstants.ERROR_HEADER));
            Assertions.assertEquals(EventConstants.CONTROL_VALUE_COMPLETE,
                                    reply.metadata().get(EventConstants.CONTROL_HEADER));
            Assertions.assertNotNull(reply.data());
            Assertions.assertTrue(reply.data().length > 0);
        } finally {
            replyConsumer.unregister();
        }
    }

    /**
     * A single-value runtime (a TS service) answers a stream-typed caller with one terminal reply that
     * carries both the value and the completion marker. The proxy must emit the value and then complete
     * instead of treating the event as a bare completion.
     */
    @Test
    public void testTerminalReplyWithValueCompletesFluxProxy() throws Exception {
        CRI serviceCri = CRI.create(EventConstants.SERVICE_DESTINATION_SCHEME,
                                    "org.kinotic.core.internal.api.support.TerminalReplyService");
        EventConsumer serviceConsumer = eventBusService.listen(serviceCri);
        serviceConsumer.handler(request -> {
            Event<byte[]> reply = EventUtil.createReplyEvent(request.metadata(),
                                                             Map.of(EventConstants.CONTENT_TYPE_HEADER, "application/json",
                                                                    EventConstants.CONTROL_HEADER, EventConstants.CONTROL_VALUE_COMPLETE),
                                                             () -> jsonMapper.writeValueAsBytes("hello"));
            eventBusService.send(reply);
        });
        serviceConsumer.completion().toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
        try {
            StepVerifier.create(terminalReplyServiceProxy.streamFromSingleValueRuntime())
                        .expectNext("hello")
                        .expectComplete()
                        .verify(Duration.ofSeconds(15));
        } finally {
            serviceConsumer.unregister();
        }
    }

    @Test
    public void testFutureHandlerSettlesOnceUnderCompetingSignals() throws Exception {
        // a lost-node failure and a reply reach the same handler from different contexts; the second signal is dropped
        RpcReturnValueHandler handler = rpcReturnValueHandlerFactory.createReturnValueHandler(
                RpcTestServiceProxy.class.getMethod("getAnotherString"), new Object[0]);
        @SuppressWarnings("unchecked")
        Future<String> future = (Future<String>) handler.getReturnValue(new RpcRequest() {
            @Override
            public void send() {}
            @Override
            public void cancelRequest() {}
        });
        handler.processError(new RpcServiceUnavailableException("node left"));
        handler.processError(new IllegalStateException("late"));
        handler.cancel("released");

        Assertions.assertTrue(future.failed());
        Assertions.assertInstanceOf(RpcServiceUnavailableException.class, future.cause());
    }

    @Test
    public void testInvocationsOfOneServiceOverlap(){
        // four calls whose results complete 500 ms later, off the delivery context; they are dispatched on
        // the service's context and must not wait on one another
        long start = System.currentTimeMillis();
        List<CompletableFuture<String>> calls = new ArrayList<>();
        for(int i = 0; i < 4; i++){
            calls.add(rpcTestServiceProxy.getMonoAfterDelay("done", 500).toFuture());
        }
        for(CompletableFuture<String> call : calls){
            Assertions.assertEquals("done", call.orTimeout(10, TimeUnit.SECONDS).join());
        }
        long elapsed = System.currentTimeMillis() - start;
        Assertions.assertTrue(elapsed < 1500, "four 500 ms results took " + elapsed + " ms, so they were serialized");
    }

    @Test
    public void testMultipleRequests(){
        Mono<String> mono = Mono.fromFuture(rpcTestServiceProxy.getString());

        StepVerifier.create(mono).expectNext(RpcTestService.STRING_VALUE).expectComplete().verify();

        Mono<String> mono2 = Mono.fromFuture(rpcTestServiceProxy.getString());

        StepVerifier.create(mono2).expectNext(RpcTestService.STRING_VALUE).expectComplete().verify();

        Mono<String> mono3 = Mono.fromFuture(rpcTestServiceProxy.getString());

        StepVerifier.create(mono3).expectNext(RpcTestService.STRING_VALUE).expectComplete().verify();
    }

    @Test
    public void testNestedArrays(){

        List<List<String>> input = new ArrayList<>();
        input.add(RpcTestService.LIST_OF_STRINGS);
        input.add(RpcTestService.LIST_OF_STRINGS);
        input.add(RpcTestService.LIST_OF_STRINGS);
        Mono<List<List<String>>> mono = rpcTestServiceProxy.getAListOfLists(input);
        StepVerifier.create(mono)
                    .expectNext(input.stream().map(strings -> strings.stream().map(s -> "Hello "+ s).collect(Collectors.toList())).collect(Collectors.toList()))
                    .expectComplete()
                    .verify();
    }

    @Test
    public void testPutListOfSimpleObjects(){
        List<SimpleObject> simpleObjects = new ArrayList<>();
        for(int i = 0; i < 10; i++){
            simpleObjects.add(RpcTestService.STATIC_SIMPLE_OBJECT);
        }
        Mono<Integer> mono = rpcTestServiceProxy.putListOfSimpleObjects(simpleObjects);
        StepVerifier.create(mono)
                    .expectNext(10)
                    .expectComplete()
                    .verify();
    }

    @Test
    public void testPutMapOfSimpleObjects(){
        Map<String, SimpleObject> simpleObjects = new HashMap<>();
        for(int i = 0; i < 10; i++){
            simpleObjects.put(UUID.randomUUID().toString(), RpcTestService.STATIC_SIMPLE_OBJECT);
        }
        Mono<Integer> mono = rpcTestServiceProxy.putMapOfSimpleObjects(simpleObjects);
        StepVerifier.create(mono)
                    .expectNext(10)
                    .expectComplete()
                    .verify();
    }

    @Test
    public void testPutNestedGenerics(){
        List<Map<String, Set<SimpleObject>>> toSend = new ArrayList<>();
        for(int x = 0; x < 2; x++){
            Map<String, Set<SimpleObject>> simpleObjectsMap = new HashMap<>();
            for(int i = 0; i < 5; i++){

                Set<SimpleObject> simpleObjectSet = new HashSet<>();
                for(int o = 0; o < 10; o++){
                    SimpleObject obj = new SimpleObject().setFirstName("Johnny")
                                                         .setLastName("Blaze_" + o)
                                                         .setCount(10)
                                                         .setBigCount(10000000L);
                    simpleObjectSet.add(obj);
                }

                simpleObjectsMap.put(UUID.randomUUID().toString(),
                                     simpleObjectSet);
            }
            toSend.add(simpleObjectsMap);
        }

        Mono<Integer> mono = rpcTestServiceProxy.putNestedGenerics(toSend);
        StepVerifier.create(mono)
                    .expectNext(100)
                    .expectComplete()
                    .verify();
    }

    @Test
    public void testReceiveCollection(){
        Mono<List<String>> mono = rpcTestServiceProxy.getListOfStrings();
        StepVerifier.create(mono)
                    .expectNext(RpcTestService.LIST_OF_STRINGS)
                    .expectComplete()
                    .verify();
    }

    @Test
    public void testRpcCompletableFutureString(){
        CompletableFuture<String> mono = rpcTestServiceProxy.getString();

        StepVerifier.create(Mono.fromFuture(mono)).expectNext(RpcTestService.STRING_VALUE).expectComplete().verify();
    }

    @Test
    public void testSendAndReceiveCollection(){
        Mono<List<String>> mono = rpcTestServiceProxy.modifyListOfStrings(RpcTestService.LIST_OF_STRINGS);
        StepVerifier.create(mono)
                    .expectNext(RpcTestService.LIST_OF_STRINGS.stream().map(s -> "Hello "+ s).collect(Collectors.toList()))
                    .expectComplete()
                    .verify();
    }

    @Test
    public void testSendCollection(){
        Mono<Integer> mono = rpcTestServiceProxy.putListOfStrings(RpcTestService.LIST_OF_STRINGS);
        StepVerifier.create(mono)
                    .expectNext(RpcTestService.LIST_OF_STRINGS.size())
                    .expectComplete()
                    .verify();
    }

    @Test
    public void testSendTokenBuffer() {
        try (TokenBuffer tokenBuffer = new TokenBuffer(jsonMapper._serializationContext(), false)) {
            tokenBuffer.writeStartObject();
            tokenBuffer.writeStringProperty("test", "Hello Sucka");
            tokenBuffer.writeEndObject();

            Mono<String> mono = rpcTestServiceProxy.echoTokenBuffer(tokenBuffer);
            StepVerifier.create(mono)
                        .expectNext("{\"test\":\"Hello Sucka\"}")
                        .expectComplete()
                        .verify();
        }
    }

    @Test
    public void testSimpleObject(){
        Mono<Tuple2<SimpleObject, String>> mono = rpcTestServiceProxy.getSimpleObject()
                                                                     .zipWhen(simpleObject -> rpcTestServiceProxy.getSimpleObjectToString(simpleObject));

        StepVerifier.create(mono)
                    .expectNextMatches(tuple -> tuple.getT1().toString().equals(tuple.getT2()))
                    .expectComplete()
                    .verify();
    }

    @Test
    public void testUnknownFailure(){
        Mono<String> mono = rpcTestServiceProxy.getUnknownFailure();

        StepVerifier.create(mono).expectErrorMatches(throwable -> {
            boolean ret = false;
            // When the original exception class cannot be instantiated you wll get a RpcInvocationException
            if(throwable instanceof RpcInvocationException){
                ret = Objects.equals(((RpcInvocationException) throwable).getOriginalClassName(),
                                     "org.kinotic.core.internal.api.support.DefaultRpcTestService$UnknownThrowable");
            }
            return ret;
        }).verify();
    }

    @Test
    public void testVertxFuture(){
        Future<String> future  = rpcTestServiceProxy.getAnotherString();

        Awaitility.await().until(future::isComplete);

        if(future.failed()){
            throw new IllegalStateException("TestServiceProxy method invocation failed", future.cause());
        }else if(!future.result().equals(RpcTestService.STRING_VALUE)){
            throw new IllegalStateException("Service data returned does not match what was expected: "+RpcTestService.STRING_VALUE+" got: "+future.result());
        }
    }

    @Test
    public void testVertxFutureNullString(){
        Future<String> future  = rpcTestServiceProxy.getVertxFutureNullString();

        Awaitility.await().until(future::isComplete);

        if(future.failed()){
            throw new IllegalStateException("TestServiceProxy method invocation failed", future.cause());
        }else if(!(future.result() == null)){
            throw new IllegalStateException("Service data returned does not match what was expected: null got: "+future.result());
        }
    }

}
