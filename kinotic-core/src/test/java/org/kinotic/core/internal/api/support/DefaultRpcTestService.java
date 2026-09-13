

package org.kinotic.core.internal.api.support;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import org.kinotic.core.api.security.Participant;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoSink;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.util.TokenBuffer;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 *
 * Created by navid on 10/30/19
 */
@Component
public class DefaultRpcTestService implements RpcTestService{

    @Autowired
    private JsonMapper jsonMapper;
    @Autowired
    private Vertx vertx;

    @Override
    public ABunchOfArgumentsHolder acceptABunchOfArguments(int intValue,
                                            long longValue,
                                            String stringValue,
                                            boolean boolValue,
                                            SimpleObject simpleObject,
                                            List<String> listOfStrings) {
        return new ABunchOfArgumentsHolder(intValue, longValue, stringValue, boolValue, simpleObject, listOfStrings);
    }

    @Override
    public String concatString(String lhs, String rhs) {
        return lhs + rhs;
    }

    @Override
    public Mono<String> firstArgParticipant(Participant participant, String suffix){
        return Mono.just(participant.getId() + suffix);
    }

    @Override
    public List<List<String>> getAListOfLists(List<List<String>> inputList) {
        return inputList.stream().map(strings -> strings.stream().map(s -> "Hello "+ s).collect(Collectors.toList())).collect(Collectors.toList());
    }

    @Override
    public String getAnotherString() {
        return RpcTestService.STRING_VALUE;
    }

    @Override
    public Buffer getBuffer() {
        return Buffer.buffer(RpcTestService.BINARY_VALUE);
    }

    @Override
    public byte[] getByteArray() {
        return RpcTestService.BINARY_VALUE;
    }

    @Override
    public Flux<byte[]> getByteArrayFlux() {
        return Flux.fromArray(RpcTestService.BINARY_CHUNKS);
    }

    @Override
    public Flux<String> getInfiniteFlux() {
        return Flux.create(sink -> {
            AtomicLong count = new AtomicLong(0);
            long timerId = vertx.setPeriodic(1000, l ->{
                sink.next("Hello Sucka " + count.get());
                count.incrementAndGet();
            });

            sink.onDispose(() ->  {
                vertx.cancelTimer(timerId);
            });

        });
    }

    @Override
    public Flux<Integer> getLimitedFlux() {
        return Flux.just(1,2,3,4,5);
    }

    @Override
    public List<String> getListOfStrings() {
        return LIST_OF_STRINGS;
    }

    @Override
    public Mono<byte[]> getMonoByteArray() {
        return Mono.just(RpcTestService.BINARY_VALUE);
    }

    @Override
    public Mono<String> getMonoEmptyString() {
        return Mono.just("");
    }

    @Override
    public Mono<String> getMonoFailure() {
        return Mono.error(new IllegalStateException("Something went terribly wrong"));
    }

    @Override
    public Mono<Integer> getMonoIntegerNull() {
        return Mono.create(sink -> {
            sink.success(null);
        });
    }

    @Override
    public Mono<String> getMonoStringLiterallyNull() {
        return Mono.just("null");
    }

    @Override
    public Mono<String> getMonoStringNull() {
        return Mono.create(sink -> {
            sink.success(null);
        });
    }

    @Override
    public Mono<String> getMonoWithValue() {
        return Mono.just("Hello Bob");
    }

    @Override
    public Mono<String> getMonoAfterDelay(String value, long delayMillis) {
        return Mono.delay(Duration.ofMillis(delayMillis)).map(_ -> value);
    }

    @Override
    public Mono<Void> getMonoWithVoidFromEmpty() {
        return Mono.create(MonoSink::success);
    }

    @Override
    public Mono<Void> getMonoWithVoidFromNull() {
        return Mono.create(sink -> {
            sink.success(null);
        });
    }

    @Override
    public SimpleObject getSimpleObject() {
        return STATIC_SIMPLE_OBJECT;
    }

    @Override
    public String getSimpleObjectToString(SimpleObject simpleObject) {
        return simpleObject.toString();
    }

    @Override
    public String getString() {
        return RpcTestService.STRING_VALUE;
    }

    @Override
    public String getUnknownFailure() {
        throw new UnknownThrowable("Everything failed Sucka!");
    }

    @Override
    public Future<String> getVertxFutureNullString() {
        return Future.succeededFuture(null);
    }

    @Override
    public Mono<String> lastArgParticipant(String prefix, Participant participant){
        return Mono.just(prefix + participant.getId());
    }

    @Override
    public Mono<String> middleArgParticipant(String prefix, Participant participant, String suffix){
        return Mono.just(prefix + participant.getId() + suffix);
    }

    @Override
    public List<String> modifyListOfStrings(String[] stringsToModify) {
        return Arrays.stream(stringsToModify).map(s -> "Hello "+ s).collect(Collectors.toList());
    }

    @Override
    public Mono<String> narrowParticipant(NarrowParticipant participant){
        return Mono.just(participant.getId());
    }

    @Override
    public Integer putListOfSimpleObjects(List<SimpleObject> simpleObjects) {
        for(SimpleObject simpleObject : simpleObjects){
            if(simpleObject == null){
                throw new IllegalArgumentException("SimpleObject cannot be null");
            }
            if(!simpleObject.getFirstName().equals("Johnny")){
                throw new IllegalArgumentException("SimpleObject firstname must be Johnny");
            }
        }
        return simpleObjects.size();
    }

    @Override
    public Integer putListOfStrings(List<String> strings) {
        return strings.size();
    }

    @Override
    public Integer putMapOfSimpleObjects(Map<String, SimpleObject> simpleObjects) {
        for(Map.Entry<String, SimpleObject> entry : simpleObjects.entrySet()){
            if(entry == null){
                throw new IllegalArgumentException("Map.Entry cannot be null");
            }

            //noinspection ResultOfMethodCallIgnored
            UUID.fromString(entry.getKey());

            if(!entry.getValue().getFirstName().equals("Johnny")){
                throw new IllegalArgumentException("SimpleObject firstname must be Johnny");
            }
        }
        return simpleObjects.size();
    }

    @Override
    public Integer putNestedGenerics(List<Map<String, Set<SimpleObject>>> objects) {
        Integer ret = 0;
        for(Map<String, Set<SimpleObject>> map : objects){
            for(Map.Entry<String, Set<SimpleObject>> entry : map.entrySet()){

                //noinspection ResultOfMethodCallIgnored
                UUID.fromString(entry.getKey());

                for(SimpleObject simpleObject : entry.getValue()){
                    if(simpleObject == null){
                        throw new IllegalArgumentException("SimpleObject cannot be null");
                    }
                    if(!simpleObject.getFirstName().equals("Johnny")){
                        throw new IllegalArgumentException("SimpleObject firstname must be Johnny");
                    }
                    ret++;
                }
            }
        }
        return ret;
    }

    @Override
    public String echoTokenBuffer(TokenBuffer tokenBuffer) {
        try {
            String jsonString = jsonMapper.writeValueAsString(tokenBuffer);
            return jsonString;
        } catch (JacksonException e) {
            throw new IllegalStateException(e);
        }
    }

    private static class UnknownThrowable extends RuntimeException{
        public UnknownThrowable(String message) {
            super(message);
        }
    }
}
