

package org.kinotic.core.internal.api.support;

import io.vertx.core.Future;
import io.vertx.core.buffer.Buffer;
import org.kinotic.core.api.annotations.Publish;
import org.kinotic.core.api.security.Participant;
import org.kinotic.core.internal.api.RpcTests;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import tools.jackson.databind.util.TokenBuffer;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Class to publish various service methods to be used by the {@link RpcTests}
 *
 * Created by navid on 10/30/19
 */
@Publish
public interface RpcTestService {

    List<String> LIST_OF_STRINGS = List.of("Bob", "Annie", "Wendy", "Nick", "Jose", "Joaquin", "Chaoxiang", "Johnny Blaze", "Sucka");
    SimpleObject STATIC_SIMPLE_OBJECT  = new SimpleObject().setFirstName("Johnny")
                                                           .setLastName("Blaze")
                                                           .setCount(10)
                                                           .setBigCount(10000000L);
    ABunchOfArgumentsHolder BUNCH_OF_ARGUMENTS = new ABunchOfArgumentsHolder(42, 23421432343242L, "Method Man", true, STATIC_SIMPLE_OBJECT, LIST_OF_STRINGS);
    /**
     * The value returned by getString below
     */
    String STRING_VALUE = "Hello Sucka!";

    byte[] BINARY_VALUE = {0, 1, 2, 3, (byte) 0xFF, (byte) 0xFE, 42, -1};

    byte[][] BINARY_CHUNKS = {{10, 20, 30}, {40, 50}, {60, 70, 80, 90}};

    ABunchOfArgumentsHolder acceptABunchOfArguments(int intValue,
                                                    long longValue,
                                                    String stringValue,
                                                    boolean boolValue,
                                                    SimpleObject simpleObject,
                                                    List<String> listOfStrings);

    String concatString(String lhs, String rhs);

    Mono<String> firstArgParticipant(Participant participant, String suffix);

    List<List<String>> getAListOfLists(List<List<String>> inputList);

    String getAnotherString();

    Buffer getBuffer();

    byte[] getByteArray();

    Flux<byte[]> getByteArrayFlux();

    Flux<String> getInfiniteFlux();

    Flux<Integer> getLimitedFlux();

    List<String> getListOfStrings();

    Mono<byte[]> getMonoByteArray();

    Mono<String> getMonoEmptyString();

    Mono<String> getMonoFailure();

    Mono<Integer> getMonoIntegerNull();

    Mono<String> getMonoStringLiterallyNull();

    Mono<String> getMonoStringNull();

    Mono<String> getMonoWithValue();

    /**
     * Completes with the given value after the delay, off the delivery context.
     */
    Mono<String> getMonoAfterDelay(String value, long delayMillis);

    Mono<Void> getMonoWithVoidFromEmpty();

    Mono<Void> getMonoWithVoidFromNull();

    SimpleObject getSimpleObject();

    String getSimpleObjectToString(SimpleObject simpleObject);

    String getString();

    String getUnknownFailure();

    Future<String> getVertxFutureNullString();

    Mono<String> lastArgParticipant(String prefix, Participant participant);

    Mono<String> middleArgParticipant(String prefix, Participant participant, String suffix);

    List<String> modifyListOfStrings(String[] stringsToModify);

    Mono<String> narrowParticipant(NarrowParticipant participant);

    Integer putListOfSimpleObjects(List<SimpleObject> simpleObjects);

    Integer putListOfStrings(List<String> strings);

    Integer putMapOfSimpleObjects(Map<String, SimpleObject> simpleObjects);

    Integer putNestedGenerics(List<Map<String, Set<SimpleObject>>> objects);

    String echoTokenBuffer(TokenBuffer tokenBuffer);

}
