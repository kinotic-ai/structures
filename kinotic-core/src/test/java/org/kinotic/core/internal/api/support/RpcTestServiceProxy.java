

package org.kinotic.core.internal.api.support;

import io.vertx.core.Future;
import io.vertx.core.buffer.Buffer;
import org.kinotic.core.api.annotations.Proxy;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import tools.jackson.databind.util.TokenBuffer;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

/**
 *
 * Created by navid on 10/30/19
 */
@Proxy(namespace = "org.kinotic.core.internal.api.support",
       name = "RpcTestService")
public interface RpcTestServiceProxy {

    Mono<ABunchOfArgumentsHolder> acceptABunchOfArguments(int intValue,
                                                          long longValue,
                                                          String stringValue,
                                                          boolean boolValue,
                                                          SimpleObject simpleObject,
                                                          List<String> listOfStrings);

    Mono<String> concatString(String lhs, String rhs);

    Mono<String> firstArgParticipant(String suffix);

    Mono<List<List<String>>> getAListOfLists(List<List<String>> inputList);

    Future<String> getAnotherString();

    Mono<Buffer> getBuffer();

    Mono<byte[]> getByteArray();

    Flux<byte[]> getByteArrayFlux();

    Flux<String> getInfiniteFlux();

    Flux<Integer> getLimitedFlux();

    Mono<List<String>> getListOfStrings();

    Mono<String> getMissingRemoteMethodFailure();

    Mono<byte[]> getMonoByteArray();

    Mono<String> getMonoEmptyString();

    Mono<Integer> getMonoIntegerNull();

    Mono<String> getMonoStringLiterallyNull();

    Mono<String> getMonoStringNull();

    Mono<String> getMonoWithValue();

    Mono<String> getMonoAfterDelay(String value, long delayMillis);

    Mono<Void> getMonoWithVoidFromEmpty();

    Mono<Void> getMonoWithVoidFromNull();

    Mono<SimpleObject> getSimpleObject();

    Mono<String> getSimpleObjectToString(SimpleObject simpleObject);

    CompletableFuture<String> getString();

    Mono<String> getUnknownFailure();

    Future<String> getVertxFutureNullString();

    Mono<String> lastArgParticipant(String prefix);

    Mono<String> middleArgParticipant(String prefix, String suffix);

    Mono<List<String>> modifyListOfStrings(List<String> stringsToModify);

    Mono<String> narrowParticipant();

    Mono<Integer> putListOfSimpleObjects(List<SimpleObject> simpleObjects);

    Mono<Integer> putListOfStrings(List<String> strings);

    Mono<Integer> putMapOfSimpleObjects(Map<String, SimpleObject> simpleObjects);

    Mono<Integer> putNestedGenerics(List<Map<String, Set<SimpleObject>>> objects);

    Mono<String> echoTokenBuffer(TokenBuffer tokenBuffer);
}
