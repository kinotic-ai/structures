

package org.kinotic.core.internal.api.service.rpc.types;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import org.kinotic.core.api.event.Event;
import org.kinotic.core.api.event.EventConstants;
import org.kinotic.core.internal.api.service.ExceptionConverter;
import org.kinotic.core.internal.api.service.rpc.RpcRequest;
import org.kinotic.core.internal.api.service.rpc.RpcResponseConverter;
import org.kinotic.core.internal.api.service.rpc.RpcReturnValueHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.MethodParameter;
import org.springframework.util.Assert;

/**
 * Return value handler that provides a {@link Future}
 *
 * Created by navid on 2019-04-25.
 */
public class VertxFutureRpcReturnValueHandler implements RpcReturnValueHandler {

    private static final Logger log = LoggerFactory.getLogger(VertxFutureRpcReturnValueHandler.class);

    private final MethodParameter methodParameter;
    private final RpcResponseConverter rpcResponseConverter;
    private final ExceptionConverter exceptionConverter;
    private final Promise<Object> promise;

    public VertxFutureRpcReturnValueHandler(MethodParameter methodParameter,
                                            RpcResponseConverter rpcResponseConverter,
                                            ExceptionConverter exceptionConverter) {

        Assert.notNull(methodParameter, "methodParameter must not be null");
        Assert.notNull(rpcResponseConverter, "responseConverter must not be null");
        Assert.notNull(exceptionConverter, "exceptionConverter must not be null");

        this.methodParameter = methodParameter;
        this.rpcResponseConverter = rpcResponseConverter;
        this.exceptionConverter = exceptionConverter;
        this.promise = Promise.promise();
    }

    @Override
    public boolean processResponse(Event<byte[]> incomingEvent) {
        try{
            // Error data is returned differently
            // the reply and a lost-node failure run on different contexts and can both reach this promise
            if(incomingEvent.metadata().contains(EventConstants.ERROR_HEADER)) {
                promise.tryFail(exceptionConverter.convert(incomingEvent));
            }else{
                promise.tryComplete(rpcResponseConverter.convert(incomingEvent, methodParameter));
            }
        }catch (Exception e){
            log.error("Error converting the incoming message to expected java type", e);
            promise.tryFail(e);
        }
        return true;
    }

    @Override
    public boolean isMultiValue() {
        return false;
    }

    @Override
    public Object getReturnValue(RpcRequest rpcRequest) {
        rpcRequest.send();
        return promise.future();
    }

    @Override
    public void processError(Throwable throwable) {
        promise.tryFail(throwable);
    }

    @Override
    public void cancel(String message) {
        promise.tryFail(message);
    }

}
