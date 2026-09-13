


package org.kinotic.core.internal.api.service.rpc;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.propagation.TextMapPropagator;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.EventBus;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.Validate;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.kinotic.core.api.RpcServiceProxy;
import org.kinotic.core.api.RpcServiceProxyHandle;
import org.kinotic.core.api.directory.ServiceDirectory;
import org.kinotic.core.api.event.*;
import org.kinotic.core.api.exceptions.RpcServiceUnavailableException;
import org.kinotic.core.api.security.SecurityContext;
import org.kinotic.core.api.service.RequestLivenessWatcher;
import org.kinotic.core.api.service.ServiceIdentifier;
import org.kinotic.core.api.utils.KinoticUtil;
import org.kinotic.core.internal.utils.MetaUtil;
import org.kinotic.core.internal.utils.TelemetryUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.util.ReflectionUtils;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Base implementation of the {@link RpcServiceProxyHandle}
 * Will send all service requests on the Vertx {@link EventBus}
 *
 * Created by navid on 2019-04-18.
 */
public class DefaultRpcServiceProxyHandle<T> implements RpcServiceProxyHandle<T>, InvocationHandler {

    private static final Logger log = LoggerFactory.getLogger(DefaultRpcServiceProxyHandle.class);

    private static final String INSTRUMENTATION_NAME = "org.kinotic.core.rpc-proxy";

    private final ServiceIdentifier serviceIdentifier;
    private final String nodeName;
    private final Class<T> serviceClass;
    private final CRI handlerCRI;
    private final RpcArgumentConverter rpcArgumentConverter;
    private final RpcReturnValueHandlerFactory rpcReturnValueHandlerFactory;
    private final EventBusService eventBusService;
    private final SecurityContext securityContext;
    private final ObjectProvider<ServiceDirectory> serviceDirectoryProvider;
    private final RequestLivenessWatcher requestLivenessWatcher;
    private final TextMapPropagator propagator;
    private final Tracer tracer;
    private final TraceLogFilter traceLogFilter;
    private final Vertx vertx;

    private final Map<Method, Integer> methodsWithScopeAnnotation = new HashMap<>();
    private final EventConsumer replyEventConsumer;
    private final T serviceProxy;
    private final AtomicBoolean released = new AtomicBoolean(false);

    private final ConcurrentHashMap<String, RpcReturnValueHandler> responseMap = new ConcurrentHashMap<>();
    // correlationIds we recently sent a cancel for, to debounce the in-flight burst before the server stops
    private final Set<String> recentlyReaped = ConcurrentHashMap.newKeySet();
    private static final long REAP_DEBOUNCE_MS = 5000;

    public DefaultRpcServiceProxyHandle(ServiceIdentifier serviceIdentifier,
                                        String nodeName,
                                        Class<T> serviceClass,
                                        RpcArgumentConverter rpcArgumentConverter,
                                        RpcReturnValueHandlerFactory rpcReturnValueHandlerFactory,
                                        EventBusService eventBusService,
                                        SecurityContext securityContext,
                                        ObjectProvider<ServiceDirectory> serviceDirectoryProvider,
                                        RequestLivenessWatcher requestLivenessWatcher,
                                        Vertx vertx,
                                        ClassLoader classLoader,
                                        OpenTelemetry openTelemetry,
                                        TraceLogFilter traceLogFilter) {

        Validate.notNull(serviceIdentifier, "serviceIdentifier must not be null");
        Validate.notBlank(nodeName, "nodeName must not be blank");
        Validate.notNull(serviceClass, "serviceClass must not be null");
        Validate.notNull(rpcArgumentConverter, "argumentConverter must not be null");
        Validate.notNull(rpcReturnValueHandlerFactory, "returnValueHandlerFactory must not be null");
        Validate.notNull(eventBusService, "eventBusService must not be null");
        Validate.notNull(securityContext, "securityContext must not be null");
        Validate.notNull(serviceDirectoryProvider, "serviceDirectoryProvider must not be null");
        Validate.notNull(requestLivenessWatcher, "requestLivenessWatcher must not be null");
        Validate.notNull(vertx, "vertx must not be null");
        Validate.notNull(classLoader, "classLoader must not be null");
        Validate.notNull(openTelemetry, "openTelemetry must not be null");
        Validate.notNull(traceLogFilter, "traceLogFilter must not be null");

        this.serviceIdentifier = serviceIdentifier;
        this.nodeName = nodeName;
        String encodedNodeName = KinoticUtil.safeEncodeURI(nodeName);
        this.serviceClass = serviceClass;
        this.rpcArgumentConverter = rpcArgumentConverter;
        this.rpcReturnValueHandlerFactory = rpcReturnValueHandlerFactory;
        this.eventBusService = eventBusService;
        this.securityContext = securityContext;
        this.serviceDirectoryProvider = serviceDirectoryProvider;
        this.requestLivenessWatcher = requestLivenessWatcher;
        this.tracer = openTelemetry.getTracer(INSTRUMENTATION_NAME);
        this.propagator = openTelemetry.getPropagators().getTextMapPropagator();
        this.traceLogFilter = traceLogFilter;
        this.vertx = vertx;

        this.handlerCRI = CRI.create(EventConstants.REPLY_DESTINATION_SCHEME, encodedNodeName + ":" + UUID.randomUUID(), KinoticUtil.safeEncodeURI(serviceClass.getName())+"RpcProxyResponseHandler");

        // Verify that a proxy can be built supporting all methods of the provided serviceClass
        ReflectionUtils.doWithMethods(serviceClass, method -> {

            if(!rpcReturnValueHandlerFactory.supports(method)){
                throw new IllegalArgumentException("The method: "+ method +" does not have a supported RpcReturnValueHandlerFactory");
            }

            // find any parameters annotated with @Scope
            // we populate this map upfront to speed up invocations later
            Integer parameterIndexWithScopeAnnotation = MetaUtil.findParameterIndexWithScopeAnnotation(method);
            if(parameterIndexWithScopeAnnotation != null){
                methodsWithScopeAnnotation.put(method, parameterIndexWithScopeAnnotation);
            }

        }, ReflectionUtils.USER_DECLARED_METHODS);

        serviceProxy = serviceClass.cast(Proxy.newProxyInstance(classLoader, new Class[] { serviceClass, RpcServiceProxy.class}, this));

        /*
          Response handler logic to correlate response from remote service invocation's
         */
        replyEventConsumer = eventBusService.listen(this.handlerCRI);
        replyEventConsumer.handler(event -> {

                    String correlationId = event.metadata().get(EventConstants.CORRELATION_ID_HEADER);
                    if(correlationId != null){
                        // one lookup: a cancel or a lost node can remove the entry from another context
                        RpcReturnValueHandler handler = responseMap.get(correlationId);
                        if(handler != null){
                            try {
                                if(handler.processResponse(event)){
                                    settle(correlationId);
                                }
                            } catch (Exception e) {
                                log.error("URGENT: Unhandled exception in RpcReturnValueHandler.processResponse, Proxy Will be Released!!", e);
                                release();
                            }
                        }else{
                            reapOrphanedStream(event, correlationId);
                        }

                    }else{
                        log.error("Received Message with no " + EventConstants.CORRELATION_ID_HEADER +" header");
                    }
                })
                .exceptionHandler(throwable -> log.error("Reply Event listener error", throwable))
                // Vert.x invokes the end handler on every unregistration, including the one release() performs.
                // release() sets released before unregistering, so reaching the log means something other than
                // release() unregistered the consumer and no in-flight response can be correlated any more.
                .endHandler(_ -> {
                    if(!released.get()){
                        log.warn("Reply event listener for {} was unregistered without a call to release()",
                                 serviceClass.getName());
                    }
                });
    }

    @Override
    public T getService() {
        return serviceProxy;
    }

    /**
     * Starts the span covering one outbound service invocation. It ends when the invocation settles,
     * which {@link TracingRpcReturnValueHandler} decides.
     * @param method being invoked on the remote service
     * @param scope the scope the invocation is routed to, or null when the service is not scoped
     * @return a started {@link Span}
     */
    private Span startInvocationSpan(Method method, String scope){
        String serviceName = serviceIdentifier.qualifiedName();
        Span ret = tracer.spanBuilder(serviceIdentifier.name() + "/" + method.getName())
                         .setSpanKind(SpanKind.CLIENT)
                         .setAttribute(TelemetryUtil.RPC_SYSTEM, TelemetryUtil.SYSTEM_VALUE)
                         .setAttribute(TelemetryUtil.RPC_SERVICE, serviceName)
                         .setAttribute(TelemetryUtil.RPC_METHOD, method.getName())
                         .startSpan();
        if(scope != null){
            ret.setAttribute("kinotic.scope", scope);
        }
        return ret;
    }

    @Override
    public void release() {
        if(released.compareAndSet(false,true)){
            replyEventConsumer.unregister();

            for(String correlationId : responseMap.keySet()){
                RpcReturnValueHandler returnValueHandler = responseMap.remove(correlationId);
                if(returnValueHandler != null){
                    requestLivenessWatcher.settle(correlationId);
                    returnValueHandler.cancel(serviceClass.getSimpleName() + " released. No further responses will be processed");
                }
            }
            recentlyReaped.clear();
        }
    }

    /**
     * Handles a reply for a correlationId this proxy no longer tracks by cancelling the orphaned stream.
     * The server can't detect such an abandoned stream itself, since all of this proxy's requests share one
     * reply destination, so a cancel goes to the origin CRI the server sends on stream replies.
     */
    private void reapOrphanedStream(Event<byte[]> event, String correlationId){
        String originCri = event.metadata().get(EventConstants.ORIGIN_CRI_HEADER);
        if(originCri == null){
            log.error("Received Message for correlationId: {} but no response handler is set", correlationId);
            return;
        }
        // recentlyReaped debounces the burst of in-flight values that arrive before the server stops; the
        // timer expires the entry so a lost cancel self-heals on the next stray value.
        if(recentlyReaped.add(correlationId)){
            vertx.setTimer(REAP_DEBOUNCE_MS, _ -> recentlyReaped.remove(correlationId));

            sendCancel(CRI.create(originCri), correlationId);
        }
    }

    // Published, not sent: the instance producing the stream is whichever one round-robin gave the request,
    // so every instance of the service gets the cancel and the ones without the stream ignore it
    private void sendCancel(CRI serviceCri, String correlationId){
        Metadata metadata = Metadata.create();
        metadata.put(EventConstants.CONTROL_HEADER, EventConstants.CONTROL_VALUE_CANCEL);
        metadata.put(EventConstants.CORRELATION_ID_HEADER, correlationId);
        eventBusService.publish(Event.create(serviceCri, metadata, null));
    }

    /**
     * Ends the tracking of a request whose response handler is done: it is no longer pinned to the node
     * that acknowledged it and no longer receives replies.
     */
    private void settle(String correlationId){
        // the entry goes first: once it is gone the ack's computeIfPresent can no longer pin, so the lease
        // released next is the last one this request can have
        responseMap.remove(correlationId);
        requestLivenessWatcher.settle(correlationId);
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        Object ret;
        // The JDK proxy dispatches equals, hashCode and toString here with Object as the declaring class, even
        // when a proxy interface redeclares one of them. None of them reach the remote end, so they are answered
        // ahead of the released guard: logging or collecting a released proxy must not fail.
        if(method.getDeclaringClass() == Object.class){

            // a handle mints exactly one proxy, so the proxy's identity is the handle's identity
            ret = switch (method.getName()) {
                case "equals" -> proxy == args[0];
                case "hashCode" -> System.identityHashCode(proxy);
                default -> toString();
            };

        }else if(!released.get()){

            // Get all data for remote invocation. If anything fails in this step the error automatically props up
            // This way no ReturnValueHandler is created until message is ready to get dispatched to remote end


            // if there is a scope parameter remove it from args to be sent, and store it as part of the CRI
            Integer scopeParameter = methodsWithScopeAnnotation.get(method);
            String scope = (scopeParameter != null ? args[scopeParameter].toString() : null); // effectively final.. lol
            if(scopeParameter != null){
                args = ArrayUtils.remove(args, scopeParameter);
            }

            // convert arguments to be sent
            byte[] argumentData = rpcArgumentConverter.convert(method, args);
            String correlationId = UUID.randomUUID().toString();

            Span span = startInvocationSpan(method, scope);

            // Now create response handler and store, so we can propagate response in replyMessageConsumer
            RpcReturnValueHandler handler = new TracingRpcReturnValueHandler(
                    rpcReturnValueHandlerFactory.createReturnValueHandler(method, args), span);
            responseMap.put(correlationId, handler);
            // release() may have scanned the map between the guard above and this put
            if(released.get()){
                responseMap.remove(correlationId);
                handler.cancel(serviceClass.getSimpleName() + " released. No further responses will be processed");
                throw new IllegalStateException("RpcServiceProxyHandle has already been released. No service method can be called after release.");
            }

            // Create Event to be sent to remote end to cause service invocation
            Metadata metadata = Metadata.create();
            metadata.put(EventConstants.REPLY_TO_HEADER, handlerCRI.raw());
            metadata.put(EventConstants.CORRELATION_ID_HEADER, correlationId);
            metadata.put(EventConstants.CONTENT_TYPE_HEADER, rpcArgumentConverter.producesContentType());

            // Carries this span to the remote end, which continues the trace instead of starting one
            propagator.inject(Context.current().with(span), metadata, TelemetryUtil.METADATA_SETTER);

            // TODO: use version string to determine how specific the invocation has to be like npm semantics ^1.0.0 ect
            CRI requestCri = CRI.create(EventConstants.SERVICE_DESTINATION_SCHEME,
                                        scope,
                                        serviceIdentifier.qualifiedName(),
                                        "/" + method.getName(),
                                        serviceIdentifier.version());

            if(log.isTraceEnabled() && !traceLogFilter.isExcluded(requestCri.raw())){
                log.trace("Proxy for {} Method Invoked {}", serviceClass.getSimpleName(), method.toString());
            }

            // Propagate the participant active on the calling context so the callee sees the originator.
            Event<byte[]> rpcOutboundEvent = Event.create(requestCri,
                                                          metadata,
                                                          argumentData,
                                                          securityContext.currentParticipant());

            ret = handler.getReturnValue(new RpcRequest() {
                @Override
                public void send() {
                    // Send data to remote end to trigger service invocation
                    eventBusService.sendWithAck(rpcOutboundEvent)
                                   .onComplete(ar -> {
                                       if(ar.failed()) {
                                           // send failed, signal handler so failure can be relayed to the return value
                                           try{

                                               responseMap.remove(correlationId);

                                               Throwable throwable = KinoticUtil.mapSendFailure(ar.cause(),
                                                                                                requestCri,
                                                                                                serviceDirectoryProvider.getIfAvailable());
                                               handler.processError(throwable);
                                           }catch (Exception e){
                                               log.error("URGENT: Unhandled exception in RpcReturnValueHandler.processError, Proxy Will be Released!!", e);
                                               release();
                                           }
                                       } else {
                                           // computeIfPresent serializes with settle() on this key, so a reply that
                                           // lands before the acknowledgement is processed leaves nothing pinned
                                           responseMap.computeIfPresent(correlationId, (_, pending) -> {
                                               requestLivenessWatcher.watch(correlationId, ar.result(), () -> failLost(correlationId, requestCri, ar.result()));
                                               return pending;
                                           });
                                       }
                                   });
                }

                @Override
                public void cancelRequest() {
                    if(handler.isMultiValue()) {
                        sendCancel(requestCri, correlationId);
                        settle(correlationId);
                    } else {
                        throw new IllegalStateException("Cancel is not supported if RpcReturnValueHandler.isMultiValue returns false");
                    }
                }
            });

        }else{
           throw new IllegalStateException("RpcServiceProxyHandle has already been released. No service method can be called after release.");
        }
        return ret;
    }

    // Runs on the request's context when the node that took it leaves the cluster. Only the party that
    // removes the handler signals it, so a reply that settled the request first leaves nothing to fail.
    private void failLost(String correlationId, CRI requestCri, String nodeId){
        RpcReturnValueHandler lost = responseMap.remove(correlationId);
        if(lost != null){
            lost.processError(new RpcServiceUnavailableException("Node " + nodeId + " left the cluster while serving the request to " + requestCri.raw()));
        }
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("serviceIdentifier", serviceIdentifier)
                .append("handlerCRI", handlerCRI)
                .toString();
    }

}
