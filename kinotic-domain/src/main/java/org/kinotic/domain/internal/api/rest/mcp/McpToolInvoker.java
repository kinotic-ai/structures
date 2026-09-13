package org.kinotic.domain.internal.api.rest.mcp;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.kinotic.core.api.directory.McpToolDefinition;
import org.kinotic.core.api.directory.ServiceDirectory;
import org.kinotic.core.api.event.*;
import org.kinotic.core.api.exceptions.RpcMissingServiceException;
import org.kinotic.core.api.exceptions.RpcServiceUnavailableException;
import org.kinotic.core.api.security.Participant;
import org.kinotic.core.api.service.RequestLivenessWatcher;
import org.kinotic.core.api.utils.KinoticUtil;
import org.kinotic.domain.api.model.security.participant.ParticipantScope;
import org.kinotic.domain.api.model.security.participant.ScopedParticipant;
import org.kinotic.domain.api.model.security.ZoneRules;
import org.kinotic.domain.internal.api.rest.mcp.model.McpCallToolResult;
import org.springframework.stereotype.Component;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.ObjectNode;

import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Dispatches an MCP {@code tools/call} through the existing RPC path: the tool resolves to its stored CRI, the
 * arguments object is forwarded verbatim with the named-arguments content type, and the single reply becomes the
 * tool result.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class McpToolInvoker {

    private final EventBusService eventBusService;
    private final JsonMapper jsonMapper;
    private final ConcurrentHashMap<String, Promise<McpCallToolResult>> pendingCalls = new ConcurrentHashMap<>();
    private final CRI replyCri = CRI.create(EventConstants.REPLY_DESTINATION_SCHEME,
                                            UUID.randomUUID().toString(),
                                            "org.kinotic.gateway.McpToolInvoker");
    private final ServiceDirectory serviceDirectory;
    private final RequestLivenessWatcher requestLivenessWatcher;
    private volatile boolean ready = false;
    private EventConsumer replyConsumer;

    @PostConstruct
    void init() {
        replyConsumer = eventBusService.listen(replyCri);
        replyConsumer.handler(replyEvent -> {

            String correlationId = replyEvent.metadata().get(EventConstants.CORRELATION_ID_HEADER);
            Promise<McpCallToolResult> pending = correlationId != null ? pendingCalls.remove(correlationId) : null;
            if (correlationId != null) {
                requestLivenessWatcher.settle(correlationId);
            }
            if (pending == null) {
                // a reply whose pending entry is gone (its send already failed) has no caller to complete
                log.debug("Discarding MCP reply with correlation id {}", correlationId);
            } else if (replyEvent.metadata().contains(EventConstants.ERROR_HEADER)) {
                pending.complete(McpCallToolResult.error(replyEvent.metadata().get(EventConstants.ERROR_HEADER)));
            } else {
                byte[] data = replyEvent.data();
                pending.complete(McpCallToolResult.text(data != null ? new String(data, StandardCharsets.UTF_8) : "null"));
            }
        });
        replyConsumer.completion()
                     .onSuccess(v -> ready = true)
                     .onFailure(throwable -> log.error("MCP reply consumer registration failed, tools/call is disabled", throwable));
    }

    @PreDestroy
    void shutdown() {
        replyConsumer.unregister();
    }

    /**
     * Invokes the named tool for the given participant and completes with the MCP {@code tools/call} result node.
     * Service failures (offline, invocation error) complete normally with an {@code isError} result; an
     * unknown tool fails the future with {@link IllegalArgumentException}.
     * @param toolName the MCP tool name to invoke
     * @param arguments the MCP arguments object, forwarded verbatim to the service
     * @param participant the authenticated caller, propagated as the event sender
     * @return a future completing with the {@code tools/call} result node
     */
    public Future<McpCallToolResult> invoke(String toolName, ObjectNode arguments, ScopedParticipant participant) {
        if (!ready) {
            return Future.succeededFuture(McpCallToolResult.error("The MCP endpoint is not ready"));
        }
        ParticipantScope callerScope = participant.getScope();
        // resolution uses the caller-visible query, so zone visibility is enforced by the lookup itself
        return serviceDirectory.findMcpToolByName(toolName, callerScope.organizationId(), callerScope.applicationId())
                               .compose(tool -> {
                                   Future<McpCallToolResult> ret;
                                   if (tool == null) {
                                       ret = Future.failedFuture(new IllegalArgumentException("Unknown tool: " + toolName));
                                   } else {
                                       ret = dispatch(tool, arguments, participant);
                                   }
                                   return ret;
                               });
    }

    private Future<McpCallToolResult> dispatch(McpToolDefinition tool, ObjectNode arguments, Participant participant) {
        CRI requestCri = CRI.create(tool.getCri());
        // defense in depth over the zone visibility filter in findMcpToolByName: the resolved CRI must pass
        // the same zone send rules StompAuthorizer enforces, so a directory or query defect can never
        // dispatch across zones — logged as a server fault, answered as an unknown tool
        if (!ZoneRules.from(participant).sendAllowed(requestCri)) {
            log.error("MCP tool '{}' resolved to CRI {} which participant {} may not address",
                      tool.getName(), tool.getCri(), participant.getId());
            return Future.failedFuture(new IllegalArgumentException("Unknown tool: " + tool.getName()));
        }

        String correlationId = UUID.randomUUID().toString();
        Promise<McpCallToolResult> ret = Promise.promise();
        pendingCalls.put(correlationId, ret);

        Metadata metadata = Metadata.create();
        metadata.put(EventConstants.REPLY_TO_HEADER, replyCri.raw());
        metadata.put(EventConstants.CORRELATION_ID_HEADER, correlationId);
        metadata.put(EventConstants.CONTENT_TYPE_HEADER, EventConstants.CONTENT_TYPE_NAMED_JSON);
        Event<byte[]> event = Event.create(requestCri,
                                           metadata,
                                           jsonMapper.writeValueAsBytes(arguments),
                                           participant);
        eventBusService.sendWithAck(event)
                       .onComplete(ar -> {
                           if (ar.failed()) {
                               // only the party that removes the entry completes it: a reply can land before an
                               // acknowledgement times out
                               Promise<McpCallToolResult> unsent = pendingCalls.remove(correlationId);
                               if (unsent != null) {
                                   // mapSendFailure also reports unreachability to the directory on NO_HANDLERS
                                   Throwable mapped = KinoticUtil.mapSendFailure(ar.cause(), requestCri, serviceDirectory);
                                   if (mapped instanceof RpcMissingServiceException) {
                                       unsent.complete(McpCallToolResult.error("Service is offline: " + tool.getCri()));
                                   } else if (mapped instanceof RpcServiceUnavailableException) {
                                       unsent.complete(McpCallToolResult.error("Service became unavailable: " + tool.getCri()));
                                   } else {
                                       log.warn("MCP tool '{}' dispatch to {} failed", tool.getName(), tool.getCri(), ar.cause());
                                       unsent.complete(McpCallToolResult.error(ar.cause().getMessage()));
                                   }
                               }
                           } else {
                               // computeIfPresent serializes with the reply handler's remove, so a reply that
                               // lands before the acknowledgement is processed leaves nothing pinned
                               pendingCalls.computeIfPresent(correlationId, (_, pending) -> {
                                   requestLivenessWatcher.watch(correlationId, ar.result(), () -> {
                                       // only the party that removes the entry completes it
                                       Promise<McpCallToolResult> lost = pendingCalls.remove(correlationId);
                                       if (lost != null) {
                                           lost.complete(McpCallToolResult.error("Service became unavailable: " + tool.getCri()));
                                       }
                                   });
                                   return pending;
                               });
                           }
                       });

        return ret.future();
    }


}
