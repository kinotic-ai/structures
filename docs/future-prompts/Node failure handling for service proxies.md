# Node failure handling for service proxies — phase plan

Plan of record for making a proxy call fail when the node serving it dies, instead of hanging
forever. Phase 1 landed in PR #476; Phase 2 in #540; Phase 3 in #544; Phase 4 is PR #545; Phase 5 is PR #546, based on #545; Phase 6 in #547; Phases 7 and 8 were built as #548 and #549 and dropped, see their section; the fail-on-disconnect rule that replaces them is PR #551; Phase 9 is PR #552, based on #551 (#550 was closed with the dropped Phase 8 base); the wrap-up is PR #553, based on #552; TS service streaming with cancel is PR #554, based on #553; the review fixes are PR #558. Everything below was re-validated against `develop` at `3adf17d`
(2026-09-08); the adjustments that pass produced are folded in, and the phase numbering below
supersedes the earlier chat numbering (mapping at the end).

Process: one PR per phase, review and merge before the next starts, ~10 files per phase. No
phase may rewrite, refactor, or restructure what an earlier phase produced; if a later phase
would force that, the earlier phase drew its boundary wrong and the plan is revisited first.

## The problem, in code

`sendWithAck` acks receipt, never completion:

```java
// DefaultEventConsumer.java:44-46
if (message.replyAddress() != null) {
    message.reply(null);            // "bytes received", nothing more
}
```

so every pending-request map in the system waits without a lifeline once the callee is gone:

```java
// DefaultRpcServiceProxyHandle.java — removed only on reply, send failure, or release()
private final ConcurrentHashMap<String, RpcReturnValueHandler> responseMap = new ConcurrentHashMap<>();
// McpToolInvoker.pendingCalls has the same shape; the TS EventBus holds an rxjs subscription per correlationId
```

A call issued *after* the callee's registration is gone already fails fast (`NO_HANDLERS` →
`RpcMissingServiceException`). A call in flight *when* the callee dies hangs, in every runtime.

## The design

A service's registration in the clustered event bus *is* its liveness: Ignite removes a dead
node's entries from `__vertx.subs`, and `KinoticIgniteClusterManager.statusFlux()` already turns
that into a push signal. The callee side already uses it in one direction:

```java
// ServiceInvocationSupervisor.java — callee watches the CALLER's reply address, cancels streams on INACTIVE
Flux<ListenerStatus> replyListenerStatus = eventBusService.monitorListenerStatus(replyCRI);
```

The plan makes that symmetric and universal: **every pending request becomes a watch on the
registration that serves it, at whichever hop already has cluster visibility.** No timeouts, no
timers, no per-call policy; a legitimate ten-hour call is never killed, and a dead node fails its
pending calls within the cluster's failure-detection window.

| Caller | Pending state lives | Who watches |
|---|---|---|
| Java proxy, MCP invoker | caller's JVM | the JVM, through one shared watcher (Phase 3) |
| TS client (UI, TS service) via STOMP | TS client, which cannot see the cluster | the **gateway**, on the client's behalf (Phase 4) |
| any caller of a TS service that dies while its gateway lives | — | the **gateway**, from the socket close (Phase 5) |

Semantics: two exceptions under one parent. `RpcMissingServiceException` (exists): rejected at
send, never executed. `RpcServiceUnavailableException` (Phase 3): the callee vanished mid-call,
may or may not have executed. Callers of non-idempotent operations need the distinction.

What this deliberately does not cover: a callee that is alive, registered, and wedged. No
liveness signal exists for that; the pending-count metric the watcher exposes is how it becomes
visible. If it ever needs bounding, it composes as a callee-declared deadline, later.

## Phase 1 — wire contract (PR #476)

Every terminal reply carries `control: complete` on the reply event itself: single-value replies
from the Java supervisor and the TS `BasicReturnValueConverter` carry it alongside their data,
stream completion events already did, error replies stay terminal via the `error` header. Any hop
holding per-request state can release it on that one event without knowing the method's shape.
Receivers accept marked and unmarked replies. `KinoticUtil.mapSendFailure` is the one
NO_HANDLERS mapping (proxy, gateway, MCP) and the proxy path now reports unreachability.

## Phase 2 — the ack names the node (~7 files)

Address-level watching alone has a blind spot: any address with more than one registration
round-robins, and it stays ACTIVE while *any* instance survives. That is every unscoped service
published on more than one node — Java on N JVMs, or N TS instances of one service — and also the
shared unscoped address a scoped service joins for its `@ScopeOptional` methods, which behaves
exactly like any other multi-instance address:

```java
// ServiceInvocationSupervisor.java:139-144
methodInvocationEventConsumer = listenAt(serviceDescriptor.serviceIdentifier().cri());              // srv://<scope>@...   one instance
if(serviceDescriptor.serviceIdentifier().scope() != null && !scopeOptionalMethodIds.isEmpty()){
    unscopedInvocationEventConsumer = listenAt(serviceDescriptor.serviceIdentifier().unscopedCri()); // srv://...          every instance
}
```

However an address comes to hold N registrations, a call to it lands on one of them, so the
caller has to know **which** registration took its request. The ack is where that is learned, and it costs nothing on the wire beyond the reply
body the ack already sends:

```java
// DefaultEventConsumer — the ack carries the id of the node whose consumer took the request
if (message.replyAddress() != null) {
    message.reply(nodeId);          // clusterManager.getNodeId(), the same id DefaultKinotic.serverInfo uses
}
```

```java
// EventBusService — the ack's payload reaches the sender
Future<String> sendWithAck(Event<byte[]> event);     // completes with the acking node id (was Future<Void>)

// KinoticIgniteClusterManager — a NodeListener on the discovery events vertx-ignite already
// subscribes to (EVT_NODE_JOINED, EVT_NODE_LEFT, EVT_NODE_FAILED) emits the cluster membership
Flux<Set<String>> clusterNodesFlux();
// exposed as EventBusService.monitorClusterNodes()
```

Membership, not the address's registration set, is what a pinned lease checks against. A JVM
unregisters an address while it is still up — `ServiceRegistrationBeanPostProcessor` runs
`ServiceInvocationSupervisor.stop()` on bean destruction, and an invocation already running there
still replies — so the registration set fires a false failure on every rolling restart, at the
exact moment Phase 3's drain wants callers to keep waiting. `nodeLeft` fires only when the node is
gone: at once after a graceful leave, after a crash once Ignite's `failureDetectionTimeout` (10 s)
expires. Cost is one N-sized snapshot per membership change per node, however many addresses have
calls in flight; the registration set would have cost a k-sized set per monitored address, since
every platform address is registered on all k nodes.

Nothing watches the address before the ack. `sendWithAck` is a Vert.x `request`, which fails with
`NO_HANDLERS` at once when nothing is registered and with `TIMEOUT` after `DeliveryOptions`'
default 30 s when the ack never arrives — a bound on one network hop, since `DefaultEventConsumer`
acks before dispatching, never on the invocation.

Files: `DefaultEventConsumer`, `DefaultEventBusService`, `EventBusService`,
`KinoticIgniteClusterManager`, the four `sendWithAck` call sites adapting to the new return type
(`DefaultRpcServiceProxyHandle`, `McpToolInvoker`, `DefaultEventService`,
`EndpointConnectionHandler`), `EventBusServiceTests` (the membership names the local node;
the ack names the local node).

Why not `RegistrationInfo.seq()`: it exists, but a `MessageConsumer` never learns its own seq, so
identifying below node granularity from the consumer side needs Vert.x internals. Node id plus
the gateway-side synthesis in Phase 5 covers every case without them.

## Phase 3 — the watcher, in kinotic-core (~11 files)

One component every in-cluster caller uses, in `api/service` because the MCP invoker lives in
`kinotic-domain`:

```java
public interface RequestLivenessWatcher {
    void watch(String correlationId, String nodeId, Runnable onLost);   // pin at the ack
    void settle(String correlationId);                                   // release when the reply settles it
    int pendingCount();                                                  // the rpc.pending.requests gauge
}
```

Keyed by correlation id rather than handing out lease objects, so the two callers keep no lease
bookkeeping of their own: each pins in the ack handler and settles wherever it already removes
the request from its own map.

A lease starts at the ack. Until then Vert.x owns the outcome (`NO_HANDLERS` or `TIMEOUT` on the
`request`, see Phase 2), and `KinoticUtil.mapSendFailure` maps `TIMEOUT` to
`RpcServiceUnavailableException`, because an ack lost in flight cannot rule out that the handler
ran. From the ack on, the lease is pinned to the node id it named and fails when that id leaves
`monitorClusterNodes()`. The address may stay ACTIVE on other instances, and the node may already
have unregistered the address to drain; the lease holds until the node is gone.

Coverage of the round-robin cases:

| Call | Ack names | Fails when |
|---|---|---|
| scoped `srv://<scope>@…` | the one node | that node leaves the cluster |
| unscoped, N Java instances (one per JVM) — a service on N nodes, or `@ScopeOptional` methods | the JVM that took it | that JVM leaves the cluster |
| unscoped, N TS instances behind gateways | the **gateway** holding that instance's subscription | that gateway dies (this phase); the instance dies while its gateway lives (Phase 5) |

Wired into `DefaultRpcServiceProxyHandle` (pinned inside `responseMap.computeIfPresent` so a reply
that lands before the ack is processed leaves nothing pinned; `onLost` removes the handler and calls
`processError(new RpcServiceUnavailableException(...))`, and only the party that removes the handler
signals it) and `McpToolInvoker`. `onLost` only *dispatches* the failure to the request's own context, never
runs continuations on the cluster manager's delivery context: under a death burst that loop must
drain in queue submissions, not user code. One membership subscription per JVM, held for the
watcher's lifetime, and leases keyed by the node id they are pinned to, so a membership change costs
one lookup per pinned node. Non-clustered guards are gone from `develop` (#496), so there is no
fallback path.

Files: `RequestLivenessWatcher`, `DefaultRequestLivenessWatcher`, `RpcServiceUnavailableException`,
`KinoticUtil`, `DefaultRpcServiceProxyHandle`, `DefaultServiceRegistry`, `McpToolInvoker`,
`ServiceInvocationSupervisor`, `ServiceRegistrationBeanPostProcessor`, `RpcLivenessTests` +
`DrainTestService` (a second Ignite node in the test JVM hosts an address, acks, and leaves mid-call
→ `RpcServiceUnavailableException`; the local service unregistering mid-call → the reply still
arrives and `unregister` waits for it; a stream cut by `stop()` → `RpcServiceUnavailableException`;
unrelated membership churn → no false failure).

The proxy constructor grows by one parameter, the way `develop` grew it for `TraceLogFilter`;
it is not restructured.

The callee side of the same phase: `ServiceInvocationSupervisor.stop()` drains instead of cutting.

```java
// ServiceInvocationSupervisor.stop() — Phase 3
Future<Void> ret = methodInvocationEventConsumer.unregister()          // 1. stop accepting
    .compose(v -> inFlight.drained());                                 // 2. single-value invocations reply first
for(... : activeStreamingResults.entrySet()){
    streamSubscribers.getValue().fail(new RpcServiceUnavailableException(...));   // 3. streams end with a terminal error
}
```

Today `stop()` cancels streams with no terminal reply and returns before running invocations have
replied; with membership as the post-ack signal a cut stream's caller would otherwise wait for the
JVM to exit. The supervisor only tracked streams, so it gains an in-flight count for single-value invocations:
the synchronous handler run plus every pending single-value reactive result. The drain is bounded
by `DRAIN_TIMEOUT_MS` (30 s), a constant until an environment needs a different value, never a
per-call bound. The drain only helps while Vert.x and Ignite are still up, and
`ServiceRegistrationBeanPostProcessor.postProcessBeforeDestruction` runs on the service bean's
destruction, where a published bean that injects nothing from kinotic had no dependency edge forcing
that order; the post processor now records each published bean as dependent on the registry, so
Spring destroys it, and drains it, before the registry and the Vert.x behind it.

## Phase 4 — gateway, caller side: session state, leases, session touch (~10 files)

**The boundary-critical phase.** Introduce a per-connection `ReplySessionState` owning what is spread through `EndpointConnectionHandler`
today — the `reply://` consumer and, new, the pending-request records
`(correlationId, metadata, destination)` with a Phase 3 lease each. On lease loss, synthesize the
error reply through the *existing* recover path (`exceptionConverter.convert` → `send`); release
leases on terminal-marked replies observed in `StompSubscriptionEventSubscriber`. Lifecycle is
socket-bound (`shutdown()` disposes it), and disposal is one call on a self-contained object.

Also here, because parking cannot be built on it otherwise: the session touch on the
WebSocket path never reaches the store.

```java
// EndpointConnectionHandler.java:318-325 — and startSessionTouchTimer does the same
private void signalActivity() {
    if (sessionKeepAliveMode == SessionKeepAliveMode.ACTIVITY) {
        session.setAccessed();      // mutates the local copy; SessionHandler only flushes on response end,
    }                               // which on a WebSocket happened once, at the 101 upgrade
}
```

The clustered entry's TTL is frozen at upgrade, so an `ACTIVITY`/`CONNECTION` session can expire
under a live connection (the sizing doc's open item 7). Fix: an explicit store put where the touch
happens, pinned by a test that a session outlives its timeout under an active connection.

As built:

```java
// ReplySessionState — one per connection, owned by EndpointConnectionHandler
void subscribe(CRI cri, String subscriptionIdentifier, StompSubscriptionHandler handler);  // reply destinations, settling each terminal reply on the way through
boolean unsubscribe(String subscriptionIdentifier);
void track(Event<byte[]> request);              // at send: records the reply metadata under the correlation id; a cancel settles instead
void pin(String correlationId, String nodeId, CRI destination);   // at the ack, inside computeIfPresent so a reply that beat the ack leaves nothing pinned
void settle(String correlationId);              // terminal reply, failed send, or cancel
void dispose();                                 // shutdown(): settle everything, unregister the reply subscriptions
```

Terminal replies settle inside the reply consumer's handler rather than in
`StompSubscriptionEventSubscriber`, so that class is untouched and the state stays self-contained. A lost node answers through `exceptionConverter.convert` → `eventBusService.send`
on the recorded reply metadata, the same path a failed send takes.

The session touch writes through `SessionStore.put` at most once per quarter of the session
timeout, from both the ACTIVITY touch and the CONNECTION timer. A put that fails on the store's
version check, because a request on the same cookie flushed a newer copy, adopts the stored copy
and retries once.

Files: `ReplySessionState`, `EndpointConnectionHandler`, `Services` (+ `RequestLivenessWatcher`,
`SessionStore`), the gateway's test dependencies (`spring-boot-starter-test`, `vertx-ignite`),
`EndpointConnectionHandlerTests` on a clustered Vert.x of its own (a lost node answers the reply
destination with `RpcServiceUnavailableException`; a terminal reply settles the request; shutdown
settles everything; a session under an open connection outlives its timeout, and expires after the
connection closes).

## Phase 5 — gateway, callee side: heartbeats and instance-level synthesis (~7 files)

Two things the gateway alone can see.

**Dead sockets.** Every socket-derived signal in this plan assumes the gateway notices a closed
connection. A hard-killed VM sends no FIN and the server sets no STOMP heartbeats:

```java
// ApiGatewayVertcleFactory.java:82 — no heartbeat configuration
StompServerOptions stompServerOptions = new StompServerOptions()
        .setWebsocketPath(STOMP_WEBSOCKET_PATH)
        .setMaxBodyLength(properties.getMaxEventPayloadSize());
```

The client already offers `heartbeatOutgoing: 30000`. Enforce it server-side (verify
vertx-stomp-lite's option) so a vanished workload's connection closes deterministically within
about two heartbeat intervals, and its `srv://` registration with it. Property beside
`sessionTimeout` and `sessionCookieSameSite` on `ApiGatewayProperties`.

**A TS instance dying while its gateway lives.** The Phase 2 ack names the gateway, so a
node-level lease cannot tell one TS instance on that gateway from another. The gateway can: it
forwards every invocation to a specific socket, and the terminal reply comes back through
`EndpointConnectionHandler.send()` on the reply scheme. Per `srv://` subscription, track each
delivered invocation `(correlationId, reply-to)` until its terminal reply passes back; on
`closed()`, synthesize `RpcServiceUnavailableException` to every reply-to still tracked, through
the same synthesis path as Phase 4. This closes the last row of the Phase 3 table and is also the
fastest signal there is for the scoped VM-node case — a socket close, ahead of Ignite's
failure-detection window.

As built. The premise on heartbeats was stale: vertx-stomp-lite already negotiates them, with a
server default of 30 s both ways, and closes a connection silent for two intervals through
`handler.closed()`. The phase set the same 30 s explicitly as `ApiGatewayProperties.stompHeartbeat`;
the property was removed again in PR #554: the interval is a protocol agreement with the TS client,
which offers 30 s both ways, and STOMP negotiates each direction to the larger offer, so a
deployment could only widen it. The library default stands. `StompHeartbeatTests`, which pinned
the library's own close on silence, went with it: that is vertx-stomp-lite's behaviour to test.

```java
// ServiceSessionState — the callee side of one connection, sibling of ReplySessionState
void deliver(Event<byte[]> event, StompSubscriptionHandler handler);  // in the srv:// subscription handler; a cancel forgets its invocation
void observeReply(Event<byte[]> reply);      // in send() on the reply scheme: a terminal reply settles, a stream value starts the requester watch
void dispose();                              // shutdown(): RpcServiceUnavailableException to every reply-to still outstanding
```

`EventUtil.replyMetadataOf` and `EventUtil.isTerminalReply` carry the two rules both session
states share.

Files: `ApiGatewayProperties`, `ApiGatewayVertcleFactory`, `ServiceSessionState`,
`EndpointConnectionHandler`, `EventUtil`, `ReplySessionState` (uses the shared helpers),
`EndpointConnectionHandlerTests` (a closed connection answers the invocations it still owes; a
terminal reply back through the connection settles one).

## Phase 6 — TS client edges (~4 files)

`develop` already closed two of the three: requests are no longer queued while disconnected
(`send()` requires `connected`, `retryIfDisconnected: false`), and the stale `replyToId` replay
on re-activation is skipped. What remains: typed error bodies so `RpcServiceUnavailableException`
crosses to TS as more than a message string (the `EventBus.ts` TODO); the `NONE` keep-alive edge
where a network drop keeps the session and resurrects the old `replyToId` until expiry; and the
vm-manager's overlapping heartbeat `setInterval`.

Failing in-flight calls on a reconnect is the rule the dropped Phases 7 and 8 settled on; see
their section.

As built: `RpcError` (`api/event`, exported) carries `exceptionName` and `exceptionClass` parsed
from the `ServiceExceptionWrapper` body of an error reply, falling back to the error header;
`EventBus.requestStream` rejects with it. The `NONE` edge was a gateway defect, not a client one:
`removeSession()` only marked the session destroyed, and nothing flushes a destroyed session on
the WebSocket path, so the store entry and its `replyToId` survived until the timeout; it now
deletes the entry from the store, and `shutdown()` does the same, so a NONE session ends with its
connection however the connection ended. The vm-manager heartbeat is a self-rescheduling
`setTimeout`, so a beat that outlives its interval never overlaps the next. Files: `RpcError.ts`,
`EventBus.ts`, `index.ts`, `RpcError.test.ts`, vm-manager `index.ts`, `EndpointConnectionHandler`,
`EndpointConnectionHandlerTests` (a NONE session is gone from the store once the connection
closes). `@kinotic-ai/core` moves to 5.0.0-beta.11 for the new export.

## Phases 7 and 8 — dropped: a call does not outlive its connection

Phase 7 parked a closed connection's reply state on its node for a window, buffering replies and
keeping leases armed so the same client reconnecting resumed every call; Phase 8 handed that state
across nodes with a release published to the reply destination. Both were built (PRs #548 and #549)
and closed unmerged, for a finding about the deployment rather than the code: the STOMP port sits
behind the cluster load balancer with no session affinity, so a reconnect lands on any node and the
cross-node case is the normal one, not the rollover exception. Parking then pays off only with the
handoff on top, and the handoff still loses the buffer, and the pending records with it, when the
node holding them dies during the window, the one case a rollover produces. Each fix for that was
another piece of held state that could be on the wrong node.

What replaces them is one rule on both ends: a call is bound to its connection.

```ts
// StompConnectionManager — a drop after the initial connect
this.rxStomp.connectionState$.subscribe(state => {
    if (state === RxStompState.CLOSED && this.isActive && this.initialConnectionSuccessful) {
        this.connectionLostHandler?.()          // EventBus: resetRequestReplies('Connection lost')
    }
})
```

```java
// EndpointConnectionHandler.shutdown — Phase 4 as merged, the server side of the same rule
replySessionState.dispose();      // leases settled, reply consumers unregistered → server streams cancel on INACTIVE
serviceSessionState.dispose();    // Phase 5: the invocations this connection owed are failed to their requesters
```

Both sides let go at the moment of the close, streams included, so nothing is held that could be
on the wrong node or die with one. A blip costs the caller a retry, and a non-idempotent call
carries the ambiguity `RpcServiceUnavailableException` already documents. A caller that needs a
message to survive its connection needs a delivery guarantee, which is a queue behind the API with
acknowledgement and redelivery, a separate layer with its own contract. The Phase 7 and 8
branches remain on origin (`claude/node-failure-proxy-handling-phase7`, `-phase8`) should a
sticky load balancer ever change the calculus.

## Phase 9 — orchestrator fast path (~5 files, optional)

The orchestrator now lives in `kinotic-system-api`
(`org.kinotic.system.internal.api.services.DefaultVmNodeOrchestrationService`,
`org.kinotic.system.api.workload.VmManagerProxy`). Feed `RpcServiceUnavailableException` /
`NO_HANDLERS` on `VmManagerProxy` into VmNode health verification as an invalidation trigger, the
way `ServiceLivenessUpdater` works — the orchestrator otherwise learns of node death only from
the heartbeat reaper. And fix the guard that keeps a draining node from ever going offline:

```java
// DefaultVmNodeOrchestrationService.java:228-230
if (node.getStatus().getType() == VmNodeStatusType.ONLINE      // DRAINING never enters
        && node.getLastSeen() != null
        && node.getLastSeen().before(cutoffDate)) {
```

This matters more now: the vm-manager reports telemetry-shipping problems as DRAINING, so a node
that loses Loki/Tempo and then dies keeps its workloads RUNNING forever.

As built. `VmNodeOrchestrationService.verifyNode(nodeId)` is the invalidation trigger:
`DefaultWorkloadOrchestrationService` calls it when a `VmManagerProxy` call fails with
`RpcMissingServiceException` or `RpcServiceUnavailableException`. Verification reads the
vm-manager's registration for the node (`monitorListenerStatus` on the scoped address, first
emission); absent, the node becomes `UNREACHABLE`, a new `VmNodeStatusType`, so placement stops
at once. Its workloads are left to the heartbeat reaper: a vm-manager reconnecting after a blip
is registration-less for a few seconds, and failing workloads on that would be a false positive.
The reaper now marks any non-OFFLINE node with a stale `lastSeen` OFFLINE and fails its
workloads, which covers DRAINING and UNREACHABLE alike. The next heartbeat brings an UNREACHABLE
node back to ONLINE through the existing status comparison. `KinoticUtil.serviceIdentifierOf`
carries the proxy-to-identifier derivation `DefaultServiceRegistry` had inline, so the orchestrator
builds the scoped address the same way the proxy does.

Files: `KinoticUtil`, `DefaultServiceRegistry`, `VmNodeStatusType` (Java and TS),
`VmNodeOrchestrationService`, `DefaultVmNodeOrchestrationService`,
`DefaultWorkloadOrchestrationService`, `WorkloadOrchestrationTest` (+ `StubVmNodeService.findAll`):
an unreachable vm-manager with no registration marks its node UNREACHABLE; one still registered
leaves it ONLINE; a silent DRAINING node goes OFFLINE with its workload FAILED.

## Wrap-up (after Phase 9)

Built after the direction change, against the whole series:

- The TS client's incoming heartbeat matches the gateway's 30 s heartbeat (from 120 s). The
  client had asked the gateway for a beat every 120 s, so a gateway VM that vanished without closing
  the socket took stompjs two of those to notice, and every call on the connection hung for that
  long; the gateway itself has bounded the reverse direction at two 30 s intervals since Phase 5.
- The system console handles `UNREACHABLE`: the node list ranks nodes by fitness in the client
  rather than by the keyword order of `status.type`, which would have put `UNREACHABLE` above
  `ONLINE`; the dashboard counts it, the node pages explain it, and the attention list reports it.
- `DefaultStompServerHandler.closed()` describes what a close does with the session.

Still open, and outside this repository's harness:

- Publish `@kinotic-ai/core` 5.0.0-beta.11 and `@kinotic-ai/system-api` 5.0.0-beta.12. The console's
  catalog ranges admit both; `kinotic-cli` pins core at 5.0.0-beta.10 exactly and needs a bump to
  fail its calls on a lost connection. Until core is published, a browser or CLI on 5.0.0-beta.10
  still waits on a dropped connection.
- A TS service could not stream at all: `BasicReturnValueConverter` serialised whatever a method
  returned, an `Observable` included, and `processControlPlaneRequest` dropped every control, while
  the streaming page documented the feature. Built as the follow-up to the wrap-up: the TS supervisor
  streams an `Observable` result (one reply per value carrying the origin CRI, a bodiless completion
  control at the end, an error reply on failure), honours a cancel control, ends every stream with an
  error reply on `stop()`, and cancels them all when `IEventBus.connectionLost` fires, since the
  gateway has already failed those requesters. The gateway's `ServiceSessionState` watches the
  requester's reply destination once an invocation answers with a stream value and delivers a cancel
  control over the socket on INACTIVE, which is the Java supervisor's reply-listener cancel carried
  one hop further.
- An end-to-end run against a cluster: a Java caller and a UI caller each mid-call while the serving
  node is killed, and a UI mid-call while its gateway node is killed.

## Review fixes (after the merges)

A four-way adversarial review of everything the series landed, one reviewer per area, found the
defects below; each was traced in the code before it was fixed. All are on one branch on top of the
merged stack.

- Every TS `invokeStream` ended with a trailing `null`: stompjs yields an empty array, never an
  absent body, so a stream's bodiless completion reached `EventBus` with data present and was emitted
  as a value. Since Phase 1. An empty body is now no body where frames become events.
- A TS stream killed its own connection on its second value: the reply grant was one send per
  delivered invocation, consumed by the first reply, and the zone rules refuse `reply://`. A reply to
  a pending invocation's own destination is allowed while it is pending.
- A lease could fall out of the watcher's node index: `watch` added the id to a set fetched with
  `computeIfAbsent`, outside the lock, and a concurrent `settle` emptying that set dropped it. The add
  runs inside `compute`.
- Reply-consumer collision on reconnect: the reply discriminator was per client instance, so a
  reconnect within the heartbeat window shared its reply address with the previous socket's consumer
  and `send` round-robined replies into the dead socket. Minted per connection.
- A request during a pending `connect()` cached the reply subscription on a null or stale address,
  which the vm-manager's reconnect loop hit through its heartbeat. `requestStream` requires a
  connection and an address; `connect()` resets the cache when the address changed.
- The session touch put a session back that a logout had deleted, because the clustered store only
  checks versions on an existing entry. The touch reads first and never re-creates.
- `DefaultRpcServiceProxyHandle.settle()` released the lease before the map entry, so a reply that beat
  the ack leaked a lease; the reply handler's `containsKey` then `get` could NPE and release the proxy.
- `SingleValueSubscriber.onNext` rethrowing leaked the in-flight count, so `stop()` waited the full
  drain; an invocation queued for the worker pool was not counted until it ran.
- `beforeSave` stamped `lastSeen` on every `VmNode` save, so marking a node UNREACHABLE restarted its
  reaper clock; only a heartbeat or registration stamps it. `verifyNode` bridged a Mono through a
  context-less Promise.
- TS supervisor: a value the converter cannot serialise, an unknown control, and an error reply with
  no reply-to each became an uncaught exception or a running stream; `connectionLost` fired per failed
  reconnect attempt; `disconnect()` waited behind a `connect()` that could never settle.
- A NONE keep-alive connection deleted a login session it had not created; `disconnected()` duplicated
  `closed()`; membership snapshots could emit out of order.

A second pass took the TS connection layer and the Java supervisor on their own, with the bar of no
known defect.

- `StompConnectionManager` is reworked around a `StompActivation` per `activate()` call, which owns
  the listeners, the pending reject, a socket produced for stompjs, and the attempt stompjs is
  awaiting. `deactivate()` ends only the activation it took, waits for that attempt before the next
  activation may start, and reports an open connection's end once. A fatal raised by an ended
  activation's attempt or listener is not published; a bad CONNECTED frame rejects the connect with
  its own reason; a connect queued behind a `disconnect()` never starts; `activate()` rejects with
  `Error`s; the attempt budget is granted again for each connection's reconnects. A fake STOMP server
  behind the socket factory drives these in `ConnectionLifecycle.test.ts`.
- `ServiceInvocationSupervisor`: a stream whose caller's reply listener goes INACTIVE, whose listener
  monitor ends, or whose service stops after the drain began ends with an error reply instead of a
  silent cancel; `fail()` sends that reply once whichever path gets there first; a request reusing a
  live stream's correlation id is answered with an error instead of dropped with its span open; an
  unknown control cancels the source it answers with an error; a single-value reply that cannot be
  sent is answered the same way whether it came from `onNext` or an empty completion.
- `ServiceInvocationSupervisor` no longer dispatches through `vertx.executeBlocking`: an invocation
  runs on its delivery context, so the synchronous slice of one service's invocations is no longer
  serialized on one ordered worker and no worker hop or shared pool sits in the request path. A slow
  handler hands off inside itself, the rule the platform docs now state. The stream registry uses
  `putIfAbsent` and removes inline, and the in-flight count is a pair of plain methods. The argument
  tokenizer is a synchronous call with no `block()` on the event loop.

The second review's remaining findings (`Node failure review round two.md`) are fixed on the same
branch: a cancel is published to every instance of the service, so the one producing the stream gets
it whichever instance round-robin gave the request; a reply the gateway connection does not owe is
dropped instead of ending the connection, and a TS service drops results for invocations of a lost
connection; `VertxFutureRpcReturnValueHandler` completes idempotently; lease keys are qualified per
connection; a CONNECTION keep-alive touches the session at connect and every quarter of the timeout;
a control event earns no reply grant; a reused subscription id ends its predecessor; a requester whose
listener is gone is answered as well as cancelled; VmNode status is a partial update and the allocation an atomic scripted
reservation and release, so only a heartbeat writes `lastSeen` and two concurrent deploys can never both
take a node's last room; the reaper takes STOPPING workloads with the rest; the TS
supervisor stops a synchronous stream on its first unsendable value, answers with the
`{exceptionName, exceptionClass, errorMessage}` shape every runtime reads, and the vm-manager bounds
its graceful disconnect on shutdown.

## Numbering

| Earlier chat numbering | This document |
|---|---|
| 1 wire contract | 1 |
| 2 watcher (address-level) | 2 ack names the node, 3 watcher (node-level) |
| 3 gateway leases + heartbeats | 4 caller side + session touch, 5 callee side + heartbeats |
| 4 TS edges | 6 |
| 5 mailbox | 7, dropped |
| 6 handoff | 8, dropped |
| 7 orchestrator | 9 |

Dependency spine: 1 → 2 → 3 → 4, with 5 after 4, 6 after 1, 9 after 3.

## Scaling

Watches are per address, not per request; the registration-event stream they ride on is
delivered to every node already (`__vertx.subs` is REPLICATED, FULL_SYNC); no new replicated
state is added. The genuinely new per-request cost is the gateway record (Phases 4 and 5): tens of
bytes, on the connection's own event loop, behind the per-send `pause()` in
`DefaultStompServerHandler` that bounds in-flight requests per connection today. The one place
to be deliberate is the cluster manager's shared delivery context under a death burst: `onLost`
dispatches, never runs continuations inline.
