Second adversarial review of the node-failure series, run after the first round's fixes landed on
PR #558. Every item below was traced in the code before it was written down, and every one is now
resolved on that same PR: 1 through 9 as written (4 fell out of the supervisor pass), 10 by
publishing cancels instead of routing them, 11 by dispatching invocations on the delivery context.
The TS connection manager and event bus were taken out of this list and reworked directly, so
nothing here concerns `StompConnectionManager.ts` or `EventBus.ts`. The items stay as the record of
what was found and why each fix has the shape it has.

## 1. A refused reply kills the client's connection

`EndpointConnectionHandler.send()` allows a `reply://` send only when `outgoingInvocations.owesReply`
is true or the authorizer has a grant; `ZoneRules.sendAllowed` never allows the scheme; and
`DefaultStompServerHandler` answers a failed send with `sendErrorAndDisconnect`. Two ordinary
sequences reach that path:

- A cancel crossing values in flight. `OutgoingInvocations.cancel()` forgets the invocation before the
  cancel frame reaches the client, and the values the client already sent sit queued behind the
  socket's per-frame `pause()`, so the next value is refused. A UI navigating away from a stream does
  this through `deliver()`'s cancel branch as well.
- A late reply after a reconnect. A vm-manager finishes an invocation it received on the old socket
  and sends the reply on the new one, whose `OutgoingInvocations` does not hold it.

Either way the connection dies, `dispose()` fails every invocation it owed, the `srv://`
registration drops, and Phase 9 marks the node UNREACHABLE.

```java
// EndpointConnectionHandler.send — a reply this connection does not owe is dropped, not fatal
if (incomingEvent.cri().scheme().equals(EventConstants.REPLY_DESTINATION_SCHEME)) {
    if (outgoingInvocations.owesReply(incomingEvent)) {
        outgoingInvocations.observeReply(incomingEvent);
        services.eventBusService.send(incomingEvent);
    } else if (stompAuthorizer.sendAllowed(incomingEvent.cri())) {
        services.eventBusService.send(incomingEvent);
    } else {
        log.debug("Dropping reply to {} for an invocation this connection does not owe", incomingEvent.cri());
    }
    return Future.succeededFuture();
}
```

```ts
// ServiceInvocationSupervisor.ts — results of an invocation from a previous connection are not sent
private connectionGeneration = 0                       // ++ in the connectionLost subscription
const generation = this.connectionGeneration           // captured in processInvocationRequest
if (generation !== this.connectionGeneration) { span.end(); return }   // in the result and error callbacks
```

`EndpointConnectionHandlerTests.testEveryReplyOfAPendingInvocationIsAllowed` asserts the refusal
today; change it to assert the late reply is not forwarded and the send succeeds.

## 2. A lost-node failure racing a reply releases the whole proxy

```java
// VertxFutureRpcReturnValueHandler — Vert.x 5 Promise.complete/fail throw when already completed
promise.complete(rpcResponseConverter.convert(incomingEvent, methodParameter));
promise.fail(e);
```

`DefaultRpcServiceProxyHandle.failLost` runs on the request's context and the reply handler on the
consumer's, so both can reach the same promise. The second throws; the catch calls `promise.fail`
and throws again; the reply handler's catch calls `release()`. Every other call on the proxy fails
and every later one refuses, and `@Proxy` beans are singletons. Fix: `tryComplete`/`tryFail` in all
five places, after which the `catch (IllegalStateException)` in `failLost` is dead. Test: a
`Future`-returning proxy whose node leaves as the reply arrives keeps serving afterwards.

## 3. Lease keys are the client's own correlation ids

`IncomingInvocations.pin` passes the STOMP client's `correlation-id` header straight into the
node-wide `RequestLivenessWatcher`. Two connections on one gateway using the same id overwrite each
other's leases and settle each other's: one hangs when its node dies, the other is failed for a
node it never used. Prefix the key per connection:

```java
private final String leasePrefix = UUID.randomUUID() + ":";
services.requestLivenessWatcher.watch(leasePrefix + correlationId, nodeId, () -> fail(correlationId, destination, nodeId));
services.requestLivenessWatcher.settle(leasePrefix + correlationId);     // settle() and dispose()
```

## 4. `SingleValueSubscriber.onComplete` is the unguarded twin of the `onNext` fix (fixed by the supervisor pass)

```java
if(!valueReceived){
    convertAndSend(incomingMetadata, handlerMethod, null);   // rethrows; no error reply, count never left
}
```

Every `Mono<Void>` method takes this path. Mirror `onNext`: catch, `handleException`, `failSpan`,
`leaveInFlight.run()`, return.

## 5. Full-document `VmNode` writes move `lastSeen` backwards

With the stamp moved into `heartbeat()`, `deployWorkload`, `destroyWorkload`, `markUnreachable` and
the reaper write back the `lastSeen` they read. A heartbeat inside their read-to-write window is
overwritten with the older value. It can reap a live node only when two heartbeat intervals exceed
the timeout (defaults 30 s against 90 s cannot), but the two knobs are set independently on node and
server. Fix: partial updates for status and allocation through `CrudServiceTemplate.partialUpdateSync`,
exposed as `VmNodeService.updateStatusSync` and `updateAllocationSync`, so only a heartbeat or a
registration ever writes `lastSeen`. Also: the reaper skips `STOPPING`, so a workload whose stop
failed on a dead node stays STOPPING forever; include it.

`WorkloadOrchestrationTest`: the new tests assert on state written by `verifyNode`'s continuation,
which runs on the `Vertx` context the fix added, while the test asserts on its own thread. Poll for
the status, and close the `Vertx` instance in `@AfterEach`. Add: marking UNREACHABLE leaves
`lastSeen` alone; both `registerNode` branches stamp; the reaper takes an UNREACHABLE node OFFLINE.

## 6. Gateway session touch, three small ones

- vertx-core 5.1.6 never flushes the session on a WebSocket upgrade (`completeHandshake` skips the
  headers-end handlers), so a CONNECTION keep-alive connection's first store write is `T/2` after
  connect. Call `touchSession()` in `connect()` for CONNECTION mode. The `replyToId` comment saying
  it is reused across reconnects is false for the same reason; every connect mints one.
- `lastSessionFlush` advances before the store round trip completes, so in CONNECTION mode one
  failed flush lands the next on the TTL boundary. Run the timer at `T/4`, or advance on success.
- A requester's cancel carries a reply-to and is not recorded, so `subscribe()`'s handler adds a
  one-shot grant nothing consumes. Grant only when the event has no control header.

## 7. Gateway, watch and subscriptions

- A spurious INACTIVE on the first emission of `monitorListenerStatus` (the requester's registration
  not yet replicated to this node) cancels a healthy stream silently. `OutgoingInvocations.cancel()`
  should also answer the requester with `RpcServiceUnavailableException`; a requester that is truly
  gone makes that a silent NO_HANDLERS.
- `IncomingInvocations.subscribe` overwrites a duplicate subscription id without unregistering the
  previous consumer, which keeps the reply address ACTIVE after the connection closes. Unregister the
  one replaced. Same pattern in `EndpointConnectionHandler.subscribe` for the other schemes.

## 8. Core, two small ones

- `McpToolInvoker`'s ack-failure path calls `ret.complete` after `pendingCalls.remove` without
  checking the removal, so an ack timeout after the reply throws. Complete only when this path
  removed the entry.
- `DefaultRpcServiceProxyHandle.invoke()` can put into `responseMap` after `release()` scanned it,
  leaving a request whose reply consumer is gone. Re-check `released` after the put.

## 9. TS supervisor, three small ones

- A synchronous source whose value the converter cannot serialise is not stopped: `subscription` is
  still null inside `subscribe()`, so one error reply per value goes out plus a completion. Gate on a
  `failed` flag in `next` and `complete`.
- `handleException` sends `{message}`; `RpcError.fromEvent` reads the Java `ServiceExceptionWrapper`
  shape, so a TS-hosted service's errors carry no `exceptionName`. Emit
  `{exceptionName, exceptionClass, errorMessage}`.
- vm-manager `shutdown()` awaits a graceful `disconnect()` that on a half-open socket waits for the
  heartbeat timeout plus the WebSocket close timeout. Bound it and exit.

## 10. Decided: a cancel is published to every instance

The per-instance control address below was the first design; the ack already names the node, but
Vert.x has no way to send to one node on a shared address and its node selector sees only the
address, so a node-scoped cancel needs a new address per service per node. A cancel names a UUID
correlation id, and an instance that does not hold it already ignores it, so the cancel is
published instead: every instance receives it, the producer acts, nothing else changes on the
wire. `RpcLivenessTests.testCancelReachesTheInstanceProducingTheStream` pins it.

The original finding, kept for the record:

`DefaultRpcServiceProxyHandle.cancelRequest` sends the cancel to the request address; with two remote
instances of an unscoped service and no interleaved traffic, round-robin lands it on the wrong one
every time. `__origin-cri` is the same shared address, so routing through it changes nothing. The
proxy's reply address is never INACTIVE while the proxy lives, so a quiet stream leaks its
`StreamSubscriber`, upstream subscription, address monitor and map entry until the JVM leaves. The
fix is a per-instance control address: the supervisor listens on
`srv://<nodeId>@<qualifiedName>/__control`, puts it in `__origin-cri`, and the proxy cancels to the
origin it saw on the first value. A wire change, to be decided separately.

## 11. Decided: invocations run on the delivery context, and a slow handler hands off itself

`ServiceInvocationSupervisor.listenAt` dispatched with `vertx.executeBlocking(callable)`, whose
one-argument overload is ordered. Each delivery has its own duplicated context, but a duplicate's
ordered tasks share the consumer context's queue, so the synchronous slice of every invocation of
one service (argument parsing, the call, up to the handler returning its `Future`) ran one at a time
on one worker. Measured with a handler that sleeps 500 ms: four concurrent calls took 2177 ms on
`vert.x-worker-thread-7`. For a handler that returns a `Future` at once the slice is microseconds,
which is why a 1k-concurrent load test of `JsonEntitiesService` never showed it; a burst of large
bodies to one service would have, since the parsing is inside the slice.

Every `@Publish`ed interface in the tree returns `Future`, `CompletableFuture`, `Mono` or `Flux`, or
a trivial in-memory value, and every Elasticsearch use is the async client, so the worker hop bought
nothing. `processEvent` now runs on the delivery context, the same way the proxy has always handled
replies, and the rule is stated in the platform docs the way Vert.x states it: never block the event
loop; a slow handler calls `vertx.executeBlocking` inside itself and returns the `Future`.
