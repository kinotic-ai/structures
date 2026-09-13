import {type ConnectOptions, ServerInfo} from '@/api/ConnectOptions'
import {ConnectedInfo} from '@/api/security/ConnectedInfo'
import {createDebugLogger, type Logger} from '@/internal/api/Logger'
import {StompConnectionManager} from '@/internal/api/StompConnectionManager'
import {context, propagation} from '@opentelemetry/api';
import type {IMessage} from '@stomp/rx-stomp';
import {ConnectableObservable, firstValueFrom, Observable, Subject, Subscription, type Unsubscribable} from 'rxjs'
import {filter, map, multicast, tap} from 'rxjs/operators'
import {Optional} from 'typescript-optional'
import {v4 as uuidv4} from 'uuid'
import {EventConstants, type IEvent, type IEventBus} from './IEventBus'
import {RpcError} from './RpcError'

/**
 * Default IEvent implementation
 */
export class Event implements IEvent {

    public cri: string
    public headers: Map<string, string>
    public data: Optional<Uint8Array>

    constructor(cri: string,
                headers?: Map<string, string>,
                data?: Uint8Array) {

        this.cri = cri

        if (headers !== undefined) {
            this.headers = headers
        } else {
            this.headers = new Map<string, string>()
        }

        this.data = Optional.ofNullable(data)
    }

    public getHeader(key: string): string | undefined {
        return this.headers.get(key)
    }

    public hasHeader(key: string): boolean {
        return this.headers.has(key)
    }

    public setHeader(key: string, value: string): void {
        this.headers.set(key, value)
    }

    public removeHeader(key: string): boolean {
        return this.headers.delete(key)
    }

    public setDataString(data: string): void {
        const uint8Array = new TextEncoder().encode(data)
        this.data = Optional.ofNonNull(uint8Array)
    }

    public getDataString(): string {
        let ret = ''
        this.data.ifPresent(( value: any ) => ret = new TextDecoder().decode(value))
        return ret
    }
}

interface Carrier {
    traceparent?: string;
    tracestate?: string;
}

/**
 * Default implementation of {@link IEventBus}
 */
export class EventBus implements IEventBus {

    public serverInfo: ServerInfo | null = null
    private readonly log: Logger = createDebugLogger('kinotic:EventBus')
    private stompConnectionManager: StompConnectionManager = new StompConnectionManager()
    private connectionLifecycle: Promise<unknown> = Promise.resolve()
    // counts disconnect() calls, so a connect() queued before one of them never starts
    private disconnectRequests: number = 0
    private requestRepliesObservable: ConnectableObservable<IEvent> | null = null
    private requestRepliesSubject: Subject<IEvent> | null = null
    private requestRepliesSubscription: Subscription | null = null
    private readonly activeCorrelationIds: Set<string> = new Set<string>()
    private readonly recentlyReaped: Set<string> = new Set<string>()
    private readonly connectionLostSubject: Subject<void> = new Subject<void>()
    // How long a sent cancel suppresses repeat cancels for the same stream before retrying.
    private static readonly REAP_DEBOUNCE_MS = 5000

    constructor() {
        // the manager has already deactivated when it reports a fatal error; in-flight requests fail here
        this.stompConnectionManager.fatalErrors.subscribe(() => this.serverInfo = null)
        // The server drops every reply consumer and lease of a closed connection, so a call in flight
        // across a drop is failed here rather than waited on. The manager reports each open connection's
        // end once, however it ends, so this is the one place connectionLost is emitted.
        this.stompConnectionManager.connectionLostHandler = () => {
            this.resetRequestReplies('Connection lost')
            this.connectionLostSubject.next()
        }
    }

    public get fatalErrors(): Observable<Error> {
        return this.stompConnectionManager.fatalErrors
    }

    public get connectionLost(): Observable<void> {
        return this.connectionLostSubject.asObservable()
    }

    public isConnectionActive(): boolean{
        return this.stompConnectionManager.active
    }

    public isConnected(): boolean {
        return this.stompConnectionManager.connected
    }

    public connect(options: ConnectOptions): Promise<ConnectedInfo> {
        const disconnectsWhenQueued = this.disconnectRequests
        return this.serializeLifecycle(async () => {
            // a disconnect() requested while this connect was queued cancels it
            if (this.disconnectRequests !== disconnectsWhenQueued) {
                throw new Error('Disconnected before the connection was started')
            }
            if(!this.stompConnectionManager.active){
                const connectedInfo = await this.stompConnectionManager.activate(options)
                // copy so the reported server never aliases the caller's options object
                this.serverInfo = {...options.server} as ServerInfo

                return connectedInfo
            }else{
                throw new Error('Event Bus connection already active')
            }
        })
    }

    public disconnect(force?: boolean): Promise<void> {
        this.disconnectRequests++
        // A connect() waiting for its socket holds the lifecycle queue. Deactivating now rejects it, so the
        // teardown queued below runs at once instead of waiting on a server that may never answer.
        const deactivated = this.stompConnectionManager.deactivate(force)
        return this.serializeLifecycle(async () => {
            await deactivated
            await this.stompConnectionManager.deactivate(force)
            this.serverInfo = null
        })
    }

    /** Runs connect and disconnect one at a time so they never overlap on the shared socket. */
    private serializeLifecycle<T>(operation: () => Promise<T>): Promise<T> {
        // A one-at-a-time promise queue. `connectionLifecycle` always holds a promise that resolves
        // when the previously queued operation finished, so running `operation` off it defers
        // `operation` until that one is done. We then store this operation's own completion back
        // into `connectionLifecycle`, so the next call defers behind this one — and so on.
        const result = this.connectionLifecycle.then(operation)
        // `result` is the operation's real outcome and goes to the caller. The copy we keep as the
        // next "previous" swallows rejections, so one failed operation can't reject the chain and
        // stall everything queued behind it.
        this.connectionLifecycle = result.catch(() => {})
        return result
    }

    public send(event: IEvent): void {
        if(this.stompConnectionManager.connected){
            const headers: any = {}

            for (const [key, value] of event.headers.entries()) {
                headers[key] = value
            }

            const carrier: Carrier = {}
            propagation.inject(context.active(), carrier)
            if(carrier.traceparent){
                headers[EventConstants.TRACEPARENT_HEADER] = carrier.traceparent
            }
            if(carrier.tracestate){
                headers[EventConstants.TRACESTATE_HEADER] = carrier.tracestate
            }

            // send data over stomp. retryIfDisconnected keeps RxStomp from holding the frame and
            // flushing it onto the next connection: a request in flight across a drop is failed at the
            // drop, so nobody would receive its reply, and under a NONE session its reply-to names a
            // replyToId the server no longer issues, which it terminates the connection over.
            this.stompConnectionManager.rxStomp.publish({
                                                            destination: event.cri,
                                                            headers,
                                                            binaryBody: event.data.orUndefined(),
                                                            retryIfDisconnected: false
                                                        })
        }else{
            throw this.createSendUnavailableError()
        }
    }

    public request(event: IEvent): Promise<IEvent> {
        return firstValueFrom(this.requestStream(event, false))
    }

    public requestStream(event: IEvent, sendControlEvents: boolean = true): Observable<IEvent> {
        return new Observable<IEvent>((subscriber) => {
                // Read at subscribe time, not when the Observable was created: a reconnect in between
                // mints a new destination. The manager mints it on every CONNECTED frame and clears it on
                // deactivate, so a connected manager always has one. The shared reply subscription was torn
                // down at the last drop, so the first request on a connection builds it on that
                // connection's destination.
                const replyToCri = this.stompConnectionManager.replyToCri
                if (!this.stompConnectionManager.connected || replyToCri == null) {
                    subscriber.error(this.createSendUnavailableError())
                    return
                }

                if (this.requestRepliesObservable == null) {
                    this.requestRepliesSubject = new Subject<IEvent>()
                    // Reaper: before multicast, so it runs once per reply to cancel streams we no longer track.
                    this.requestRepliesObservable = this._observe(replyToCri)
                                                        .pipe(tap((value: IEvent) => this.cancelIfUnexpected(value)),
                                                              multicast(this.requestRepliesSubject)) as ConnectableObservable<IEvent>
                    this.requestRepliesSubscription = this.requestRepliesObservable.connect()
                }

                let serverSignaledCompletion = false
                let replyDestinationReset = false
                const correlationId = uuidv4()
                this.activeCorrelationIds.add(correlationId)
                // registered before the request is sent so a send that throws still untracks it
                subscriber.add(() => this.activeCorrelationIds.delete(correlationId))
                const defaultMessagesSubscription: Unsubscribable
                          = this.requestRepliesObservable
                                .pipe(filter((value: IEvent): boolean => {
                                    return value.headers.get(EventConstants.CORRELATION_ID_HEADER) === correlationId
                                })).subscribe({
                                                  next(value: IEvent): void {

                                                      if (value.hasHeader(EventConstants.CONTROL_HEADER)) {

                                                          if (value.headers.get(EventConstants.CONTROL_HEADER) === EventConstants.CONTROL_VALUE_COMPLETE) {
                                                              serverSignaledCompletion = true
                                                              // control: complete marks the last event of a request, and two kinds arrive here:
                                                              //  - a single-value reply (invoke): the result rides on this same event, so it is
                                                              //    emitted before completing. A void result has no body but is emitted anyway,
                                                              //    so the caller's promise resolves with null instead of failing on an empty stream.
                                                              //  - a stream's completion event (invokeStream): it carries nothing and only
                                                              //    ends the stream, so nothing is emitted.
                                                              // sendControlEvents is true only for streams, which is what tells the two apart.
                                                              if (value.data.isPresent() || !sendControlEvents) {
                                                                  subscriber.next(value)
                                                              }
                                                              subscriber.complete()
                                                          } else {
                                                              subscriber.error(new Error('Control Header ' + value.headers.get(EventConstants.CONTROL_HEADER) + ' is not supported'))
                                                          }

                                                      } else if (value.hasHeader(EventConstants.ERROR_HEADER)) {

                                                          serverSignaledCompletion = true
                                                          subscriber.error(RpcError.fromEvent(value))

                                                      } else {

                                                          subscriber.next(value)

                                                      }
                                                  },
                                                  error(err: any): void {
                                                      // a reset: the connection the stream ran on is gone, and
                                                      // the gateway released the stream with it
                                                      replyDestinationReset = true
                                                      subscriber.error(err)
                                                  },
                                                  complete(): void {
                                                      subscriber.complete()
                                                  }
                                              })

                subscriber.add(defaultMessagesSubscription)

                event.setHeader(EventConstants.REPLY_TO_HEADER, replyToCri)
                event.setHeader(EventConstants.CORRELATION_ID_HEADER, correlationId)

                this.send(event)

                // registered after the send so a request that never went out is never cancelled
                subscriber.add(() => {
                    if (sendControlEvents && !serverSignaledCompletion && !replyDestinationReset) {
                        try {
                            this.sendCancel(event.cri, correlationId)
                        } catch (e) {
                            // The subscription is already torn down, so there is no subscriber left
                            // to fail. A stream left running on the server is worth knowing about.
                            this.log.warn(`Could not cancel stream ${correlationId} on ${event.cri}`, e)
                        }
                    }
                })
        })
    }

    public listen(_serverInfo: ServerInfo): Promise<void> {
        return Promise.reject('Not implemented')
    }

    public observe(cri: string): Observable<IEvent> {
        return this._observe(cri)
    }

    /**
     * Cancels a server stream whose reply arrived for a correlationId we no longer track. The server can't
     * detect this itself since all requests share one reply destination, so the cancel is sent to the
     * originating service, whose CRI the server includes on each reply.
     */
    private cancelIfUnexpected(value: IEvent): void {
        const correlationId = value.getHeader(EventConstants.CORRELATION_ID_HEADER)
        if (correlationId === undefined || this.activeCorrelationIds.has(correlationId)) {
            return
        }
        const originCri = value.getHeader(EventConstants.ORIGIN_CRI_HEADER)
        // The server may have many values already in flight when we cancel, and each one re-enters here.
        // recentlyReaped suppresses those duplicates so we send one cancel per stale stream, not one per
        // stray value.
        if (originCri === undefined || this.recentlyReaped.has(correlationId)) {
            return
        }
        try {
            this.sendCancel(originCri, correlationId)

            // Expire the entry after the debounce window so a lost cancel self-heals: if the stream is
            // still sending by then, the next stray value falls through the guard above and we cancel again.
            this.recentlyReaped.add(correlationId)
            setTimeout(() => this.recentlyReaped.delete(correlationId), EventBus.REAP_DEBOUNCE_MS)
        } catch (e) {
            // recentlyReaped is only marked after a successful send, so the next stray value retries
            this.log.warn(`Could not cancel orphaned stream ${correlationId} on ${originCri}`, e)
        }
    }

    /**
     * Sends the control event that stops the server stream identified by the correlationId.
     * @param cri the service the stream originates from
     * @param correlationId the correlationId of the stream to cancel
     */
    private sendCancel(cri: string, correlationId: string): void {
        const cancelEvent: Event = new Event(cri)
        cancelEvent.setHeader(EventConstants.CONTROL_HEADER, EventConstants.CONTROL_VALUE_CANCEL)
        cancelEvent.setHeader(EventConstants.CORRELATION_ID_HEADER, correlationId)
        // The service ignores it, but the gateway rejects any service send without a reply-to
        // and terminates the connection over the rejection.
        cancelEvent.setHeader(EventConstants.REPLY_TO_HEADER, this.stompConnectionManager.replyToCri as string)
        this.send(cancelEvent)
    }

    /**
     * Tears down the shared request-replies stream so the next request rebuilds it against the
     * connection's reply destination. Any in-flight requests are failed with the given reason since
     * their replies can no longer be delivered.
     */
    private resetRequestReplies(reason: string): void {
        if (this.requestRepliesSubject != null) {

            this.requestRepliesSubject.error(new Error(reason))

            if (this.requestRepliesSubscription != null) {
                this.requestRepliesSubscription.unsubscribe()
                this.requestRepliesSubscription = null
            }

            this.requestRepliesSubject = null
            this.requestRepliesObservable = null
            this.recentlyReaped.clear()
        }
    }

    /**
     * Creates the proper error to return if this.stompConnectionManager?.rxStomp is not available on a send request
     */
    private createSendUnavailableError(): Error {
        let ret: string
        if(this.stompConnectionManager.maxConnectionAttemptsReached){
            ret = 'Max connection attempts reached event bus is not available'
        }else if(this.stompConnectionManager.active){
            ret = 'The event bus is not connected to the server'
        }else{
            ret = 'You must call connect on the event bus before sending any request'
        }
        return new Error(ret)
    }

    /**
     * This is an internal impl of observe that creates a cold observable.
     * The public variants transform this to some type of hot observable depending on the need
     * @param cri to observe
     * @return the cold {@link Observable<IEvent>} for the given destination
     */
    private _observe(cri: string): Observable<IEvent> {
        // watch() is durable: a subscription made before the first connect queues until the
        // broker connects, and re-subscribes on every reconnect — so service registrations
        // and observed CRIs stay live across disconnect/connect cycles.
        return this.stompConnectionManager
                   .rxStomp
                   .watch(cri)
                   .pipe(map<IMessage, IEvent>((message: IMessage): IEvent => {

                           // We translate all IMessage objects to IEvent objects
                           const headers: Map<string, string> = new Map<string, string>()
                           let destination: string = ''
                           for (const prop of Object.keys(message.headers)) {
                               if (prop === 'destination') {
                                   destination = message.headers[prop] as string
                               }else{
                                   headers.set(prop, message.headers[prop] as string)
                               }
                           }

                           // stompjs hands over an empty array for a frame with no body; a completion control
                           // must not look like a value
                           return new Event(destination, headers, message.binaryBody.length > 0 ? message.binaryBody : undefined)
                       }))
    }

}

