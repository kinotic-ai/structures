import { describe, expect, it } from 'vitest'
import { BehaviorSubject, type Observable, of } from 'rxjs'
import { Event, EventConstants, RpcError, type ConnectOptions, type IWebSocket } from '../src'
import { EventBus } from '../src/api/event/EventBus'
import { ServiceIdentifier } from '../src/api/ServiceIdentifier'
import { ServiceInvocationSupervisor } from '../src/internal/api/ServiceInvocationSupervisor'
import { FakeStompServer, type FakeFrame, type FakeSocket } from './FakeStompServer'

const SERVICE = new ServiceIdentifier('com.example', 'SupervisedService')
const ADDRESS = SERVICE.cri().baseResource()

const sleep = (ms: number) => new Promise(resolve => setTimeout(resolve, ms))

// The service under supervision. BigInt values are what the converter cannot serialise: JSON.stringify
// throws on them.
class SupervisedService {
    public readonly current = new BehaviorSubject<unknown>(1n)
    public release: (value: string) => void = () => {}

    unserialisable(): Observable<unknown> {
        return of(1n, 2n, 3n)
    }

    unserialisableOpen(): Observable<unknown> {
        return this.current
    }

    fail(): string {
        throw new RangeError('out of range')
    }

    /** Resolves when the test calls release. */
    slow(): Promise<string> {
        return new Promise(resolve => this.release = resolve)
    }

    greet(): string {
        return 'hi'
    }
}

function options(server: FakeStompServer): ConnectOptions {
    return { server: { host: 'fake', useSSL: false }, webSocketFactory: server.factory as () => IWebSocket }
}

async function startSupervised(server: FakeStompServer): Promise<{ bus: EventBus, service: SupervisedService, supervisor: ServiceInvocationSupervisor }> {
    const bus = new EventBus()
    const service = new SupervisedService()
    const supervisor = new ServiceInvocationSupervisor(SERVICE, service, bus, () => null)
    supervisor.start()
    await bus.connect(options(server))
    return { bus, service, supervisor }
}

// Delivers an invocation of the method on the service's subscription, addressed the way the gateway
// addresses one: the method is the path of the destination
function invoke(socket: FakeSocket, method: string, correlationId: string): void {
    socket.message(ADDRESS, {
        destination: `${ADDRESS}/${method}`,
        [EventConstants.REPLY_TO_HEADER]: `reply://${socket.replyToId}:caller@kinotic.js.EventBus/replyHandler`,
        [EventConstants.CORRELATION_ID_HEADER]: correlationId,
        [EventConstants.CONTENT_TYPE_HEADER]: EventConstants.CONTENT_JSON
    })
}

function replies(socket: FakeSocket): FakeFrame[] {
    return socket.received.filter(f => f.command === 'SEND')
}

// The reply as the caller's EventBus would see it
function asEvent(frame: FakeFrame): Event {
    const event = new Event(frame.headers['destination']!, new Map(Object.entries(frame.headers)))
    event.setDataString(frame.body)
    return event
}

describe('service invocation supervisor', () => {

    it('answers a synchronous stream whose values cannot be serialised with one error reply and no completion', async () => {
        const server = new FakeStompServer()
        const { bus, supervisor } = await startSupervised(server)

        invoke(server.current, 'unserialisable', 'c1')
        await sleep(50)

        const sent = replies(server.current)
        expect(sent.length).toBe(1)
        expect(sent[0]!.headers[EventConstants.ERROR_HEADER]).toBeDefined()
        expect(sent[0]!.headers[EventConstants.CORRELATION_ID_HEADER]).toBe('c1')
        expect(sent[0]!.headers[EventConstants.CONTROL_HEADER]).toBeUndefined()
        expect(RpcError.fromEvent(asEvent(sent[0]!)).exceptionName).toBe('TypeError')
        supervisor.stop()
        await bus.disconnect()
    })

    it('ends a stream that fails on a value while its source is still inside subscribe', async () => {
        const server = new FakeStompServer()
        const { bus, service, supervisor } = await startSupervised(server)

        invoke(server.current, 'unserialisableOpen', 'c1')
        await sleep(50)

        expect(replies(server.current).length).toBe(1)
        expect(service.current.observed).toBe(false)
        supervisor.stop()
        await bus.disconnect()
    })

    it('describes the exception a method threw so a caller reads it as it reads a Java one', async () => {
        const server = new FakeStompServer()
        const { bus, supervisor } = await startSupervised(server)

        invoke(server.current, 'fail', 'c1')
        await sleep(50)

        const sent = replies(server.current)
        expect(sent.length).toBe(1)
        expect(sent[0]!.headers[EventConstants.ERROR_HEADER]).toBe('out of range')
        expect(JSON.parse(sent[0]!.body)).toEqual({ exceptionName: 'RangeError', exceptionClass: 'RangeError', errorMessage: 'out of range' })
        const error = RpcError.fromEvent(asEvent(sent[0]!))
        expect(error.exceptionName).toBe('RangeError')
        expect(error.exceptionClass).toBe('RangeError')
        expect(error.message).toBe('out of range')
        supervisor.stop()
        await bus.disconnect()
    })

    it('drops the result of an invocation received on a connection that was lost and answers one received on the next', async () => {
        const server = new FakeStompServer()
        const { bus, service, supervisor } = await startSupervised(server)
        const firstSocket = server.current

        invoke(firstSocket, 'slow', 'c1')
        await sleep(50)
        firstSocket.drop()
        await sleep(10)

        // rx-stomp reconnects after its delay plus the client's jitter
        const deadline = Date.now() + 15_000
        while (!bus.isConnected() && Date.now() < deadline) {
            await sleep(50)
        }
        expect(bus.isConnected()).toBe(true)
        expect(server.current).not.toBe(firstSocket)
        await sleep(50)

        service.release('late')
        await sleep(50)
        expect(replies(firstSocket)).toEqual([])
        expect(replies(server.current)).toEqual([])

        invoke(server.current, 'greet', 'c2')
        await sleep(50)
        const sent = replies(server.current)
        expect(sent.length).toBe(1)
        expect(sent[0]!.headers[EventConstants.CORRELATION_ID_HEADER]).toBe('c2')
        expect(sent[0]!.headers[EventConstants.CONTROL_HEADER]).toBe(EventConstants.CONTROL_VALUE_COMPLETE)
        expect(JSON.parse(sent[0]!.body)).toBe('hi')
        supervisor.stop()
        await bus.disconnect()
    }, 30_000)
})
