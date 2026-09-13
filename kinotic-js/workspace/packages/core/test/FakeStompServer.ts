import type { IWebSocket } from '../src'
import { EventConstants } from '../src'

/**
 * A STOMP frame as the fake server parsed or built it.
 */
export interface FakeFrame {
    command: string
    headers: Record<string, string>
    body: string
}

/**
 * An in-process STOMP server behind a WebSocket factory, so the client's connection lifecycle can be
 * driven without a gateway: it answers CONNECT with a connected-info header, records subscriptions and
 * sends, delivers MESSAGE frames back on a subscription, and can drop the socket, refuse with ERROR, or
 * stay silent. One FakeSocket per connection attempt; the latest is the live one.
 */
export class FakeStompServer {

    public readonly sockets: FakeSocket[] = []
    /** Answers a SEND with frames to deliver to the client; null delivers nothing. */
    public onSend: ((frame: FakeFrame, socket: FakeSocket) => void) | null = null
    /** When true, CONNECT is never answered. */
    public silent: boolean = false
    /** When false, CONNECTED carries no connected-info header. */
    public connectedInfo: boolean = true
    /** Milliseconds a new socket stays CONNECTING before it opens. */
    public openDelayMs: number = 0
    /** Milliseconds between the client's close() and the socket reporting closed. */
    public closeDelayMs: number = 0
    private replyToCounter: number = 0

    public readonly factory = (): IWebSocket => {
        const socket = new FakeSocket(this, 'reply-' + (++this.replyToCounter))
        this.sockets.push(socket)
        return socket
    }

    public get current(): FakeSocket {
        return this.sockets[this.sockets.length - 1]!
    }
}

export class FakeSocket implements IWebSocket {

    public url: string = 'ws://fake/v1'
    public binaryType?: string
    public readyState: number = 0
    public onopen: ((ev?: any) => any) | undefined | null = null
    public onclose: ((ev?: any) => any) | undefined | null = null
    public onerror: ((ev: any) => any) | undefined | null = null
    public onmessage: ((ev: any) => any) | undefined | null = null
    public readonly received: FakeFrame[] = []
    /** subscription id by destination, as the client subscribed */
    public readonly subscriptions: Map<string, string> = new Map()
    public closedByClient: boolean = false
    private messageId: number = 0

    constructor(private readonly server: FakeStompServer, public readonly replyToId: string) {
        setTimeout(() => {
            if (this.readyState === 0) {
                this.readyState = 1
                this.onopen?.({})
            }
        }, server.openDelayMs)
    }

    public send(data: string | ArrayBuffer): void {
        const text = typeof data === 'string' ? data : new TextDecoder().decode(data)
        for (const raw of text.split('\0')) {
            if (raw.trim().length === 0) {
                continue
            }
            const frame = parseFrame(raw)
            this.received.push(frame)
            this.handle(frame)
        }
    }

    public close(): void {
        this.closedByClient = true
        // closing a socket that never opened reports an error first, as a WebSocket does
        if (this.readyState === 0) {
            this.onerror?.({})
        }
        this.finish(1000, 'closed by client', this.server.closeDelayMs)
    }

    /** The network drops: the socket reports an unclean close without the client asking. */
    public drop(): void {
        this.finish(1006, 'dropped', 0)
    }

    /** The server sends an ERROR frame and keeps the socket open. */
    public error(message: string): void {
        this.deliver({ command: 'ERROR', headers: { message }, body: '' })
    }

    /** The server refuses the connection with an ERROR frame and closes, as the gateway does. */
    public refuse(message: string): void {
        this.error(message)
        this.finish(1002, 'refused', this.server.closeDelayMs)
    }

    /** Delivers a MESSAGE on the subscription the client holds for the destination. */
    public message(destination: string, headers: Record<string, string>, body?: string): void {
        const subscription = this.subscriptions.get(destination)
        if (subscription === undefined) {
            throw new Error('the client is not subscribed to ' + destination)
        }
        this.deliver({
            command: 'MESSAGE',
            headers: { subscription, destination, 'message-id': String(++this.messageId), ...headers },
            body: body ?? ''
        })
    }

    private handle(frame: FakeFrame): void {
        switch (frame.command) {
            case 'CONNECT':
            case 'STOMP':
                if (!this.server.silent) {
                    const headers: Record<string, string> = { version: '1.2', 'heart-beat': '0,0' }
                    if (this.server.connectedInfo) {
                        headers[EventConstants.CONNECTED_INFO_HEADER] = JSON.stringify({ replyToId: this.replyToId, participant: { id: 'test' } })
                    }
                    this.deliver({ command: 'CONNECTED', headers, body: '' })
                }
                break
            case 'SUBSCRIBE':
                this.subscriptions.set(frame.headers['destination']!, frame.headers['id']!)
                break
            case 'UNSUBSCRIBE':
                for (const [destination, id] of this.subscriptions) {
                    if (id === frame.headers['id']) {
                        this.subscriptions.delete(destination)
                    }
                }
                break
            case 'SEND':
                this.server.onSend?.(frame, this)
                break
            case 'DISCONNECT':
                if (frame.headers['receipt']) {
                    this.deliver({ command: 'RECEIPT', headers: { 'receipt-id': frame.headers['receipt'] }, body: '' })
                }
                break
        }
    }

    private deliver(frame: FakeFrame): void {
        if (this.readyState !== 1) {
            return
        }
        const headers = Object.entries(frame.headers).map(([key, value]) => key + ':' + value).join('\n')
        const body = frame.body
        const contentLength = body.length > 0 ? '\ncontent-length:' + new TextEncoder().encode(body).length : ''
        const raw = frame.command + '\n' + headers + contentLength + '\n\n' + body + '\0'
        setTimeout(() => this.onmessage?.({ data: raw }), 0)
    }

    // CLOSING until the close event fires, as a real WebSocket is; stompjs waits for that event
    private finish(code: number, reason: string, delayMs: number): void {
        if (this.readyState >= 2) {
            return
        }
        this.readyState = 2
        setTimeout(() => {
            this.readyState = 3
            this.onclose?.({ code, reason, wasClean: code === 1000 })
        }, delayMs)
    }
}

function parseFrame(raw: string): FakeFrame {
    const text = raw.replace(/^\n+/, '')
    const separator = text.indexOf('\n\n')
    const head = separator === -1 ? text : text.substring(0, separator)
    const body = separator === -1 ? '' : text.substring(separator + 2)
    const [command, ...headerLines] = head.split('\n')
    const headers: Record<string, string> = {}
    for (const line of headerLines) {
        const colon = line.indexOf(':')
        if (colon > 0) {
            headers[line.substring(0, colon)] = line.substring(colon + 1).replace(/\\c/g, ':').replace(/\\n/g, '\n')
        }
    }
    return { command: command!, headers, body }
}
