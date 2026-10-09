import WebSocket, { MessageEvent, CloseEvent, ErrorEvent } from "isomorphic-ws";
import { SubscriptionMessage, Message, ConnectionStatus } from "./model";

const DEFAULT_HOST = "wss://ws-live-data.polymarket.com";
const DEFAULT_PING_INTERVAL = 5000;

/** Delay before the first reconnect attempt, in milliseconds. */
const DEFAULT_RECONNECT_BASE_DELAY = 500;

/** Upper bound for the exponential reconnect backoff, in milliseconds. */
const DEFAULT_MAX_RECONNECT_DELAY = 30_000;

/**
 * Interface representing the arguments for initializing a RealTimeDataClient.
 */
export interface RealTimeDataClientArgs {
    /**
     * Optional callback function that is called when the client successfully connects.
     * @param client - The instance of the RealTimeDataClient that has connected.
     */
    onConnect?: (client: RealTimeDataClient) => void;

    /**
     * Optional callback function that is called when the client receives a message.
     * @param client - The instance of the RealTimeDataClient that received the message.
     * @param message - The message received by the client.
     */
    onMessage?: (client: RealTimeDataClient, message: Message) => void;

    /**
     * Optional callback function that is called when the client receives a connection status update.
     * @param status - The connection status of the client.
     */
    onStatusChange?: (status: ConnectionStatus) => void;

    /**
     * Optional host address to connect to.
     */
    host?: string;

    /**
     * Optional interval in milliseconds for sending ping messages to keep the connection alive.
     */
    pingInterval?: number;

    /**
     * Optional flag to enable or disable automatic reconnection when the connection is lost.
     * Defaults to `true`. Pass `false` to disable reconnection.
     */
    autoReconnect?: boolean;

    /**
     * Optional delay in milliseconds before the first reconnect attempt. Each
     * subsequent attempt doubles this value, with full jitter applied, until
     * {@link maxReconnectDelay} is reached.
     */
    reconnectBaseDelay?: number;

    /**
     * Optional upper bound in milliseconds for the reconnect backoff.
     */
    maxReconnectDelay?: number;
}

/**
 * A client for managing real-time WebSocket connections, handling messages, subscriptions,
 * and automatic reconnections.
 */
export class RealTimeDataClient {
    /** WebSocket server host URL */
    private readonly host: string;

    /** Interval (in milliseconds) for sending ping messages */
    private readonly pingInterval: number;

    /** Determines whether the client should automatically reconnect on disconnection */
    private readonly autoReconnect: boolean;

    /** Delay before the first reconnect attempt, in milliseconds */
    private readonly reconnectBaseDelay: number;

    /** Upper bound for the reconnect backoff, in milliseconds */
    private readonly maxReconnectDelay: number;

    /** Callback function executed when the connection is established */
    private readonly onConnect?: (client: RealTimeDataClient) => void;

    /** Callback function executed when a custom message is received */
    private readonly onCustomMessage?: (client: RealTimeDataClient, message: Message) => void;

    /** Callback function executed on a connection status update */
    private readonly onStatusChange?: (status: ConnectionStatus) => void;

    /**
     * WebSocket instance.
     *
     * Optional rather than definitely assigned: it only exists between
     * `connect()` and the socket being torn down, and the public methods have to
     * cope with being called before `connect()`.
     */
    private ws?: WebSocket;

    /** Handle for the keepalive timer, if one is running. */
    private pingTimer?: ReturnType<typeof setTimeout>;

    /** Handle for the pending reconnect timer, if one is scheduled. */
    private reconnectTimer?: ReturnType<typeof setTimeout>;

    /** Number of consecutive failed connection attempts, used for backoff. */
    private reconnectAttempts = 0;

    /** True once `disconnect()` has been called, to stop all reconnection. */
    private closedByCaller = false;

    /**
     * Constructs a new RealTimeDataClient instance.
     * @param args Configuration options for the client. All fields are optional.
     */
    constructor(args: RealTimeDataClientArgs = {}) {
        // `args` is defaulted rather than non-null asserted. The previous
        // `args!.host` form made `new RealTimeDataClient()` — the documented
        // zero-argument form — throw a TypeError at runtime.
        this.host = args.host || DEFAULT_HOST;

        // `??` rather than `||` so an explicit `0` is not silently replaced, and
        // then validated, because a zero or negative interval would turn the
        // keepalive into a busy loop.
        const pingInterval = args.pingInterval ?? DEFAULT_PING_INTERVAL;
        if (!Number.isFinite(pingInterval) || pingInterval <= 0) {
            throw new Error(
                `pingInterval must be a positive number of milliseconds, received ${pingInterval}`,
            );
        }
        this.pingInterval = pingInterval;

        // `??` is required here: `args.autoReconnect || true` is always `true`,
        // so passing `autoReconnect: false` used to be silently ignored.
        this.autoReconnect = args.autoReconnect ?? true;

        this.reconnectBaseDelay = args.reconnectBaseDelay ?? DEFAULT_RECONNECT_BASE_DELAY;
        this.maxReconnectDelay = args.maxReconnectDelay ?? DEFAULT_MAX_RECONNECT_DELAY;

        this.onCustomMessage = args.onMessage;
        this.onConnect = args.onConnect;
        this.onStatusChange = args.onStatusChange;
    }

    /**
     * Establishes a WebSocket connection to the server.
     *
     * Any socket from a previous attempt is detached and closed first, so a
     * reconnect cannot leave an orphaned socket whose handlers still fire.
     */
    public connect(): this {
        this.closedByCaller = false;
        this.clearReconnectTimer();
        this.teardownSocket();

        this.notifyStatusChange(ConnectionStatus.CONNECTING);

        this.ws = new WebSocket(this.host);
        this.ws.onopen = this.onOpen;
        this.ws.onmessage = this.onMessage;
        this.ws.onclose = this.onClose;
        this.ws.onerror = this.onError;

        return this;
    }

    /**
     * Detaches the handlers from the current socket and closes it.
     *
     * Detaching before closing is what stops a discarded socket from invoking
     * `onClose` and scheduling a second reconnect for a connection the client
     * has already given up on.
     */
    private teardownSocket(): void {
        this.stopKeepalive();

        const socket = this.ws;
        if (!socket) {
            return;
        }

        this.ws = undefined;

        socket.onopen = null;
        socket.onmessage = null;
        socket.onclose = null;
        socket.onerror = null;

        try {
            socket.close();
        } catch {
            // Closing a socket that never finished opening can throw; there is
            // nothing useful to do about it and the handlers are already gone.
        }
    }

    /**
     * Schedules a reconnect attempt using exponential backoff with full jitter.
     *
     * A single timer is used, so the `onerror` + `onclose` pair that a failed
     * connection produces results in one reconnect rather than two. Without this
     * the client used to double the number of in-flight sockets on every
     * failure, and retried with no delay at all.
     */
    private scheduleReconnect(): void {
        if (!this.autoReconnect || this.closedByCaller || this.reconnectTimer !== undefined) {
            return;
        }

        const exponentialDelay = Math.min(
            this.maxReconnectDelay,
            this.reconnectBaseDelay * 2 ** this.reconnectAttempts,
        );

        // Full jitter: a random point in [0, exponentialDelay]. This stops a
        // fleet of clients that dropped together from reconnecting in lockstep.
        const delayMs = Math.random() * exponentialDelay;

        this.reconnectAttempts += 1;

        this.reconnectTimer = setTimeout(() => {
            this.reconnectTimer = undefined;
            if (!this.closedByCaller && this.autoReconnect) {
                this.connect();
            }
        }, delayMs);
    }

    /** Cancels a pending reconnect attempt, if any. */
    private clearReconnectTimer(): void {
        if (this.reconnectTimer !== undefined) {
            clearTimeout(this.reconnectTimer);
            this.reconnectTimer = undefined;
        }
    }

    /**
     * Handles WebSocket 'open' event. Executes the `onConnect` callback and starts the keepalive.
     */
    private onOpen = (): void => {
        // A successful open resets the backoff so the next disconnect starts
        // from the base delay again.
        this.reconnectAttempts = 0;

        this.startKeepalive();
        this.notifyStatusChange(ConnectionStatus.CONNECTED);

        if (this.onConnect) {
            this.onConnect(this);
        }
    };

    /**
     * Starts the keepalive timer.
     *
     * The previous implementation chained the next ping off a `pong` handler
     * assigned as `ws.pong = ...`. `pong` is an event name on the Node `ws`
     * implementation, not an assignable handler property, and it does not exist
     * in the browser at all, so the callback never fired and exactly one ping
     * was ever sent. A self-rescheduling timer keeps the connection alive on
     * both platforms.
     */
    private startKeepalive(): void {
        this.stopKeepalive();

        const sendPing = () => {
            if (!this.ws || this.ws.readyState !== WebSocket.OPEN) {
                return;
            }

            this.rawSend("ping");
            this.pingTimer = setTimeout(sendPing, this.pingInterval);
        };

        this.pingTimer = setTimeout(sendPing, this.pingInterval);
    }

    /** Stops the keepalive timer, if it is running. */
    private stopKeepalive(): void {
        if (this.pingTimer !== undefined) {
            clearTimeout(this.pingTimer);
            this.pingTimer = undefined;
        }
    }

    /**
     * Handles WebSocket errors. Logs the error and schedules a reconnect if `autoReconnect` is enabled.
     *
     * `onerror` is normally followed by `onclose` for the same socket. Both call
     * `scheduleReconnect`, which is idempotent while a reconnect is already
     * pending, so a failure produces exactly one retry.
     * @param err Error object describing the issue.
     */
    private onError = (err: ErrorEvent): void => {
        console.error("error", err);
        this.scheduleReconnect();
    };

    /**
     * Handles WebSocket 'close' event. Logs the disconnect reason and schedules a
     * reconnect if `autoReconnect` is enabled.
     * @param message Close event carrying the code and reason for closure.
     */
    private onClose = (message: CloseEvent): void => {
        console.error("disconnected", "code", message.code, "reason", message.reason);

        this.stopKeepalive();
        this.notifyStatusChange(ConnectionStatus.DISCONNECTED);
        this.scheduleReconnect();
    };

    /**
     * Handles incoming WebSocket messages. Parses and dispatches messages to the
     * `onMessage` callback.
     *
     * Parsing is guarded: an unparseable frame used to throw out of this handler,
     * which surfaces as an unhandled exception rather than a dropped message.
     * Dispatch is also based on the parsed object rather than on searching the
     * raw text for the substring `"payload"`, which both misclassified frames
     * that merely mentioned the word and discarded well-formed frames that did
     * not.
     * @param event Raw WebSocket message data.
     */
    private onMessage = (event: MessageEvent): void => {
        if (typeof event.data !== "string" || event.data.length === 0) {
            return;
        }

        // Keepalive replies are not data messages and carry no JSON body.
        if (event.data === "pong" || event.data === "ping") {
            return;
        }

        if (!this.onCustomMessage) {
            return;
        }

        let parsed: unknown;
        try {
            parsed = JSON.parse(event.data);
        } catch (err) {
            console.error("failed to parse message", err);
            return;
        }

        if (!isMessage(parsed)) {
            console.warn("ignoring message with unexpected shape", { data: event.data });
            return;
        }

        this.onCustomMessage(this, parsed);
    };

    /**
     * Sends a frame on the current socket.
     *
     * The two-argument `send(data, callback)` form is specific to the Node `ws`
     * package; the browser `WebSocket.send()` accepts a single argument and
     * silently ignores the callback, so error reporting used to disappear on the
     * web. This wrapper relies on the synchronous throw that both
     * implementations perform instead, which behaves the same everywhere.
     *
     * @param data Frame payload to send.
     * @returns `true` when the frame was handed to the socket.
     */
    private rawSend(data: string): boolean {
        const socket = this.ws;

        if (!socket || socket.readyState !== WebSocket.OPEN) {
            console.warn("Socket not open. Ready state is:", socket?.readyState);
            return false;
        }

        try {
            socket.send(data);
            return true;
        } catch (err) {
            console.error("send error", err);
            return false;
        }
    }

    /**
     * Closes the WebSocket connection and stops all reconnection.
     */
    public disconnect(): void {
        this.closedByCaller = true;
        this.clearReconnectTimer();
        this.teardownSocket();
    }

    /**
     * Subscribes to a data stream by sending a subscription message.
     * @param msg Subscription request message.
     * @returns `true` when the subscription frame was sent.
     */
    public subscribe(msg: SubscriptionMessage): boolean {
        return this.rawSend(JSON.stringify({ action: "subscribe", ...msg }));
    }

    /**
     * Unsubscribes from a data stream by sending an unsubscription message.
     * @param msg Unsubscription request message.
     * @returns `true` when the unsubscription frame was sent.
     */
    public unsubscribe(msg: SubscriptionMessage): boolean {
        return this.rawSend(JSON.stringify({ action: "unsubscribe", ...msg }));
    }

    /**
     * Callback for connection status changes
     * @param status status of the connection
     */
    private notifyStatusChange(status: ConnectionStatus): ConnectionStatus {
        if (this.onStatusChange) {
            this.onStatusChange(status);
        }
        return status;
    }
}

/**
 * Narrows a parsed JSON value to a {@link Message}.
 *
 * Only the fields the client depends on are checked; unknown extra fields are
 * left alone so the server can add them without breaking existing clients.
 */
function isMessage(value: unknown): value is Message {
    if (typeof value !== "object" || value === null) {
        return false;
    }

    const candidate = value as Partial<Message>;

    return (
        typeof candidate.topic === "string" &&
        typeof candidate.type === "string" &&
        candidate.payload !== undefined
    );
}
