import WebSocket, { MessageEvent, CloseEvent, ErrorEvent } from "isomorphic-ws";
import { SubscriptionMessage, Message, ConnectionStatus } from "./model";

const DEFAULT_HOST = "wss://ws-live-data.polymarket.com";
const DEFAULT_PING_INTERVAL = 5000;

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
     * Optional callback function that is called when the connection is closed.
     * Receives the close event so callers can inspect the code and reason.
     * @param client - The instance of the RealTimeDataClient that disconnected.
     * @param event - The WebSocket CloseEvent containing code and reason.
     */
    onClose?: (client: RealTimeDataClient, event: CloseEvent) => void;

    /**
     * Optional callback function that is called when a WebSocket error occurs.
     * @param client - The instance of the RealTimeDataClient that errored.
     * @param error - The ErrorEvent describing the error.
     */
    onError?: (client: RealTimeDataClient, error: ErrorEvent) => void;

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
     * Defaults to true.
     */
    autoReconnect?: boolean;
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
    private autoReconnect: boolean;

    /** Callback function executed when the connection is established */
    private readonly onConnect?: (client: RealTimeDataClient) => void;

    /** Callback function executed when a custom message is received */
    private readonly onCustomMessage?: (client: RealTimeDataClient, message: Message) => void;

    /** Callback function executed on a connection status update */
    private readonly onStatusChange?: (status: ConnectionStatus) => void;

    /** User-provided callback executed when the connection closes */
    private readonly onUserClose?: (client: RealTimeDataClient, event: CloseEvent) => void;

    /** User-provided callback executed when a WebSocket error occurs */
    private readonly onUserError?: (client: RealTimeDataClient, error: ErrorEvent) => void;

    /** WebSocket instance */
    private ws!: WebSocket;

    /** Monotonically-increasing counter stamped onto each socket at connect()
     *  time. close/error handlers compare their captured id against this value
     *  to decide whether they belong to the current socket or a stale dying one.
     *  This is the only reliable way to prevent double-reconnect from onError+onClose
     *  firing on the same socket while also allowing retry when a reconnect attempt
     *  itself fails before onOpen. (Cursor Bugbot review) */
    private connectionId = 0;

    /**
     * Constructs a new RealTimeDataClient instance.
     * @param args Configuration options for the client.
     */
    constructor(args?: RealTimeDataClientArgs) {
        // Use optional chaining (args?.) throughout so that calling
        // new RealTimeDataClient() with no arguments does not throw
        // "Cannot read properties of undefined". (Graphite review)
        this.host = args?.host || DEFAULT_HOST;
        this.pingInterval = args?.pingInterval || DEFAULT_PING_INTERVAL;
        // Fix: use ?? instead of || so that explicitly passing `false` is respected.
        // Using `|| true` treated false as falsy and always enabled autoReconnect.
        this.autoReconnect = args?.autoReconnect ?? true;
        this.onCustomMessage = args?.onMessage;
        this.onConnect = args?.onConnect;
        this.onStatusChange = args?.onStatusChange;
        this.onUserClose = args?.onClose;
        this.onUserError = args?.onError;
    }

    /**
     * Establishes a WebSocket connection to the server.
     */
    public connect() {
        // Stamp this socket with a unique id. Handlers that capture this id
        // can detect whether they belong to the current socket or a stale one.
        const id = ++this.connectionId;
        this.notifyStatusChange(ConnectionStatus.CONNECTING);
        this.ws = new WebSocket(this.host);
        if (this.ws) {
            this.ws.onopen = this.onOpen;
            this.ws.onmessage = this.onMessage;
            // Lambdas (not direct assignment) so we can pass the id through.
            this.ws.onclose = (event: CloseEvent) => this.onClose(event, id);
            this.ws.onerror = (err: ErrorEvent) => this.onError(err, id);
            this.ws.pong = this.onPong;
        }
        return this;
    }

    /**
     * Handles WebSocket 'open' event. Executes the `onConnect` callback and starts pinging.
     */
    private onOpen = async () => {
        this.ping();
        this.notifyStatusChange(ConnectionStatus.CONNECTED);
        if (this.onConnect) {
            this.onConnect(this);
        }
    };

    /**
     * Handles WebSocket 'pong' event. Continues the ping cycle.
     */
    private onPong = async () => {
        delay(this.pingInterval).then(() => this.ping());
    };

    /**
     * Handles WebSocket errors. Invokes the user-provided onError callback (if any),
     * then attempts reconnection if `autoReconnect` is enabled.
     * @param err Error object describing the issue.
     */
    private onError = async (err: ErrorEvent, id: number) => {
        // Stale socket: a newer connection is already in flight.
        // onError on the dying old socket after we already called connect() — ignore.
        if (id !== this.connectionId) {
            return;
        }
        console.error("error", err);
        // Guard: wrap callback so a throwing onError handler does not abort
        // the autoReconnect logic below. (Graphite review)
        try {
            if (this.onUserError) {
                this.onUserError(this, err);
            }
        } catch (callbackError) {
            console.error("Error in onError callback:", callbackError);
        }
        // connect() increments connectionId, so the dying socket's onClose
        // (which fires next) will see id !== connectionId and return early.
        if (this.autoReconnect) {
            this.connect();
        }
    };

    /**
     * Handles WebSocket 'close' event. Invokes the user-provided onClose callback (if any),
     * logs the disconnect reason, and attempts reconnection if `autoReconnect` is enabled.
     * @param message Close event containing code and reason.
     */
    private onClose = async (message: CloseEvent, id: number) => {
        // Always deliver DISCONNECTED and the onUserClose callback regardless
        // of whether this is a stale socket. The CloseEvent carries close-code
        // and reason details that are only available here (not in onError), so
        // callers need them even when onError already ran and a reconnect is
        // already in flight. (Cursor Bugbot: "Stale close skips callbacks")
        console.error("disconnected", "code", message.code, "reason", message.reason);
        this.notifyStatusChange(ConnectionStatus.DISCONNECTED);
        // Guard: wrap callback so a throwing onClose handler does not abort
        // the autoReconnect logic below. (Symmetric with onError fix)
        try {
            if (this.onUserClose) {
                this.onUserClose(this, message);
            }
        } catch (callbackError) {
            console.error("Error in onClose callback:", callbackError);
        }
        // Only trigger a reconnect from the current socket. If onError already
        // called connect() for this socket, connectionId was incremented and id
        // is now stale — skip to avoid opening a duplicate connection.
        if (this.autoReconnect && id === this.connectionId) {
            this.connect();
        }
    };

    /**
     * Sends a ping message to keep the connection alive.
     */
    private ping = async () => {
        if (this.ws.readyState !== WebSocket.OPEN) {
            return console.warn("Socket not open. Ready state is:", this.ws.readyState);
        }

        this.ws.send("ping", (err: Error | undefined) => {
            if (err) {
                console.error("ping error", err);
            }
        });
    };

    /**
     * Handles incoming WebSocket messages. Parses and processes custom messages if applicable.
     * @param event Raw WebSocket message data.
     */
    private onMessage = (event: MessageEvent): void => {
        if (typeof event.data === "string" && event.data.length > 0) {
            if (this.onCustomMessage && event.data.includes("payload")) {
                const message = JSON.parse(event.data);
                this.onCustomMessage(this, message as Message);
            } else {
                console.log("onMessage error", { event });
            }
        }
    };

    /**
     * Subscribes to a data stream by sending a subscription message.
     * @param msg Subscription request message.
     */
    public subscribe(msg: SubscriptionMessage) {
        if (this.ws.readyState !== WebSocket.OPEN) {
            return console.warn("Socket not open. Ready state is:", this.ws.readyState);
        }
        this.ws.send(JSON.stringify({ action: "subscribe", ...msg }), (err?: Error) => {
            if (err) {
                console.error("subscribe error", err);
                this.ws.close();
            }
        });
    }

    /**
     * Unsubscribes from a data stream by sending an unsubscription message.
     * @param msg Unsubscription request message.
     */
    public unsubscribe(msg: SubscriptionMessage) {
        if (this.ws.readyState !== WebSocket.OPEN) {
            return console.warn("Socket not open. Ready state is:", this.ws.readyState);
        }
        console.log("unsubscribing", { msg });
        this.ws.send(JSON.stringify({ action: "unsubscribe", ...msg }), (err?: Error) => {
            if (err) {
                console.error("unsubscribe error", err);
                this.ws.close();
            }
        });
    }

    /**
     * Closes the WebSocket connection.
     */
    public disconnect() {
        this.autoReconnect = false;
        this.ws.close();
    }

    /**
     * Callback for connection status changes
     * @param status status of the connection
     */
    private notifyStatusChange(status: ConnectionStatus) {
        if (this.onStatusChange) {
            this.onStatusChange(status);
        }
        return status;
    }
}

function delay(ms: number) {
    return new Promise(resolve => setTimeout(resolve, ms));
}
