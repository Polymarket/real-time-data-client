import WebSocket, { MessageEvent, CloseEvent, ErrorEvent } from "isomorphic-ws";
    import { SubscriptionMessage, Message, ConnectionStatus } from "./model";

    const DEFAULT_HOST = "wss://ws-live-data.polymarket.com";
    const DEFAULT_PING_INTERVAL = 5000;
    const DEFAULT_RECONNECT_DELAY_MS = 1_000;
    const MAX_RECONNECT_DELAY_MS = 30_000;

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
       * Optional callback function that is called when the client receives a connection status update.
       * @param status - The connection status of the client.
       */
      onStatusChange?: (status: ConnectionStatus) => void;

      /** Optional host address to connect to. */
      host?: string;

      /** Optional interval in milliseconds for sending ping messages to keep the connection alive. */
      pingInterval?: number;

      /**
       * Optional flag to enable or disable automatic reconnection when the connection is lost.
       * Defaults to true.
       */
      autoReconnect?: boolean;

      /**
       * Initial reconnect delay in milliseconds. Doubles on each failed attempt (exponential
       * back-off) up to maxReconnectDelay. Defaults to 1000 ms.
       */
      reconnectDelay?: number;

      /**
       * Maximum reconnect delay in milliseconds. Caps the exponential back-off growth.
       * Defaults to 30 000 ms (30 s).
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
      private autoReconnect: boolean;

      /** Initial back-off delay in milliseconds */
      private readonly reconnectDelayBase: number;

      /** Maximum back-off delay in milliseconds */
      private readonly maxReconnectDelay: number;

      /** Current reconnect attempt count — used to compute exponential back-off */
      private reconnectAttempt = 0;

      /** Pending setTimeout handle for the next reconnect attempt */
      private reconnectTimer: ReturnType<typeof setTimeout> | null = null;

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

      /**
       * Monotonically-increasing id stamped onto each WebSocket at connect() time.
       * close/error handlers compare their captured id against the current value to
       * decide whether they belong to the live socket or a stale dying one. This
       * prevents the double-reconnect triggered when both onError and onClose fire
       * on the same closed socket.
       */
      private connectionId = 0;

      /**
       * Constructs a new RealTimeDataClient instance.
       * @param args Configuration options for the client.
       */
      constructor(args?: RealTimeDataClientArgs) {
          this.host = args?.host || DEFAULT_HOST;
          this.pingInterval = args?.pingInterval || DEFAULT_PING_INTERVAL;
          // Fix: use ?? instead of || so that explicitly passing false is honoured.
          // The previous || true treated false as falsy and always enabled autoReconnect.
          this.autoReconnect = args?.autoReconnect ?? true;
          this.reconnectDelayBase = args?.reconnectDelay ?? DEFAULT_RECONNECT_DELAY_MS;
          this.maxReconnectDelay = args?.maxReconnectDelay ?? MAX_RECONNECT_DELAY_MS;
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
          const id = ++this.connectionId;
          this.notifyStatusChange(ConnectionStatus.CONNECTING);
          this.ws = new WebSocket(this.host);
          if (this.ws) {
              this.ws.onopen = () => this.onOpen(id);
              this.ws.onmessage = this.onMessage;
              this.ws.onclose = (event: CloseEvent) => this.onClose(id, event);
              this.ws.onerror = (err: ErrorEvent) => this.onError(id, err);
              this.ws.pong = this.onPong;
          }
          return this;
      }

      /**
       * Handles WebSocket open event. Resets back-off, starts pinging, fires onConnect.
       */
      private onOpen = (id: number) => {
          this.reconnectAttempt = 0;
          this.ping();
          this.notifyStatusChange(ConnectionStatus.CONNECTED);
          if (this.onConnect) {
              this.onConnect(this);
          }
      };

      /**
       * Handles WebSocket pong event. Continues the ping cycle.
       */
      private onPong = async () => {
          delay(this.pingInterval).then(() => this.ping());
      };

      /**
       * Handles WebSocket errors. Notifies caller and schedules a reconnect with back-off.
       * @param id Connection id captured at connect() time.
       * @param err Error object describing the issue.
       */
      private onError = (id: number, err: ErrorEvent) => {
          console.error("error", err);
          if (this.onUserError) {
              try { this.onUserError(this, err); } catch (e) { console.error("onError callback threw:", e); }
          }
          if (this.autoReconnect && id === this.connectionId) {
              this.scheduleReconnect();
          }
      };

      /**
       * Handles WebSocket close event. Notifies caller and schedules a reconnect with back-off.
       * @param id Connection id captured at connect() time.
       * @param message CloseEvent carrying the code and reason.
       */
      private onClose = (id: number, message: CloseEvent) => {
          console.error("disconnected", "code", message.code, "reason", message.reason);
          this.notifyStatusChange(ConnectionStatus.DISCONNECTED);
          if (this.onUserClose) {
              try { this.onUserClose(this, message); } catch (e) { console.error("onClose callback threw:", e); }
          }
          if (this.autoReconnect && id === this.connectionId) {
              this.scheduleReconnect();
          }
      };

      /**
       * Schedules a reconnect attempt using exponential back-off with jitter.
       *
       *   delay = min(reconnectDelay x 2^attempt, maxReconnectDelay) + rand(0..1000) ms
       *
       * Back-off prevents thundering herd on server restarts and stops the heap-memory
       * growth observed when a persistent network error (e.g. close code 1006) causes
       * connect() to be called in a tight loop, accumulating WebSocket objects faster
       * than the garbage collector can free them (see issue #38).
       */
      private scheduleReconnect() {
          if (this.reconnectTimer !== null) return;
          const delayMs =
              Math.min(
                  this.reconnectDelayBase * Math.pow(2, this.reconnectAttempt),
                  this.maxReconnectDelay,
              ) + Math.random() * 1_000;
          this.reconnectAttempt++;
          console.log("Reconnecting in " + Math.round(delayMs) + "ms (attempt " + this.reconnectAttempt + ")");
          this.reconnectTimer = setTimeout(() => {
              this.reconnectTimer = null;
              this.connect();
          }, delayMs);
      }

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
       * Closes the WebSocket connection and cancels any pending reconnect.
       */
      public disconnect() {
          this.autoReconnect = false;
          if (this.reconnectTimer !== null) {
              clearTimeout(this.reconnectTimer);
              this.reconnectTimer = null;
          }
          if (this.ws && this.ws.readyState !== WebSocket.CLOSED && this.ws.readyState !== WebSocket.CLOSING) {
              this.ws.close();
          }
      }

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
       * Callback for connection status changes.
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
    