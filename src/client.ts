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

      /**
       * Current reconnect attempt count — used to compute exponential back-off.
       * Reset to 0 on successful open, on disconnect(), and when connect() cancels
       * a pending timer (i.e. a manual reconnect that overrides the back-off schedule).
       */
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
       *
       * Every handler (onOpen, onClose, onError, onPong) captures this id in its
       * closure and checks it against the current value before acting. This guarantees
       * that a delayed or stale event from a superseded socket cannot:
       *   - reset reconnectAttempt / start a duplicate ping loop (onOpen)
       *   - falsely mark the client DISCONNECTED (onClose / onError)
       *   - fire user callbacks against the wrong lifecycle
       *   - continue a keepalive ping chain from a dead connection (onPong)
       *   - trigger a double reconnect when both onError and onClose fire together
       */
      private connectionId = 0;

      /**
       * The id of the most recent socket that successfully reached the OPEN state.
       * Set to the socket's id inside onOpen alongside the CONNECTED status emission.
       *
       * Used by onClose to make two independent decisions:
       *
       * 1. Notify? — only if wasConnected (id === lastConnectedId).
       *    A socket that reached CONNECTED published a status event; its close must
       *    publish the paired DISCONNECTED. A socket that never opened (failed
       *    handshake, or replaced before connecting) must NOT emit DISCONNECTED —
       *    doing so violates the state machine by producing CONNECTING → DISCONNECTED
       *    without CONNECTED in between.
       *
       * 2. Reconnect? — only if isCurrent (id === connectionId).
       *    Retry the connection regardless of whether the socket ever connected;
       *    a failed handshake is just as much a reason to back-off and retry as a
       *    mid-session drop. Skip if a replacement socket is already opening.
       */
      private lastConnectedId = -1;

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
       *
       * Safe to call at any time:
       * - Cancels any pending back-off timer so a scheduled retry cannot open a
       *   second socket alongside the one being created here.
       * - Closes the previous WebSocket (if still open) to prevent file-descriptor
       *   and connection-limit leaks when connect() is called while a socket is live.
       *
       * Ordering is critical: connectionId is incremented BEFORE the old socket is
       * closed. This ensures the old socket's onClose handler sees a stale id and
       * applies the correct notification logic — emit DISCONNECTED only if it was
       * ever connected (lastConnectedId), skip reconnect since a replacement is
       * already starting.
       *
       * Also resets reconnectAttempt when it cancels a pending timer, so a manual
       * reconnect always starts the back-off sequence from the initial delay.
       */
      public connect() {
          // Cancel any pending back-off retry.
          if (this.reconnectTimer !== null) {
              clearTimeout(this.reconnectTimer);
              this.reconnectTimer = null;
              // Manual connect overrides the schedule — start back-off from scratch.
              this.reconnectAttempt = 0;
          }

          // Increment connectionId FIRST so that when we close the old socket below,
          // its onClose fires with a stale id and follows the correct notification
          // path (emit DISCONNECTED only if it was previously connected).
          const id = ++this.connectionId;

          // Close the previous socket if still open to prevent resource leaks.
          // Handlers are already stale (id check) so this close is silent.
          if (this.ws && this.ws.readyState !== WebSocket.CLOSED && this.ws.readyState !== WebSocket.CLOSING) {
              this.ws.close();
          }

          this.notifyStatusChange(ConnectionStatus.CONNECTING);
          this.ws = new WebSocket(this.host);
          if (this.ws) {
              this.ws.onopen = () => this.onOpen(id);
              this.ws.onmessage = this.onMessage;
              this.ws.onclose = (event: CloseEvent) => this.onClose(id, event);
              this.ws.onerror = (err: ErrorEvent) => this.onError(id, err);
              this.ws.pong = (data: Buffer) => this.onPong(id);
          }
          return this;
      }

      /**
       * Handles WebSocket open event. Records lastConnectedId, resets back-off,
       * starts pinging, emits CONNECTED, and fires onConnect.
       *
       * Setting lastConnectedId here is the authoritative record that this socket
       * reached CONNECTED state. onClose uses it to decide whether to emit the
       * paired DISCONNECTED notification.
       *
       * Guards against stale open events from superseded sockets.
       */
      private onOpen = (id: number) => {
          if (id !== this.connectionId) return;
          this.lastConnectedId = id;
          this.reconnectAttempt = 0;
          this.ping();
          this.notifyStatusChange(ConnectionStatus.CONNECTED);
          if (this.onConnect) {
              this.onConnect(this);
          }
      };

      /**
       * Handles WebSocket pong event. Schedules the next ping after the configured
       * interval, tied to the connection id so a stale pong chain from a superseded
       * socket cannot duplicate keepalive traffic on the current connection.
       */
      private onPong = (id: number) => {
          delay(this.pingInterval).then(() => {
              if (id === this.connectionId) {
                  this.ping();
              }
          });
      };

      /**
       * Handles WebSocket errors. Notifies the user onError callback.
       *
       * Does NOT emit DISCONNECTED — onClose always fires after onError in the
       * WebSocket lifecycle and is the single place that decides whether to emit
       * DISCONNECTED (only when the socket previously reached CONNECTED state).
       *
       * Does NOT schedule a reconnect — onClose handles that too, preventing a
       * double-reconnect from both handlers firing on the same socket.
       */
      private onError = (id: number, err: ErrorEvent) => {
          if (id !== this.connectionId) return;
          console.error("error", err);
          if (this.onUserError) {
              try { this.onUserError(this, err); } catch (e) { console.error("onError callback threw:", e); }
          }
      };

      /**
       * Handles WebSocket close event.
       *
       * Three independent concerns are separated using two flags:
       *   wasConnected = id === lastConnectedId  (socket reached OPEN state)
       *   isCurrent    = id === connectionId     (no replacement socket started yet)
       *
       * 1. DROP SILENTLY — if !wasConnected && !isCurrent
       *    The socket never connected AND has already been superseded. Nothing was
       *    ever published for it so there is nothing to unpublish.
       *
       * 2. NOTIFY (DISCONNECTED + onUserClose) — only if wasConnected
       *    A socket that reached CONNECTED published that status; its close must
       *    emit the paired DISCONNECTED regardless of whether connectionId has
       *    since advanced. A socket that closed before onOpen fired (failed
       *    handshake, immediate network error) must NOT emit DISCONNECTED — that
       *    would produce CONNECTING → DISCONNECTED without CONNECTED in between,
       *    violating the state machine.
       *
       * 3. RECONNECT (scheduleReconnect) — only if isCurrent
       *    Retry whenever the current socket closes, whether or not it ever
       *    connected — a failed handshake is just as valid a reason to back-off
       *    and retry as a mid-session drop. Skip if a replacement is already open.
       *    Re-check connectionId after the user callback in case it called connect().
       */
      private onClose = (id: number, message: CloseEvent) => {
          const wasConnected = id === this.lastConnectedId;
          const isCurrent    = id === this.connectionId;

          if (!wasConnected && !isCurrent) return;

          console.error("disconnected", "code", message.code, "reason", message.reason);

          // Emit DISCONNECTED and invoke onUserClose only when the socket previously
          // reached CONNECTED state. A failed-handshake close must not emit DISCONNECTED
          // (no CONNECTED was published so there is no paired status to unpublish).
          if (wasConnected) {
              this.notifyStatusChange(ConnectionStatus.DISCONNECTED);
              if (this.onUserClose) {
                  try { this.onUserClose(this, message); } catch (e) { console.error("onClose callback threw:", e); }
              }
          }

          // Reconnect if this is still the active connection and autoReconnect is on.
          // Re-check connectionId after the user callback: if onUserClose called connect(),
          // connectionId has advanced and we must not open a second socket on top.
          if (this.autoReconnect && isCurrent && id === this.connectionId) {
              this.scheduleReconnect();
          }
      };

      /**
       * Schedules a reconnect attempt using exponential back-off with jitter.
       *
       *   delay = min(reconnectDelay x 2^attempt, maxReconnectDelay) + rand(0..1000) ms
       *
       * The timer callback re-checks autoReconnect before calling connect(). This
       * closes a race where disconnect() is called after the timer fires but before
       * this callback runs — clearTimeout() cannot stop a callback already queued.
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
              if (this.autoReconnect) {
                  this.connect();
              }
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
       * Handles incoming WebSocket messages.
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
       * Closes the WebSocket connection and cancels any pending reconnect timer.
       * Resets reconnectAttempt so a subsequent connect() starts back-off fresh.
       */
      public disconnect() {
          this.autoReconnect = false;
          this.reconnectAttempt = 0;
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
    