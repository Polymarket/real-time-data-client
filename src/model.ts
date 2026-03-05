/**
 * Authentication details for Gamma authentication.
 */
export interface GammaAuth {
    /** Address used for authentication */
    address: string;
}

/**
 * Message structure for subscription requests.
 */
export interface SubscriptionMessage {
    subscriptions: {
        /** Topic to subscribe to */
        topic: string;

        /** Type of subscription */
        type: string;

        /** Optional filters for the subscription */
        filters?: string;

        /** Optional Gamma authentication credentials */
        gamma_auth?: GammaAuth;
    }[];
}

/**
 * Represents a real-time message received from the WebSocket server.
 */
export interface Message {
    /** Topic of the message */
    topic: string;

    /** Type of the message */
    type: string;

    /** Timestamp of when the message was sent */
    timestamp: number;

    /** Payload containing the message data */
    payload: object;

    /** Connection ID */
    connection_id: string;
}

/**
 * Represents websocket connection status
 */
export enum ConnectionStatus {
    CONNECTING = "CONNECTING",
    CONNECTED = "CONNECTED",
    DISCONNECTED = "DISCONNECTED",
}
