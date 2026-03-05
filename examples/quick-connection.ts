import { RealTimeDataClient } from "../src/client";
import { Message } from "../src/model";

const onMessage = (_: RealTimeDataClient, message: Message): void => {
    console.log(
        message.topic,
        message.type,
        message.timestamp,
        message.connection_id,
        message.payload,
    );
};

const onConnect = (client: RealTimeDataClient): void => {
    // Subscribe to a topic
    client.subscribe({
        subscriptions: [
            // comments
            {
                topic: "comments",
                type: "*", // "*"" can be used to connect to all the types of the topic
                //filters: `{"parentEntityID":20200,"parentEntityType":"Event"}`,
            },

            // activity
            {
                topic: "activity",
                type: "*",
                //filters: `{"event_slug":"slug"}`, // filters: `{"market_slug":"slug"}
            },

            // crypto_prices
            {
                topic: "crypto_prices",
                type: "*",
                filters: "", // filters: `{"symbol":"btCUSDt"}`,
            },

            // crypto_prices_chainlink
            {
                topic: "crypto_prices_chainlink",
                type: "*",
                filters: "", // filters: `{"symbol":"eth/usd"}`,
            },

            // equity_prices
            {
                topic: "equity_prices",
                type: "*",
                filters: "", // filters: `{"symbol":"AAPL"}`,
            },
        ],
    });

    /*
    // Unsubscribe from a topic
    client.subscribe({
        subscriptions: [
            {
                topic: "activity",
                type: "trades",
            },
        ],
    });
    */
};

new RealTimeDataClient({ onConnect, onMessage }).connect();
