# Real time data client

This client provides a wrapper to connect to the `real-time-data-streaming` `WebSocket` service.

## How to use it

Here is a quick example about how to connect to the service and start receiving messages (you can find more in the folder `examples/`):

```typescript
import { RealTimeDataClient } from "../src/client";
import { Message } from "../src/model";

const onMessage = (message: Message): void => {
    console.log(message.topic, message.type, message.payload);
};

const onConnect = (client: RealTimeDataClient): void => {
    // Subscribe to a topic
    client.subscribe({
        subscriptions: [
            {
                topic: "comments",
                type: "*", // "*"" can be used to connect to all the types of the topic
                filters: `{"parentEntityID":100,"parentEntityType":"Event"}`, // empty means no filter
            },
        ],
    });
};

new RealTimeDataClient({ onMessage, onConnect }).connect();
```

## How to subscribe and unsubscribe from messages

Once the connection is stablished and you have a `client: RealTimeDataClient` object, you can `subscribe` and `unsubscribe` to many messages streamings using the same connection.

### Subscribe

Subscribe to 'trades' messages from the topic 'activity' and to the all comments messages.

```typescript
client.subscribe({
    subscriptions: [
        {
            topic: "activity",
            type: "trades",
        },
    ],
});

client.subscribe({
    subscriptions: [
        {
            topic: "comments",
            type: "*", // "*"" can be used to connect to all the types of the topic
        },
    ],
});
```

### Unsubscribe

Unsubscribe from the new trades messages of the topic 'activity'. If 'activity' has more messages types and I used '\*' to connect to all of them, this will only unsubscribe from the type 'trades'.

```typescript
client.subscribe({
    subscriptions: [
        {
            topic: "activity",
            type: "trades",
        },
    ],
});
```

### Disconnect

The `client` object provides a method to disconnect from the `WebSocket` server:

```typescript
client.disconnect();
```

## Messages hierarchy

| Topic    | Type             | Auth | Filters (if it is empty the messages won't be filtered)         | Schema   |
| -------- | ---------------- | ---- | --------------------------------------------------------------- | -------- |
| activity | trades           | -    | '{"event_slug":"string"}' OR '{"market_slug":"string"}'         | Trade    |
| comments | comment_created  | -    | '{"parentEntityID":number,"parentEntityType":"Event / Series"}' | Comment  |
| comments | comment_removed  | -    | '{"parentEntityID":number,"parentEntityType":"Event / Series"}' | Comment  |
| comments | reaction_created | -    | '{"parentEntityID":number,"parentEntityType":"Event / Series"}' | Reaction |
| comments | reaction_removed | -    | '{"parentEntityID":number,"parentEntityType":"Event / Series"}' | Reaction |
| rfq      | request_created  | -    | -                                                               | Request  |
| rfq      | request_edited   | -    | -                                                               | Request  |
| rfq      | request_canceled | -    | -                                                               | Request  |
| rfq      | request_expired  | -    | -                                                               | Request  |
| rfq      | quote_created    | -    | -                                                               | Quote    |
| rfq      | quote_edited     | -    | -                                                               | Quote    |
| rfq      | quote_canceled   | -    | -                                                               | Quote    |
| rfq      | quote_expired    | -    | -                                                               | Quote    |

### Trade

| Name                  | Type    | Description                                        |
| --------------------- | ------- | -------------------------------------------------- |
| asset                 | string  | ERC1155 token ID of conditional token being traded |
| bio                   | string  | Bio of the user of the trade                       |
| conditionId           | string  | Id of market which is also the CTF condition ID    |
| eventSlug             | string  | Slug of the event                                  |
| icon                  | string  | URL to the market icon image                       |
| name                  | string  | Name of the user of the trade                      |
| outcome               | string  | Human readable outcome of the market               |
| outcomeIndex          | integer | Index of the outcome                               |
| price                 | float   | Price of the trade                                 |
| profileImage          | string  | URL to the user profile image                      |
| profileImageOptimized | string  | -                                                  |
| proxyWallet           | string  | Address of the user proxy wallet                   |
| pseudonym             | string  | Pseudonym of the user                              |
| side                  | string  | Side of the trade (BUY/SELL)                       |
| size                  | integer | Size of the trade                                  |
| slug                  | string  | Slug of the market                                 |
| timestamp             | integer | Timestamp of the trade                             |
| title                 | string  | Title of the event                                 |
| transactionHash       | string  | Hash of the transaction                            |

### Comment

| Name             | Type   | Description                                 |
| ---------------- | ------ | ------------------------------------------- |
| id               | string | Unique identifier of comment                |
| body             | string | Content of the comment                      |
| parentEntityType | string | Type of the parent entity (Event or Series) |
| parentEntityID   | number | ID of the parent entity                     |
| parentCommentID  | string | ID of the parent comment                    |
| userAddress      | string | Address of the user                         |
| replyAddress     | string | Address of the reply user                   |
| createdAt        | string | Creation timestamp                          |
| updatedAt        | string | Last update timestamp                       |

### Reaction

| Name         | Type   | Description                    |
| ------------ | ------ | ------------------------------ |
| id           | string | Unique identifier of reaction  |
| commentID    | number | ID of the comment              |
| reactionType | string | Type of the reaction           |
| icon         | string | Icon representing the reaction |
| userAddress  | string | Address of the user            |
| createdAt    | string | Creation timestamp             |

### Request

| Name         | Type   | Description                                                   |
| ------------ | ------ | ------------------------------------------------------------- |
| requestId    | string | Unique identifier for the request                             |
| proxyAddress | string | User proxy address                                            |
| market       | string | Id of market which is also the CTF condition ID               |
| token        | string | ERC1155 token ID of conditional token being traded            |
| complement   | string | Complement ERC1155 token ID of conditional token being traded |
| state        | string | Current state of the request                                  |
| side         | string | Indicates buy or sell side                                    |
| sizeIn       | number | Input size of the request                                     |
| sizeOut      | number | Output size of the request                                    |
| price        | number | Price from in/out sizes                                       |
| expiry       | number | Expiry timestamp (UNIX format)                                |

### Quote

| Name         | Type   | Description                                                   |
| ------------ | ------ | ------------------------------------------------------------- |
| quoteId      | string | Unique identifier for the quote                               |
| requestId    | string | Associated request identifier                                 |
| proxyAddress | string | User proxy address                                            |
| token        | string | ERC1155 token ID of conditional token being traded            |
| state        | string | Current state of the quote                                    |
| side         | string | Indicates buy or sell side                                    |
| sizeIn       | number | Input size of the quote                                       |
| sizeOut      | number | Output size of the quote                                      |
| sizeOut      | number | Output size of the request                                    |
| condition    | string | Id of market which is also the CTF condition ID               |
| complement   | string | Complement ERC1155 token ID of conditional token being traded |
| expiry       | number | Expiry timestamp (UNIX format)                                |
