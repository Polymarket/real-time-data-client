# RTDS troubleshooting and actionable bug reports

The most useful production reports let another builder reproduce the failure and let maintainers identify the responsible layer. Use this sequence:

> **Symptom → evidence → workaround → requested platform fix**

Do not include API keys, wallet private keys, authentication headers, session tokens, or other credentials in an issue or log attachment.

## Report template

### Symptom

Describe the externally visible failure in one or two sentences.

Examples:

- "The socket remains `OPEN` and answers pings, but all subscribed topics stop delivering payloads."
- "A subscription is accepted, but the requested topic never produces an initial snapshot."
- "Messages continue to arrive, but their source timestamps are delayed by more than 30 minutes."

Include whether the failure is intermittent or permanent and whether a new connection recovers it.

### Evidence

Provide enough detail to distinguish transport, subscription, and data-source failures:

- UTC start time, failure time, and recovery time
- RTDS host and exact subscription frame (`topic`, `type`, and `filters`)
- Client package version, Node/browser version, operating system, and WebSocket implementation
- Last valid payload time per topic—not only the last ping, pong, or metadata frame
- Socket `readyState`, close code/reason, and error callbacks
- Sanitized raw frames immediately before and after the failure
- Message counts and reconnect attempts
- Whether multiple topics on the same socket stopped together
- Whether a hard reconnect restored data

When possible, run a raw-frame capture beside the SDK client. This distinguishes an SDK parsing problem from a server-side publishing problem.

### Workaround

Document the smallest mitigation that restored service, including thresholds and possible false positives. A useful workaround should be safe for other builders to copy while the root cause is investigated.

### Requested platform fix

Name the invariant the platform should provide rather than only asking maintainers to "fix the socket."

Examples:

- A live gateway connection must retain valid backend subscription state.
- If backend subscription state is removed, the gateway should close the socket with an explicit reason.
- Subscription failures should return stable application-level error codes rather than internal database errors.

## Distinguish transport liveness from data liveness

A WebSocket that is `OPEN` and answering pings is not necessarily delivering subscribed data. Ping/pong may only prove that the gateway and network path are alive.

For production consumers:

1. Track the last **valid data payload** independently from ping/pong traffic.
2. For naturally quiet subscriptions, add a normally active topic to the same socket as a data-plane heartbeat.
3. If all expected payloads become stale, hard-close the socket and create a new connection.
4. Restore subscriptions from `onConnect` after reconnecting.
5. Add jitter to reconnect delays to avoid synchronized reconnect storms.

Do not use repeated duplicate subscriptions or intentionally triggered server/database errors as a production health check. They add load and couple the client to unstable implementation details.

## Known silent-stall failure mode

Issue [#26](https://github.com/Polymarket/real-time-data-client/issues/26) documents a failure where all topics on a connection can stop while the socket remains open and ping/pong continues. In the reported tests, frozen connections did not recover through re-subscription; a new socket restored delivery.

The current client reconnects after socket close/error events. Applications that require continuous delivery should also implement data-payload staleness detection until the client or server provides an explicit data-plane liveness contract.

## Minimal watchdog pattern

```ts
const STALE_AFTER_MS = 15_000;
let lastPayloadAt = Date.now();
let nextReconnectAt = 0;

const client = new RealTimeDataClient({
  onConnect: current => {
    current.subscribe({ subscriptions: activeSubscriptions });
  },
  onMessage: (_current, message) => {
    if (message?.payload) lastPayloadAt = Date.now();
    handleMessage(message);
  },
});

setInterval(() => {
  const now = Date.now();
  if (now - lastPayloadAt <= STALE_AFTER_MS) return;
  if (now < nextReconnectAt) return;

  // Replace the socket. Re-subscribing on a stale connection may not restore
  // backend subscription state. Throttle retries without marking data fresh;
  // only a valid payload updates lastPayloadAt.
  nextReconnectAt = now + STALE_AFTER_MS;
  client.disconnect();
  client.connect();
}, 1_000);
```

Choose the threshold based on the expected cadence of the heartbeat topic, not on a naturally quiet market topic. Production implementations should also guard against overlapping reconnects and reset/cancel watchdog timers during intentional shutdown.
