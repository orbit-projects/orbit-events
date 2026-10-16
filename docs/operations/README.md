# Orbit Events: operations

`orbit-events` does not define broker connection settings or `ORBIT_*` environment variables. The
selected adapter owns bootstrap servers or endpoints, credentials, TLS, timeouts, producer
configuration, consumer groups, retries, and shutdown behavior. Use that adapter's deployment
guide as the source of truth.

## Wiring and lifecycle

Construct the provider transport through its adapter plugin or register it as an application
resource. Pass the registered `EventTransport` to `EventClient`. The client does not close the
transport; ensure exactly one lifecycle owner closes the underlying producer, consumer, network
client, and background tasks.

Keep subscriptions tied to application startup and call `unsubscribe` during shutdown when the
adapter requires it. Confirm whether close drains handlers, cancels them, or waits for broker
acknowledgment. Do not assume the opaque token is portable between clients or process restarts.

## Payload and schema operations

Definitions default to a 1 MiB serialized JSON payload limit and may set a lower or higher limit up
to 16 MiB. The bound is checked on encoded UTF-8 JSON. It does not bound pre-serialization model
allocation or provider-specific envelope overhead. Keep events small and store larger artifacts
separately.

Consumers require exact schema versions. Roll out version changes according to the broker's
consumer-group and routing semantics. Before increasing producer versions, make sure consumers that
need the new events are deployed. Monitor schema validation failures as application errors and
define the adapter's retry, reject, or dead-letter response deliberately; automatic retries can
repeat side effects if handlers are not idempotent.

## Delivery behavior

The package does not promise delivery durability, order, exactly-once processing, or a specific
retry count. Test the adapter's acknowledgment timing, duplicate delivery, poison-message handling,
partition/concurrency model, backpressure, and recovery after broker interruption. Make handlers
idempotent where redelivery is possible, and use a transactional outbox or equivalent application
pattern when database changes and event publication must remain consistent.

## Monitoring and rollout checks

Track broker lag and connection health through the adapter's observability integration, plus
application-level validation failures, handler latency, and retry volume. Verify credential
rotation, graceful termination with active handlers, schema-version rollout, and recovery from
unavailable brokers in a representative non-production environment. Unit tests with a fake
transport do not validate broker behavior or deployment topology.
