# Orbit Events: operations and security

This guide organizes runtime behavior documented by the package. It does not certify production readiness. Verify provider/client versions, permissions, transport security, limits, and failure behavior in the target environment before release.

## Configuration surface

Environment names found in the package README:

The package README does not name `ORBIT_*` variables. Use its typed constructors and application configuration, and confirm exact runtime inputs in the implementation before deployment.

Use the package README's constructor and deployment examples. Store credentials in a secret manager and avoid logging credentials, raw provider errors, request data, or opaque cursors.

## Lifecycle, failure behavior, and limits

`orbit-events` adds typed, versioned application event definitions over Orbit Core's
`EventTransport`. It is a capability layer, not a second event bus: Core continues to own the
in-process `EventBus`, event envelope, event store, transport protocol, lifecycle, and plugin
orchestration. Broker adapters such as `orbit-events-kafka`, `orbit-events-rabbitmq`, and `orbit-events-nats` remain
separately installable and can be used through the same client.

```bash
python -m pip install orbit-core orbit-events orbit-events-kafka
```

```python
from pydantic import BaseModel
from orbit_events import EventClient, EventDefinition, TypedEvent


class OrderPlaced(BaseModel):
    order_id: str
    total_cents: int


ORDER_PLACED = EventDefinition("orders.placed", OrderPlaced, schema_version=1)
# `transport` is the EventTransport instance registered/owned by the chosen adapter.
events = EventClient(transport)

await events.publish(ORDER_PLACED, OrderPlaced(order_id="o-42", total_cents=1299))


async def on_order_placed(event: TypedEvent[OrderPlaced]) -> None:
    print(event.payload.order_id)


subscription = await events.subscribe(ORDER_PLACED, on_order_placed)
```

Event names follow Core's lowercase bounded identifier format. Publication validates a Pydantic
payload and serializes it into the Core envelope with the schema version in
`orbit.events.schema_version`. Consumption requires an exact schema-version match and validates
the payload before calling application code. A schema mismatch raises `EventSchemaError` through
the transport handler; broker retry/dead-letter behavior is defined by the selected adapter. This
package does not provide schema evolution, a schema registry, exactly-once delivery, or a broker.

Each definition bounds the serialized JSON payload to 1 MiB by default, configurable up to a hard
16 MiB limit. Payloads must serialize as a JSON object, including after custom Pydantic serializers
run. This keeps provider wire data bounded and prevents root/list payloads from being mislabeled as
object payloads in the Core event envelope. Use object storage and pass a reference when event data
would exceed the bound.

Validation and custom serialization failures become `EventSchemaError` messages without payload
values. Publication gives the transport a detached deep copy, so mutation of the returned envelope
cannot alter transport-owned event data.

`EventClient` does not own or close its transport. The adapter's Core plugin manages transport
registration and shutdown. Configure durable delivery, retries, and handler idempotency according
to the chosen broker's documentation.

## Production validation

Validate startup/shutdown cleanup, timeout and cancellation behavior, concurrency and payload bounds where applicable, secret rotation and least-privilege access, data durability, backup/restore, and failover against the selected provider. Do not infer distributed or durable guarantees from an in-process API or fake-client tests.
