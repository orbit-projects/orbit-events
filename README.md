# Orbit Events

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

The shared `encode_cloudevent` and `decode_cloudevent` helpers implement CloudEvents 1.0 structured
JSON with bounded UTF-8 payloads. The Kafka, NATS JetStream, and RabbitMQ adapters publish this
format and accept both CloudEvents and the previous Orbit JSON envelope while consumers migrate.
Orbit envelope metadata uses the string-valued `orbitmetadata` extension; application payload stays
in CloudEvents `data`. Non-UUID foreign CloudEvents IDs map deterministically to Orbit UUIDs, with
their original source and ID retained in reserved envelope metadata. Binary mode, `data_base64`,
and schema negotiation are not implemented.

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

## Documentation

The package-specific guides cover [architecture](docs/architecture/overview.md), [operations and security](docs/operations/README.md), and [development](docs/development/README.md), with [security guidance](docs/security/overview.md). The [documentation index](docs/README.md) links to the full package overview and project policies.

## Development and status

```bash
python -m pip install -e '.[dev]'
pytest
ruff check .
mypy
```

This package is alpha, its API is not stable, and it supports Python 3.11 through 3.14.
Licensed under Apache-2.0.

