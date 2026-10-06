# Orbit Events: architecture and boundaries

## Responsibility

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

## Declared dependencies

The following dependency declarations come from the checked-in manifests. Optional groups and development dependencies are called out separately.

### `pyproject.toml`
- `pydantic>=2.9,<3`
- `orbit-core>=0.1.0a1,<0.2`
- Optional `dev` group: `pytest>=8,<10`, `pytest-asyncio>=0.24,<2`, `ruff>=0.8,<1`, `mypy>=1.13,<2`.

Declared dependencies do not mean that optional providers or services are bundled with this package.

## Implementation layout

Representative implementation files in this checkout:

- `src/orbit_events/__init__.py`
- `src/orbit_events/client.py`
- `src/orbit_events/contracts.py`

## Public contract and scope

The README does not contain a separately headed architecture section. Its responsibility statement above and the public source files define the implemented scope; this guide adds no behavior beyond that description.

## Boundary rules

Keep provider SDKs, credentials, transports, and provider-specific error translation in provider adapters. Keep reusable capability contracts in the matching capability package and lifecycle orchestration in Core. Apply the relevant layer for this repository and preserve the dependency direction shown above.
