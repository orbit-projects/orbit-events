# Orbit Events: architecture and contracts

## Package boundaries

`orbit-events` adds typed payload models and schema versions over the `EventTransport` contract in
`orbit-core`. Core remains responsible for its event envelope, in-process bus, event store,
transport protocol, and application lifecycle. Separately installable adapters such as
`orbit-events-kafka`, `orbit-events-rabbitmq`, and `orbit-events-nats` provide broker-specific
transport behavior and own their SDK configuration and resources.

```text
application modules ── EventClient / EventDefinition ── orbit-events
                                      │
                                      ▼
                              Core EventTransport
                                      ▲
                         broker adapter plugin
                                      │
                             broker SDK / service
```

The client does not create or close the transport. The Core plugin or application resource that
registers the adapter owns its shutdown. The event capability validates typed payloads but does
not become a second bus or broker abstraction.

## Definitions and wire envelope

Broker adapters use CloudEvents 1.0 structured JSON on publication and accept both that format and
the legacy Orbit JSON envelope on consumption. `data` contains the application payload, while
Orbit's envelope metadata is carried in the `orbitmetadata` string extension. The codec rejects
duplicate JSON keys, non-finite numbers, invalid event names, unsupported specification versions,
and messages over the configured byte bound. It does not implement CloudEvents binary mode or
`data_base64`.

An `EventDefinition` binds a Core-compatible event name to a Pydantic model, a positive integer
schema version, and a per-definition byte limit. Publication validates the model, serializes it in
JSON mode, requires a JSON object at the wire root, and stores the version under
`orbit.events.schema_version` in Core envelope metadata. The serialized UTF-8 JSON representation
is bounded to 1 MiB by default and at most 16 MiB.

Consumption requires the exact declared schema version and validates the payload before invoking
the typed handler. `EventSchemaError` messages omit rejected payload values and Pydantic details.
`EventCatalog` is an optional in-process registry that prevents conflicting definitions in one
application; it is not a shared schema registry or compatibility service.

The transport gets a detached deep copy of the published envelope. `TypedEvent` contains the
validated payload and transport-provided envelope. Treat the delivered envelope as read-only; it
may be mutable and is not an acknowledgment or broker control handle.

## Schema evolution and deployment

Schema versions are exact-match identifiers, not an automatic migration system. A consumer for
version 1 rejects version 2 until its definition and deployment change. For incompatible changes,
introduce an explicit new version and coordinate consumers and producers using the selected
adapter's routing and rollout behavior. Additive changes may also break strict Pydantic models or
custom validators; test the actual old/new payload matrix before deploying.

This package does not include a schema registry, compatibility checker, replay tool, outbox,
exactly-once guarantee, retry policy, or dead-letter queue. Durable acceptance, redelivery,
ordering, partitioning, acknowledgments, and handler concurrency are defined by the selected
transport and broker adapter.

## Payload size and data placement

The encoded payload is bounded after Pydantic validation and serialization. This protects transport
handoff size but does not avoid the initial memory cost of the caller's model or serializer output.
Avoid large binary data and high-volume blobs in events. Store those separately and publish a
reference with appropriate authorization and retention. Event payloads should contain only the
data needed by consumers.

## Errors and ownership

Validation and serialization failures raise sanitized `EventSchemaError`. Transport publication
errors, handler errors, and cancellation propagate through the underlying transport; their retry
and acknowledgment outcome is provider-specific. The `EventClient` owns no SDK clients or
background tasks and exposes `unsubscribe(token)` to delegate removal to the transport that issued
the opaque token.

## Source layout and status

- `src/orbit_events/contracts.py`: event definitions, catalog, typed event, and bounded payload
  validation.
- `src/orbit_events/client.py`: typed publish/subscribe delegation.
- `src/orbit_events/__init__.py`: public package exports.

The package declares Python `>=3.11,<3.15`, Pydantic 2.9 or newer within major version 2, and
`orbit-core>=0.1.0a1,<0.2`. It is alpha; see `pyproject.toml` for authoritative metadata.
