# Copyright 2026-present Orbit Contributors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Contract tests for schema validation and delegation to Core transports."""

from collections.abc import Awaitable, Callable
from typing import Any

import pytest
from orbit.events import Event, EventHandler
from pydantic import BaseModel, ConfigDict, RootModel

from orbit_events import (
    EventCatalog,
    EventClient,
    EventDefinition,
    EventSchemaError,
    TypedEvent,
)


class OrderPlaced(BaseModel):
    """Example application-owned event payload."""

    model_config = ConfigDict(strict=True)
    order_id: str
    total_cents: int


ORDER_PLACED = EventDefinition("orders.placed", OrderPlaced, schema_version=2)


class FakeTransport:
    """Capture the Core transport protocol without network or broker dependencies."""

    def __init__(self) -> None:
        self.events: list[Event[Any]] = []
        self.handlers: dict[str, EventHandler] = {}
        self.unsubscribed: list[object] = []

    async def publish(self, event: Event[Any]) -> None:
        self.events.append(event)

    async def subscribe(self, event_name: str, handler: EventHandler) -> object:
        self.handlers[event_name] = handler
        return event_name

    async def unsubscribe(self, token: object) -> None:
        self.unsubscribed.append(token)

    async def close(self) -> None:
        raise AssertionError("EventClient must not own or close the transport.")


def test_definition_and_catalog_reject_ambiguous_or_invalid_entries() -> None:
    catalog = EventCatalog()
    catalog.register(ORDER_PLACED)
    catalog.register(ORDER_PLACED)
    assert catalog.resolve("orders.placed") == ORDER_PLACED
    assert len(catalog) == 1

    with pytest.raises(ValueError, match="lowercase"):
        EventDefinition("Orders.Placed", OrderPlaced)
    with pytest.raises(ValueError, match="positive"):
        EventDefinition("orders.placed", OrderPlaced, schema_version=0)
    with pytest.raises(TypeError, match="integer"):
        EventDefinition("orders.placed", OrderPlaced, schema_version=True)
    with pytest.raises(ValueError, match="already has"):
        catalog.register(EventDefinition("orders.placed", OrderPlaced, schema_version=3))
    with pytest.raises(ValueError, match="max_payload_bytes"):
        EventDefinition("orders.placed", OrderPlaced, max_payload_bytes=True)


@pytest.mark.asyncio
async def test_publish_validates_and_serializes_payload_with_schema_version() -> None:
    transport = FakeTransport()
    client = EventClient(transport)
    event = await client.publish(ORDER_PLACED, {"order_id": "o-1", "total_cents": 250})

    assert event is not transport.events[0]
    assert event.payload == {"order_id": "o-1", "total_cents": 250}
    event.payload["caller_mutation"] = True
    assert "caller_mutation" not in transport.events[0].payload
    assert event.metadata["orbit.events.schema_version"] == 2

    with pytest.raises(EventSchemaError, match="does not match"):
        await client.publish(ORDER_PLACED, {"order_id": "o-2", "total_cents": "secret"})


@pytest.mark.asyncio
async def test_publish_enforces_serialized_size_and_json_object_shape() -> None:
    transport = FakeTransport()
    client = EventClient(transport)
    small_event = EventDefinition("orders.placed", OrderPlaced, max_payload_bytes=8)
    with pytest.raises(EventSchemaError, match="byte limit"):
        await client.publish(small_event, {"order_id": "o-1", "total_cents": 250})

    list_event = EventDefinition("orders.tags", RootModel[list[str]])
    with pytest.raises(EventSchemaError, match="JSON object"):
        await client.publish(list_event, ["private-tag"])
    assert transport.events == []


@pytest.mark.asyncio
async def test_serialization_failures_are_normalized_without_payload_data() -> None:
    class OpaqueValue:
        def __repr__(self) -> str:
            return "private-provider-token"

    class OpaqueEvent(BaseModel):
        model_config = ConfigDict(arbitrary_types_allowed=True)
        value: object

    client = EventClient(FakeTransport())
    definition = EventDefinition("orders.opaque", OpaqueEvent)
    with pytest.raises(EventSchemaError, match="cannot be encoded") as error:
        await client.publish(definition, OpaqueEvent(value=OpaqueValue()))
    assert "private-provider-token" not in str(error.value)


@pytest.mark.asyncio
async def test_custom_validator_failures_are_normalized_without_payload_data() -> None:
    class PrivateValue(BaseModel):
        value: str

        @classmethod
        def model_validate(cls, obj: Any, *args: Any, **kwargs: Any) -> Any:
            raise RuntimeError(f"rejected private value: {obj}")

    client = EventClient(FakeTransport())
    definition = EventDefinition("orders.private", PrivateValue)
    with pytest.raises(EventSchemaError, match="could not be validated") as error:
        await client.publish(definition, {"value": "private-token"})
    assert "private-token" not in str(error.value)


@pytest.mark.asyncio
async def test_subscribe_validates_and_delivers_typed_payload() -> None:
    transport = FakeTransport()
    client = EventClient(transport)
    seen: list[TypedEvent[Any]] = []

    async def handler(event: TypedEvent[Any]) -> None:
        seen.append(event)

    token = await client.subscribe(ORDER_PLACED, handler)
    envelope = Event[dict[str, Any]](
        name="orders.placed",
        payload={"order_id": "o-1", "total_cents": 250},
        metadata={"orbit.events.schema_version": 2},
    )
    await transport.handlers["orders.placed"](envelope)
    assert token == "orders.placed"
    assert seen[0].payload == OrderPlaced(order_id="o-1", total_cents=250)
    assert seen[0].envelope is envelope

    wrong_version = Event[dict[str, Any]](
        name="orders.placed",
        payload={"order_id": "o-1", "total_cents": 250},
        metadata={"orbit.events.schema_version": 1},
    )
    with pytest.raises(EventSchemaError, match="unsupported schema version"):
        await transport.handlers["orders.placed"](wrong_version)


@pytest.mark.asyncio
async def test_unsubscribe_delegates_without_closing_transport() -> None:
    transport = FakeTransport()
    client = EventClient(transport)
    handler: Callable[[TypedEvent[Any]], Awaitable[None]] = _ignore_event
    token = await client.subscribe(ORDER_PLACED, handler)
    await client.unsubscribe(token)
    assert transport.unsubscribed == [token]


async def _ignore_event(event: TypedEvent[Any]) -> None:
    """No-op handler used to test subscription ownership."""
