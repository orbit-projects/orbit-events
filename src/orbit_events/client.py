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
"""Application-facing typed client that delegates delivery to a Core transport."""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping
from typing import Any, TypeVar
from uuid import UUID

from orbit.events import Event, EventTransport
from orbit.types import PluginId, ServiceId
from pydantic import BaseModel

from orbit_events.contracts import (
    EventDefinition,
    EventSchemaError,
    TypedEvent,
    _validate_and_encode,
)

_SCHEMA_VERSION_KEY = "orbit.events.schema_version"
PayloadT = TypeVar("PayloadT", bound=BaseModel)
TypedEventHandler = Callable[[TypedEvent[PayloadT]], Awaitable[None]]


class EventClient:
    """Publish and consume validated schemas over any Core ``EventTransport``.

    The client does not own or close the transport. Broker adapters and the Core lifecycle retain
    their existing resource ownership, while applications may use this facade with any adapter.
    """

    def __init__(self, transport: EventTransport) -> None:
        """Bind an already configured Core event transport without taking ownership of it."""
        self._transport = transport

    async def publish(
        self,
        definition: EventDefinition[PayloadT],
        payload: PayloadT | Mapping[str, Any],
        *,
        producer: ServiceId | PluginId | None = None,
        correlation_id: UUID | None = None,
        causation_id: UUID | None = None,
        delivery_key: str | None = None,
    ) -> Event[dict[str, Any]]:
        """Validate, encode, and publish one versioned event; return its Core envelope."""
        validated, wire_payload = _validate_and_encode(definition, payload)
        envelope = Event[dict[str, Any]](
            name=definition.name,
            payload=wire_payload,
            producer=producer,
            correlation_id=correlation_id,
            causation_id=causation_id,
            delivery_key=delivery_key,
            metadata={_SCHEMA_VERSION_KEY: definition.schema_version},
        )
        # The returned envelope remains caller-owned; keep transport mutation isolated.
        await self._transport.publish(envelope.model_copy(deep=True))
        return envelope

    async def subscribe(
        self,
        definition: EventDefinition[PayloadT],
        handler: TypedEventHandler[PayloadT],
    ) -> object:
        """Register a typed handler, validating event name, version, and payload on delivery."""

        async def dispatch(envelope: Event[Any]) -> None:
            if envelope.name != definition.name:
                raise EventSchemaError("Transport delivered an event with an unexpected name.")
            version = envelope.metadata.get(_SCHEMA_VERSION_KEY)
            if type(version) is not int or version != definition.schema_version:
                raise EventSchemaError(
                    f"Event {definition.name!r} has an unsupported schema version."
                )
            payload, _ = _validate_and_encode(definition, envelope.payload)
            await handler(TypedEvent(definition=definition, payload=payload, envelope=envelope))

        return await self._transport.subscribe(definition.name, dispatch)

    async def unsubscribe(self, token: object) -> None:
        """Delegate subscription removal to the transport that issued ``token``."""
        await self._transport.unsubscribe(token)


__all__ = ["EventClient", "TypedEventHandler"]
