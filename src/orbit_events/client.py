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
    Payload validation and encoding errors are raised as ``EventSchemaError``. Transport failures
    and handler failures propagate according to the underlying transport and broker adapter; this
    facade does not promise persistence, retry, ordering, acknowledgment, or exactly-once delivery.
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
        """Validate, encode, and publish one versioned event; return its Core envelope.

        The event name and schema version come from ``definition``. The encoded payload is bounded
        JSON object data, and the transport receives a deep copy so later caller mutation of the
        returned envelope cannot mutate the object submitted to the transport. This method returns
        when ``EventTransport.publish`` returns; durable broker acceptance depends on that
        transport's contract.
        """
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
        """Register a typed handler and return the transport's opaque subscription token.

        Every delivery is checked for the expected event name and exact schema version, then its
        payload is validated before the application handler runs. Validation failures raise a
        sanitized ``EventSchemaError`` through the transport callback; adapter retry, rejection,
        and dead-letter policy remain broker-specific. Handler exceptions and cancellation
        propagate unchanged. The token belongs to the transport and should be passed back to this
        client's ``unsubscribe`` method.
        """

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
        """Remove a subscription using the opaque token issued by this client's transport.

        Unsubscription is delegated directly; ordering with in-flight deliveries and behavior for
        stale tokens are defined by the selected transport.
        """
        await self._transport.unsubscribe(token)


__all__ = ["EventClient", "TypedEventHandler"]
