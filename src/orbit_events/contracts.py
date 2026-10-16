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
"""Typed event definitions layered over Orbit Core's transport contract."""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass
from json import JSONDecoder, JSONEncoder
from typing import Any, Generic, TypeVar

from orbit.events import Event
from pydantic import BaseModel, ValidationError

PayloadT = TypeVar("PayloadT", bound=BaseModel)
_EVENT_NAME = re.compile(r"[a-z][a-z0-9.-]{0,126}\Z")
_DEFAULT_MAX_PAYLOAD_BYTES = 1_048_576
_MAX_PAYLOAD_BYTES = 16 * 1_024 * 1_024


@dataclass(frozen=True, slots=True)
class EventDefinition(Generic[PayloadT]):
    """Declare one event name, payload model, and exact schema version.

    Names use the Core lowercase event-name format. A definition accepts payloads only when they
    validate as its Pydantic model and serialize to a bounded JSON object. Consumers require the
    exact version declared here; this package does not perform schema migration or compatibility
    negotiation between versions.
    """

    name: str
    payload_type: type[PayloadT]
    schema_version: int = 1
    max_payload_bytes: int = _DEFAULT_MAX_PAYLOAD_BYTES

    def __post_init__(self) -> None:
        """Reject definitions that Core cannot represent or version safely."""
        if not isinstance(self.name, str) or _EVENT_NAME.fullmatch(self.name) is None:
            raise ValueError("Event names must match Orbit Core's lowercase event-name contract.")
        if not isinstance(self.payload_type, type) or not issubclass(self.payload_type, BaseModel):
            raise TypeError("Event payload_type must be a Pydantic BaseModel subclass.")
        if isinstance(self.schema_version, bool) or not isinstance(self.schema_version, int):
            raise TypeError("Event schema_version must be an integer.")
        if self.schema_version < 1:
            raise ValueError("Event schema_version must be positive.")
        if (
            isinstance(self.max_payload_bytes, bool)
            or not isinstance(self.max_payload_bytes, int)
            or not 1 <= self.max_payload_bytes <= _MAX_PAYLOAD_BYTES
        ):
            raise ValueError(f"Event max_payload_bytes must be between 1 and {_MAX_PAYLOAD_BYTES}.")


@dataclass(frozen=True, slots=True)
class TypedEvent(Generic[PayloadT]):
    """Pair a validated application payload with the Core envelope that carried it.

    The payload is validated for this definition before a handler is invoked. The envelope remains
    the transport-provided value and may be mutable; handlers should treat it as read-only and
    must not use it to alter broker acknowledgment or retry behavior.
    """

    definition: EventDefinition[PayloadT]
    payload: PayloadT
    envelope: Event[Any]


class EventSchemaError(ValueError):
    """Raised when an event name, version, payload, or JSON encoding violates its definition.

    Messages intentionally omit payload values and Pydantic's potentially sensitive error details.
    Broker adapters decide whether this handler exception is retried, rejected, or dead-lettered.
    """


class EventCatalog:
    """Application-owned registry of explicit event definitions.

    Registration is idempotent for an equal definition and rejects a second, different definition
    under the same name. The catalog is local configuration; it is not a schema registry or a
    network service and does not negotiate producer/consumer compatibility.
    """

    def __init__(self) -> None:
        """Create an empty registry; definitions are application-owned values."""
        self._definitions: dict[str, EventDefinition[Any]] = {}

    def register(self, definition: EventDefinition[Any]) -> None:
        """Register a definition, allowing exact repeats but rejecting conflicting names."""
        existing = self._definitions.get(definition.name)
        if existing is not None and existing != definition:
            raise ValueError(f"Event name {definition.name!r} already has a different definition.")
        self._definitions[definition.name] = definition

    def resolve(self, name: str) -> EventDefinition[Any]:
        """Return the local definition for ``name`` or raise ``KeyError`` if it is unknown."""
        return self._definitions[name]

    def __len__(self) -> int:
        """Return the number of distinct event names registered."""
        return len(self._definitions)


def validate_payload(
    definition: EventDefinition[PayloadT], payload: PayloadT | Mapping[str, Any]
) -> PayloadT:
    """Validate caller or transport data and enforce bounded JSON-object wire semantics."""
    return _validate_and_encode(definition, payload)[0]


def _validate_and_encode(
    definition: EventDefinition[PayloadT], payload: PayloadT | Mapping[str, Any]
) -> tuple[PayloadT, dict[str, Any]]:
    """Validate a payload and bound its actual JSON representation before transport use."""
    try:
        validated = definition.payload_type.model_validate(payload)
    except ValidationError:
        # Pydantic errors may include rejected values; keep this boundary safe for logs.
        raise EventSchemaError(f"Payload does not match event {definition.name!r}.") from None
    except Exception:
        # User-defined validators may raise non-Pydantic exceptions containing payload values.
        raise EventSchemaError(
            f"Payload for event {definition.name!r} could not be validated."
        ) from None
    try:
        wire_value = validated.model_dump(mode="json")
        if not isinstance(wire_value, dict):
            raise EventSchemaError("Event payload must serialize to a JSON object.")
        encoder = JSONEncoder(ensure_ascii=False, separators=(",", ":"), allow_nan=False)
        pieces: list[str] = []
        encoded_size = 0
        for piece in encoder.iterencode(wire_value):
            encoded_size += len(piece.encode("utf-8"))
            if encoded_size > definition.max_payload_bytes:
                raise EventSchemaError(f"Event {definition.name!r} payload exceeds its byte limit.")
            pieces.append(piece)
        serialized = "".join(pieces)
        normalized = JSONDecoder().decode(serialized)
        if not isinstance(normalized, dict):
            raise EventSchemaError("Event payload must serialize to a JSON object.")
        return validated, normalized
    except EventSchemaError:
        raise
    except Exception:
        # Custom serializers can raise arbitrary exceptions or include rejected values in them.
        raise EventSchemaError(
            f"Payload for event {definition.name!r} cannot be encoded as bounded JSON."
        ) from None


__all__ = ["EventCatalog", "EventDefinition", "EventSchemaError", "TypedEvent", "validate_payload"]
