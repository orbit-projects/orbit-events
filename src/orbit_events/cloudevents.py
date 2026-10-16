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
"""Bounded structured JSON CloudEvents 1.0 encoding for Orbit event envelopes."""

from __future__ import annotations

import json
import re
from datetime import UTC, datetime
from typing import Any, cast
from urllib.parse import urlsplit
from uuid import UUID, uuid5

from orbit.events import Event
from orbit.types import EventId, PluginId, ServiceId

from orbit_events.contracts import EventSchemaError

_NAME = re.compile(r"[a-z][a-z0-9.-]{0,126}\Z")
_NAMESPACE = UUID("d3cc2f50-bce8-5d4e-8dfd-545b480b7f37")
_MAX_WIRE_BYTES = 16 * 1_024 * 1_024
_MAX_EVENT_ID_LENGTH = 255
_METADATA_EXTENSION = "orbitmetadata"
_CE_ID_KEY = "cloudevents.id"
_CE_SOURCE_KEY = "cloudevents.source"


def encode_cloudevent(event: Event[Any], *, max_bytes: int = 1_048_576) -> bytes:
    """Encode an Orbit event as bounded CloudEvents 1.0 structured JSON.

    Orbit envelope metadata is carried in the string-valued ``orbitmetadata`` extension. The
    ``data`` member remains the application's payload. CloudEvents source and ID from an incoming
    external event are retained in reserved Orbit metadata keys when decoded and re-encoded.
    """
    if not isinstance(event, Event):
        raise TypeError("encode_cloudevent requires an Orbit Event.")
    _validate_limit(max_bytes)
    metadata = dict(event.metadata)
    try:
        supplied_id = metadata.pop(_CE_ID_KEY, None)
        supplied_source = metadata.pop(_CE_SOURCE_KEY, None)
        event_id = str(event.id)
        source = _source_for(event)
        if (
            isinstance(supplied_id, str)
            and isinstance(supplied_source, str)
            and _is_uri_reference(supplied_source)
            and _maps_to_identity(supplied_source, supplied_id, event.id)
        ):
            event_id, source = supplied_id, supplied_source
        normalized_event = event.model_dump(mode="json")
        metadata_json = _encode_json(metadata, max_bytes).decode("utf-8")
        value: dict[str, Any] = {
            "specversion": "1.0",
            "id": event_id,
            "source": source,
            "type": event.name,
            "time": event.occurred_at.astimezone(UTC).isoformat().replace("+00:00", "Z"),
            "datacontenttype": "application/json",
            "data": normalized_event["payload"],
            _METADATA_EXTENSION: metadata_json,
        }
        if event.correlation_id is not None:
            value["orbitcorrelationid"] = str(event.correlation_id)
        if event.causation_id is not None:
            value["orbitcausationid"] = str(event.causation_id)
        if event.delivery_key is not None:
            value["orbitdeliverykey"] = event.delivery_key
        value["orbitpriority"] = event.priority
        encoded = _encode_json(value, max_bytes)
    except EventSchemaError:
        raise
    except Exception:
        raise EventSchemaError("Event cannot be encoded as structured CloudEvents JSON.") from None
    return encoded


def decode_cloudevent(payload: bytes, *, max_bytes: int = 1_048_576) -> Event[Any]:
    """Decode one bounded CloudEvents 1.0 structured JSON object into an Orbit envelope.

    Non-UUID CloudEvents IDs are deterministically mapped to Orbit's UUID event identity. The
    original ID and source are preserved in reserved metadata keys. Error messages never include
    input bytes or parser details.
    """
    _validate_limit(max_bytes)
    if not isinstance(payload, bytes) or not 1 <= len(payload) <= max_bytes:
        raise EventSchemaError("CloudEvent payload is empty or exceeds its configured byte limit.")
    try:
        value = json.loads(
            payload, object_pairs_hook=_unique_object, parse_constant=_reject_constant
        )
        if not isinstance(value, dict):
            raise ValueError
        if value.get("specversion") != "1.0":
            raise ValueError
        event_id = value.get("id")
        source = value.get("source")
        name = value.get("type")
        if (
            not isinstance(event_id, str)
            or not 1 <= len(event_id) <= _MAX_EVENT_ID_LENGTH
            or any(ord(char) < 32 or ord(char) == 127 for char in event_id)
            or not isinstance(source, str)
            or not _is_uri_reference(source)
            or not isinstance(name, str)
            or _NAME.fullmatch(name) is None
            or "data" not in value
        ):
            raise ValueError
        occurred_at = _parse_time(value.get("time"))
        raw_metadata = value.get(_METADATA_EXTENSION, "{}")
        if not isinstance(raw_metadata, str):
            raise ValueError
        metadata = json.loads(
            raw_metadata, object_pairs_hook=_unique_object, parse_constant=_reject_constant
        )
        if not isinstance(metadata, dict) or any(not isinstance(key, str) for key in metadata):
            raise ValueError
        metadata[_CE_ID_KEY] = event_id
        metadata[_CE_SOURCE_KEY] = source
        correlation_id = _optional_uuid(value.get("orbitcorrelationid"))
        causation_id = _optional_uuid(value.get("orbitcausationid"))
        delivery_key = value.get("orbitdeliverykey")
        if delivery_key is not None and not isinstance(delivery_key, str):
            raise ValueError
        priority = value.get("orbitpriority", 50)
        if isinstance(priority, bool) or not isinstance(priority, int):
            raise ValueError
        try:
            identity = UUID(event_id)
        except ValueError:
            identity = uuid5(_NAMESPACE, f"{source}\n{event_id}")
        producer = cast(ServiceId | PluginId | None, _producer_from(source))
        return Event[Any](
            id=EventId(identity),
            name=name,
            payload=value["data"],
            producer=producer,
            correlation_id=correlation_id,
            causation_id=causation_id,
            delivery_key=delivery_key,
            priority=priority,
            occurred_at=occurred_at,
            metadata=metadata,
        )
    except Exception:
        raise EventSchemaError("CloudEvent is invalid or unsupported.") from None


def decode_event(payload: bytes, *, max_bytes: int = 1_048_576) -> Event[Any]:
    """Decode CloudEvents structured JSON or a legacy Orbit JSON envelope during migration."""
    _validate_limit(max_bytes)
    if not isinstance(payload, bytes) or not 1 <= len(payload) <= max_bytes:
        raise EventSchemaError("Event payload is empty or exceeds its configured byte limit.")
    try:
        probe = json.loads(payload)
        if isinstance(probe, dict) and probe.get("specversion") == "1.0":
            return decode_cloudevent(payload, max_bytes=max_bytes)
        return Event[Any].model_validate_json(payload)
    except EventSchemaError:
        raise
    except Exception:
        raise EventSchemaError("Event payload is invalid or unsupported.") from None


def _source_for(event: Event[Any]) -> str:
    producer = str(event.producer) if event.producer is not None else "orbit"
    return f"urn:orbit:producer:{producer}"


def _producer_from(source: str) -> UUID | None:
    prefix = "urn:orbit:producer:"
    if source.startswith(prefix):
        try:
            return UUID(source[len(prefix) :])
        except ValueError:
            return None
    return None


def _maps_to_identity(source: str, event_id: str, identity: UUID) -> bool:
    """Allow round-trip identity metadata only when it matches the decoded Orbit ID."""
    try:
        parsed_id = UUID(event_id)
    except ValueError:
        parsed_id = None
    return parsed_id == identity or uuid5(_NAMESPACE, f"{source}\n{event_id}") == identity


def _is_uri_reference(value: str) -> bool:
    if not value or len(value) > 2_048 or any(ord(char) < 33 for char in value):
        return False
    try:
        urlsplit(value)
        return True
    except ValueError:
        return False


def _parse_time(value: object) -> datetime:
    if value is None:
        return datetime.now(UTC)
    if not isinstance(value, str):
        raise ValueError
    parsed = datetime.fromisoformat(value[:-1] + "+00:00" if value.endswith("Z") else value)
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError
    return parsed


def _optional_uuid(value: object) -> UUID | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise ValueError
    return UUID(value)


def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError
        result[key] = value
    return result


def _reject_constant(_value: str) -> None:
    raise ValueError


def _encode_json(value: Any, max_bytes: int) -> bytes:
    """Encode JSON incrementally so output over the configured wire bound is never retained."""
    encoder = json.JSONEncoder(
        ensure_ascii=False, separators=(",", ":"), allow_nan=False
    ).iterencode(value)
    output = bytearray()
    for piece in encoder:
        encoded_piece = piece.encode("utf-8")
        if len(output) + len(encoded_piece) > max_bytes:
            raise EventSchemaError("Encoded CloudEvent exceeds its configured byte limit.")
        output.extend(encoded_piece)
    return bytes(output)


def _validate_limit(max_bytes: int) -> None:
    if (
        isinstance(max_bytes, bool)
        or not isinstance(max_bytes, int)
        or not 1 <= max_bytes <= _MAX_WIRE_BYTES
    ):
        raise ValueError(f"max_bytes must be between 1 and {_MAX_WIRE_BYTES}.")


__all__ = ["decode_cloudevent", "decode_event", "encode_cloudevent"]
