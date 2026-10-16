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
"""CloudEvents codec tests cover interoperability, bounds, and safe failure messages."""

from __future__ import annotations

import json
from uuid import uuid4

import pytest
from orbit.events import Event
from pydantic import BaseModel, field_serializer

from orbit_events import EventSchemaError, decode_cloudevent, decode_event, encode_cloudevent


def test_orbit_event_round_trips_through_structured_cloudevents_json() -> None:
    producer = uuid4()
    correlation_id = uuid4()
    event = Event(
        name="orders.created",
        payload={"order_id": "o-42"},
        producer=producer,
        correlation_id=correlation_id,
        delivery_key="order:o-42",
        metadata={"orbit.events.schema_version": 2},
    )

    encoded = encode_cloudevent(event)
    document = json.loads(encoded)
    decoded = decode_cloudevent(encoded)

    assert document["specversion"] == "1.0"
    assert document["id"] == str(event.id)
    assert document["source"] == f"urn:orbit:producer:{producer}"
    assert document["type"] == "orders.created"
    assert document["datacontenttype"] == "application/json"
    assert document["data"] == {"order_id": "o-42"}
    assert decoded.id == event.id
    assert decoded.producer == producer
    assert decoded.correlation_id == correlation_id
    assert decoded.delivery_key == "order:o-42"
    assert decoded.metadata["orbit.events.schema_version"] == 2


def test_foreign_cloudevent_ids_have_stable_orbit_identity_and_keep_source_identity() -> None:
    payload = json.dumps(
        {
            "specversion": "1.0",
            "id": "vendor-event-123",
            "source": "https://billing.example/events",
            "type": "invoices.paid",
            "data": {"invoice_id": "i-42"},
        }
    ).encode()

    first = decode_event(payload)
    second = decode_event(payload)

    assert first.id == second.id
    assert first.metadata["cloudevents.id"] == "vendor-event-123"
    assert first.metadata["cloudevents.source"] == "https://billing.example/events"
    round_trip = json.loads(encode_cloudevent(first))
    assert round_trip["id"] == "vendor-event-123"
    assert round_trip["source"] == "https://billing.example/events"


@pytest.mark.parametrize(
    "payload",
    [
        b'{"specversion":"0.3","id":"x","source":"urn:x","type":"a","data":{}}',
        b'{"specversion":"1.0","id":"x","source":"urn:x","type":"Bad","data":{}}',
        b'{"specversion":"1.0","id":"x","id":"y","source":"urn:x","type":"a","data":{}}',
        b'{"specversion":"1.0","id":"x","source":"urn:x","type":"a","data":NaN}',
    ],
)
def test_malformed_cloudevents_are_rejected_without_echoing_input(payload: bytes) -> None:
    with pytest.raises(EventSchemaError, match="invalid or unsupported") as error:
        decode_cloudevent(payload)
    assert payload.decode(errors="ignore") not in str(error.value)


def test_codec_enforces_wire_bounds_and_preserves_legacy_event_reads() -> None:
    event = Event(name="orders.created", payload={"value": "too large"})
    with pytest.raises(EventSchemaError, match="byte limit"):
        encode_cloudevent(event, max_bytes=32)
    with pytest.raises(EventSchemaError, match="byte limit"):
        decode_event(encode_cloudevent(event), max_bytes=32)
    legacy = event.model_dump_json().encode()
    assert decode_event(legacy).id == event.id


def test_codec_redacts_custom_serializer_failures() -> None:
    class SensitivePayload(BaseModel):
        credential: str

        @field_serializer("credential")
        def fail_serialization(self, _value: str) -> str:
            raise RuntimeError("must-not-escape")

    event = Event(name="secrets.loaded", payload=SensitivePayload(credential="secret-value"))
    with pytest.raises(EventSchemaError) as error:
        encode_cloudevent(event)
    assert "must-not-escape" not in str(error.value)
    assert "secret-value" not in str(error.value)
