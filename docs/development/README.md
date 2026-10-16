# Orbit Events: development

This package owns typed event definitions on top of Orbit Core's transport contract. Broker
protocols and SDKs belong in separately installable adapters. Changes to the event envelope or
delivery contract need coordinated review in Core and every broker adapter.

## Local setup and checks

From the repository root:

```bash
python -m pip install -e '.[dev]' build
pytest
ruff check src tests
ruff format --check src tests
mypy
python -m build
```

The unit tests use a fake Core transport and no broker credentials. They cover payload validation,
JSON size bounds, schema-version checks, transport delegation, and error-message safety. They do
not validate broker retries, acknowledgments, delivery guarantees, schema rollout, or hosted CI.

## Changing an event contract

- Treat event names and schema versions as public integration APIs. Do not silently change the
  meaning of an existing version.
- Keep serialization deterministic enough for the documented size bound, and retain the root JSON
  object requirement unless Core's event envelope contract changes.
- Avoid exceptions that include rejected model values, payloads, or custom serializer details.
- Add regression tests for invalid payloads, custom serializers/validators, version mismatches,
  payload limits, cancellation, and transport ownership as relevant.
- Update examples and all package guides, and coordinate consumer rollout guidance with the
  selected broker adapters.

Follow the contribution guide, governance, code of conduct, and security policy. Keep test payloads
synthetic and report local validation separately from live broker or hosted CI evidence.
