# Orbit Events: security

`orbit-events` validates typed payload shape and serialized size. It does not encrypt events,
authorize publishers or subscribers, enforce tenant isolation, or decide how long a broker retains
data. Those controls belong to the application, adapter, broker, and deployment.

## Payloads and sensitive data

Treat event payloads as data that may be copied to every configured consumer, broker replica,
retry queue, dead-letter queue, trace, and log. Include only fields consumers need. Avoid
credentials, access tokens, payment secrets, and unnecessary personal or regulated data. Prefer
references to separately protected object storage for large or sensitive content, with independent
authorization and expiry controls.

Pydantic validation checks payload structure; it is not authorization or content safety. Enforce
permission to publish and subscribe at the application and broker boundaries. Validate that a
consumer is authorized for each tenant or resource represented by the event.

## Error and logging boundaries

`EventSchemaError` omits payload values and Pydantic error details. Transport and broker errors are
outside this package's sanitization boundary. Configure adapter logging to avoid connection
strings, credentials, full payloads, and raw provider responses. Log stable event names, versions,
and correlation identifiers only where those values are acceptable under the application's data
policy.

## Broker access

Use separate least-privilege producer and consumer identities when the broker supports them. Limit
topic or subject permissions, enforce TLS with certificate verification, protect credentials in a
secret manager, and review retention, replication, encryption, and dead-letter access. Broker
policies should prevent unrelated services from publishing or consuming sensitive event streams.

## Vulnerability reporting

Follow the repository [security policy](../../SECURITY.md) for private vulnerability reporting.
This guide is not a broker security assessment or compliance certification.
