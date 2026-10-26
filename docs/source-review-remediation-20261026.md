Source-review R20–R25 remediation
================================

Agent authority comes from authenticated task-token context. Raw agent/task
headers carry no authority. Verification-code consumption and failed attempts
serialize in short PostgreSQL transactions; provider HTTP calls have finite
deadlines, response caps and no redirect forwarding.

HTTP headers, request bodies and idle connections are bounded while intentional
SSE and WebSocket traffic retain their write lifetime. Auth throttles retain a
bounded per-source local guard when Redis is missing or fails. Readiness reports
that degraded per-process mode; its aggregate budget depends on replica count.
Fixed secret-hash slots conservatively share a limit on collisions.

The selected R24 policy favors bounded auth availability during Redis outages.
`/readyz` remains HTTP200 when database/schema checks pass, with status=degraded
and checks.auth_rate_limit=local_per_instance. It does not advertise a healthy
distributed quota. With N live processes, a source can receive at most N times
each configured local-window allowance (default auth5/minute, verification20/
minute, contact-sales5/hour); local windows/restarts are independent. Redis is
needed for a durable fleet-wide quota. Deployments must bound replica count and
monitor this degraded mode. This choice implements the originally authorized
finite local fallback, rather than making a Redis outage a global auth outage.

Claude and Codex Result metadata reports TranscriptDeliveryChecked and
DroppedMessages independently of final output. Slow consumers can lose bounded
transcript messages; a final answer does not imply complete transcript delivery.
Codex relays preserve session pins and fresh-session retries preserve known loss
counts even on errors. After draining, the daemon stores a renderable error
message with complete=false and the loss count through the existing task-message
endpoint before terminal handoff. Failure to store that marker prevents terminal
success. This adds no event spool or claim that omitted events can be rebuilt.

Focused acceptance used disposable PostgreSQL, local HTTP servers and fake
executables. No real provider, account credential or production DB was used.
Windows-wide agent suites include Unix shell fixtures that cannot run here;
Linux CI and independent cross-family review remain merge gates. This branch
does not publish a CLI release tag, deploy, or resume any automation.
