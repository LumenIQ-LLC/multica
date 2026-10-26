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

Claude and Codex Result metadata reports TranscriptDeliveryChecked and
DroppedMessages independently of final output. Slow consumers can lose bounded
transcript messages; a final answer does not imply complete transcript delivery.
Codex relays and fresh-session retries preserve the loss count. The daemon emits
an explicit incomplete-transcript warning. This adds no durable event spool or
claim that omitted events can be reconstructed.

Focused acceptance used disposable PostgreSQL, local HTTP servers and fake
executables. No real provider, account credential or production DB was used.
Windows-wide agent suites include Unix shell fixtures that cannot run here;
Linux CI and independent cross-family review remain merge gates. This branch
does not publish a CLI release tag, deploy, or resume any automation.
