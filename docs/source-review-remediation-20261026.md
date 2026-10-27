Source-review R20–R25 remediation
================================

Agent authority comes from authenticated task-token context. Raw agent/task
headers carry no authority. Verification-code consumption and failed attempts
serialize in short PostgreSQL transactions; provider HTTP calls have finite
deadlines, response caps and no redirect forwarding.

With APP_ENV=production, missing SMTP/Resend configuration now makes email
delivery fail without printing verification codes, recipients or invitation
links. The explicit non-production stdout development fallback remains. This
does not make an email backend mandatory for unrelated server/SSO features or
configure a real provider. Existing production environment selection and actual
mail delivery still require runtime qualification.

SMTP and Resend failures returned to logging callers also discard raw provider
diagnostics: a provider can reflect the submitted login code or invitation link
in its error response. Delivery still fails without retry or a success claim.

HTTP headers, request bodies and idle connections are bounded while intentional
SSE and WebSocket traffic retain their write lifetime. Auth throttles retain a
bounded per-source local guard when Redis is missing or fails. Readiness reports
that degraded per-process mode; its aggregate budget depends on replica count.
Exact source keys keep unrelated sources from sharing counters. Each limiter
retains at most 65,536 live windows, never evicts an active budget and reclaims
expired entries at most once per second. At capacity, previously unseen sources
receive HTTP429 until space expires; existing sources retain their own remaining
allowance. Distributed source pressure can therefore deny new arrivals. This is
an explicit finite-capacity limitation, not unlimited source isolation.

The selected R24 policy favors bounded auth availability during Redis outages.
`/readyz` remains HTTP200 when database/schema checks pass, with status=degraded
and checks.auth_rate_limit=local_per_instance. It does not advertise a healthy
distributed quota. With N live processes, a source can receive at most N times
each configured local-window allowance (default auth5/minute, verification20/
minute, contact-sales5/hour); local windows/restarts are independent. Redis is
needed for a durable fleet-wide quota. Deployments must bound replica count and
monitor this degraded mode. This choice implements the originally authorized
finite local fallback, rather than making a Redis outage a global auth outage.

`RATE_LIMIT_TRUSTED_PROXIES` controls forwarded client IPs independently of
other proxy configuration. With no matching trusted proxy, forwarded headers
are ignored and all users behind the same reverse proxy share its direct-peer
allowance (default login-code sends 5/minute per process). This also applies
without Redis; startup warns about that shared-peer risk. Configure only the
actual proxy CIDRs, or use direct client connections. The server never trusts
arbitrary forwarded IPs to recover availability.

Skill create/update and the exact daemon local-skill import-result route retain
the existing 8MiB supporting-file plus 1MiB SKILL.md contract. Their finite 55MiB
wire cap accommodates worst-case JSON escaping and metadata; other ordinary
routes remain 2MiB, auth 64KiB and webhooks 10MiB. Handler-level validation and
multipart limits are retained.
The exact single-file PUT route separately allows a 1MiB file with worst-case
JSON escaping under a finite 7MiB wire cap.

Claude and Codex Result metadata reports TranscriptDeliveryChecked and
DroppedMessages independently of final output. Slow consumers can lose bounded
transcript messages; a final answer does not imply complete transcript delivery.
Codex relays preserve session pins and fresh-session retries preserve known loss
counts even on errors. After draining, the daemon stores a renderable error
message with complete=false and the loss count through the existing task-message
endpoint before terminal handoff. Failure to store that marker prevents terminal
success. A checked zero-drop cancellation emits no loss marker. An interrupted
drain with no confirmed drops reports unknown completeness separately; failure
to persist that optional cancellation marker preserves the cancellation outcome.
Completed tasks with confirmed loss still fail closed if their marker cannot be
stored. This adds no event spool or claim that omitted events can be rebuilt.

Focused acceptance used disposable PostgreSQL, local HTTP servers and fake
executables. No real provider, account credential or production DB was used.
Windows-wide agent suites include Unix shell fixtures that cannot run here;
Linux CI and independent cross-family review remain merge gates. This branch
does not publish a CLI release tag, deploy, or resume any automation.
