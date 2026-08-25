CI smoke test - Actions enablement, 2026-08-25.

Delete with this branch. Not for merge.

Purpose: GitHub Actions was disabled on this fork until 2026-08-25, so
.github/workflows/ci.yml had never run - zero workflow runs existed in the
repo across every workflow. PR #4 merged f64fe144 (+806/-8, including
codex_control_test.go +317 and turn_control_test.go +141) with 0 checks.

This file exists only to match the `backend` path filter (server/**) and
force the first execution of backend-tests against current main, so we learn
whether main is green BEFORE the B1 forward-fix (mc-S2726-P0) bets on this
gate.
