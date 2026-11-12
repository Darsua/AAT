# Problem 3: Authentication and authorization — historical M1 evidence

**Date context: 2026-10-08**, based on payload timestamps in the auth log. The logs do not record a complete execution start time; a separate date for the cross-credential test was not recorded. This evidence is not current validation or an M2 guarantee. No tests were rerun during this documentation cleanup.

## Recorded execution and results

- [`cross_credential.log`](cross_credential.log), from [`test_cross_credential.sh`](../../../../scripts/test_cross_credential.sh): credentials were accepted in their own domain (HTTP 200); BMKG credentials sent to PVMBG, PVMBG credentials sent to BMKG, and unauthenticated access were rejected (HTTP 401).
- [`auth_scopes.log`](auth_scopes.log), from [`test_auth_scopes.sh`](../../../../scripts/test_auth_scopes.sh): **15 passed, 0 failed**. An incorrect client secret was rejected (401); the media token had `hazards:read:summary`, received the summary projection (200), and was rejected for `fields=all` (403). Field/internal tokens had `hazards:read:all` and successfully accessed full data (200).
- The auth log records a 60-second access-token TTL, successful refresh with a new token, a successful query using the new token (200), rejection of reuse of the old refresh token (401), and revocation of the remaining refresh session after replay (401).

## Evidence limitations

Refresh/revocation state is process-local; these tests do not establish persistence across restarts or consistency across replicas. Revoking refresh access does not invalidate previously issued access JWTs before they expire. See [ADR 005](../../../adr/005-stateless-architecture-and-m2-readiness.md) for state-management context and M1 limitations.
