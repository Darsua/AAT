## Summary of Changes

<!-- Provide a concise description of what this PR introduces, fixes, or integrates. -->

---

### Workstream & Ownership (RACI Boundary)
- [ ] **Member A (Aldoy):** `services/mock-bmkg/`, `services/mock-pvmbg/`
- [ ] **Member B (Darrel):** `services/aggregator/`, database migrations, outbox producer
- [ ] **Member C (Jenka):** `services/auth/`, `services/client-api/`, `services/consumers/`
- [ ] **Cross-cutting / All:** Root orchestration, documentation, scripts

> **Rule:** Ensure this PR only modifies files within your assigned service scope. Cross-service library sharing is strictly prohibited per course spec.

---

### Relevant Milestone 1 Problem
- [ ] **Problem 1 (P1):** Live Schema Evolution & dynamic attributes
- [ ] **Problem 2 (P2):** Concurrency, Availability & Graceful Degradation
- [ ] **Problem 3 (P3):** RBAC Token Scopes & Cross-Credential Trust Isolation
- [ ] **Problem 4 (P4):** Independent Service Rebuilds & Flexible Canonical Storage
- [ ] **Problem 5 (P5):** Pub-Sub Event Fan-Out & Consumer Downtime Recovery
- [ ] **Non-problem / Infrastructure / Documentation**

---

### Verification Checklist
- [ ] Code compiles cleanly locally (`go build ./...` or `make test`).
- [ ] Docker container builds without errors (`docker build ./services/<service-name>`).
- [ ] Frozen data contracts in `contracts/` are respected and not arbitrarily modified.
- [ ] No `.env` files or credentials are committed.
- [ ] Local tests or demo runbook scripts pass.

### Evidence / Test Output
<!-- Paste terminal outputs, log snippets, or test results demonstrating that the changes work. -->
```bash

```
