## Agent handoff

- Task ID:
- Stage: `contract` / `implementation` / `review` / `integration`
- Base SHA:
- Allowed paths respected: yes / no
- Depends on PR:

## Verification

- [ ] repository and connection-form validation
- [ ] frontend typecheck/test/build
- [ ] `cd backend && gofmt -l main.go internal`
- [ ] `cd backend && go vet ./... && go test ./...`
- [ ] Kafka container smoke (if protocol/backend behavior changed)

## Review notes

- Changed files:
- Risks or known limitations:
- Follow-up task:
- Only ephemeral credentials used: yes

