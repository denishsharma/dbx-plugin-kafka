# Agent workflow

This repository is the DBX Kafka plugin. Follow `.github/agent-flow.yml` as the machine-readable task contract.

## Handoff rules

- Use one branch/worktree per agent; never edit a shared checkout concurrently.
- Read the contract first and stay within `allowed_paths`.
- Do not alter versions, release metadata, generated output, or shared lockfiles unless the integrator owns that change.
- Use only ephemeral CI/container credentials. Never access production brokers or real secrets.
- Run the local validation commands before handoff and record results, changed files, risks, and follow-up work in the PR.
- Agents do not merge their own PRs.

## Integration rules

The integrator owns shared contract files, version bumps, generated `ui/`, package artifacts, and final cross-target packaging checks.

