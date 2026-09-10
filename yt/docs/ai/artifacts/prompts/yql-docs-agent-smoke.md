# YQL documentation agent smoke check

This is a read-only smoke test for the YQL documentation synchronization agent.

Read `a.yaml` in the current working directory and confirm that it defines both
the `yql-docs-yfm-2service-doc` weekly action and the
`yql-docs-agent-smoke` action.

Do not modify files, run commands that change state, call external services,
post comments, create commits, or create pull requests.

Return exactly one JSON object and no other text:

```json
{"status":"ok","check":"yql-docs-agent-smoke"}
```
