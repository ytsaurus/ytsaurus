---
vcsPath: ydb/docs/ru/core/yql/reference/yql-core/builtins/_includes/window/session_state.md
sourcePath: ydb/docs/ru/core/yql/reference/yql-core/builtins/_includes/window/session_state.md
---
## SessionState() {#session-state}

A non-standard window function `SessionState()` (without arguments) lets you get the session calculation status from [SessionWindow](../../../syntax/group_by.md#session-window) for the current row.
It's allowed only if `SessionWindow()` is present in the `PARTITION BY` section in the window definition.
