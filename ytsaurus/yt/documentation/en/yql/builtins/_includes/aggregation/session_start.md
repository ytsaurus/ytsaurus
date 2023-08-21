
## SessionStart {#session-start}

Without arguments. Only allowed if there is [SessionWindow](../../../syntax/group_by.md#session-window) in
[GROUP BY](../../../syntax/group_by.md)/[PARTITION BY](../../../syntax/window.md#partition).
Returns the value of the `SessionWindow` key column. In case of `SessionWindow` with two arguments — the minimum value of the first argument within a group/partition.
In case of the extended variant of `SessionWindoow` — the value of the second tuple item returned by `<calculate_lambda>` when the first tuple item is `True`.
