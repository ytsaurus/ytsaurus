
## EvaluateExpr, EvaluateAtom {#evaluate_expr_atom}

Evaluate an expression before the start of the main calculation and input its result to the query as a literal (constant). In many contexts where the standard SQL would only expect a constant (for example in table names, number of rows in [LIMIT](../../../syntax/select.md#limit), and so on), this functionality is automatically activated implicitly.

EvaluateExpr can be used where the grammar already expects an expression. For example, you can use it to:

* Round the current time to days, weeks, or months and add the time to the query. This will ensure [valid query caching](../../../syntax/pragma.md#yt.querycachemode), although normally, when you use [current-time functions](#currentutcdate), caching is disabled completely.
* Run a heavy calculation with a small result once per query instead of once per job.

Using EvaluateAtom, you can create an [atom](../../../types/special.md) dynamically, but since atoms are mainly controlled from a lower [s-expressions](/docs/s_expressions/functions) level, generally, it's not recommended to use this function directly.

The only argument for both functions is the expression for calculation and substitution.

Restrictions:

* The expression must not trigger MapReduce operations.
* This functionality is fully locked in YQL over YDB.

**Examples:**
```yql
$now = CurrentUtcDate();
SELECT EvaluateExpr(
    DateTime::MakeDate(DateTime::StartOfWeek($now)
    )
);
```
