# Best practices

- Each `DROP` call adds a step to the plan, so it is better to avoid multiple column drops. A single `SELECT` with the required set of columns is more efficient.
