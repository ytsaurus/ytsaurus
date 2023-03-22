
* The resulting table includes all columns that were found in at least one of the input tables.
* if a column wasn't present in any input table, it's automatically assigned [optional data type](../../../types/optional.md) (that permits `NULL` value);
* If a column in different input tables had different types, then the shared type (the broadest one) is output.
* If a column in different input tables had a heterogeneous type, for example, string and numeric, an error is raised.
