# Managing row-level access to tables

This section describes how Row-Level Security (RLS) works and provides examples of its use.

## Brief description { #brief_desc }

Row-Level Security (RLS) enables you to specify access rules for individual table rows based on predicates evaluated against row data. This enables you to store both public and private data in the same table and define access through arbitrary expressions in the {{product-name}} [Query language](../../../user-guide/dynamic-tables/dyn-query-language.md).

## Operation principle { #details }

The general operating principles of the access control system in {{product-name}} are described in the [Access control](../../../user-guide/storage/access-control.md) section.

To set up RLS, use ACEs of a special type where the additional `row_access_predicate` attribute is specified in the ACLs of nodes. Such ACEs do not take part in ordinary checks of permissions for objects in any way. These ACEs are taken into account at the stage of checking access to individual table rows.

The `row_access_predicate` expression must evaluate to a boolean value. The expression can reference any columns from the table schema and use standard operators and functions.

Suppose that the system is checking read access permissions for table `T` on behalf of user `U`. Such reads occur when the `read-table` command is executed or when table `T` is used as input to in operations.

In addition to having a `read` permission for the entire table, the user must also satisfy at least one row access predicate for each row they want to read.

The latter property is checked as follows: we build an effective ACL of table `T`. From the ACEs that make it up, select those that are row-level (contain `row_access_predicate` attribute) and applicable to user `U` (`U` is either explicitly mentioned in `subjects` or belongs to one of the groups mentioned in `subjects`). Call the result `L`.

If `L` is empty, this means that no row-level allowances apply to table `T` for user `U`, and the user will not be able to read any rows (unless they have `full_read` permission).

Otherwise, if at least one suitable ACE is found, for each row in the table, at least one predicate from `L` must evaluate to `true` for the user to be able to read that row.

## Compatibility mode { #compatibility_mode }

If you set a row-level ACL on a table, users who do not have the `full_read` permission will receive an authorization error when attempting to read the table.

To ensure that users explicitly understand they may not receive all data, they must specify the `omit_inaccessible_rows` flag (the default value is `%false`) for read operations. A setting with the same name is also available in the operation specification.

When this option is enabled, if the user does not have access to a table row (if there is read access to the table as a whole), the system hides that row completely from the user and the requested action is completed successfully.

## Limitations and special features

RLS is not supported for dynamic tables. Any read attempt will return an error.

Due to the dynamic nature of access rules, the system cannot always perform certain actions efficiently. For example, the amount of data read from disk is not always proportional to the number of accessible rows. But the dynamic nature affects not only speed, but also some APIs:

1. `row_index` specified in `ranges` in the `read-table` command counts indices relative to rows written to disk, not rows accessible to the user. For example, if you request the first 100 rows (`read-table //path/to/table[:#100]`) while not having access to any of these rows, an empty result will be returned.
2. Similarly, in operations over tables that the user cannot read completely, `row_index` cannot be specified.

The expression specified in `row_access_predicate` must be an expression that depends solely on the values in the row. You cannot use `JOIN`, `GROUP BY`, current user in it. You also cannot use QL UDFs.

### Information leakage through metadata

When evaluating the predicate, restrictions on access to columns (see [Managing access to table columns](../../../user-guide/storage/columnar-acl.md)) are ignored. Since access is managed by the data owner, this is not a vulnerability per se, but may lead to a data leakage based on various metadata, so you need to be careful when designing predicates.

Moreover, even if there are no column restrictions, metadata can still provide information that may be sensitive.

Consider a toy example:
Suppose a table contains two rows and has the following ACE:
```
{
    action = allow;
    subjects = [vasya];
    permissions = [read];
    row_access_predicate = "region != 'RU' or income < 1000";
}
```
Vasya reads the table and sees only one row. From this, it can be concluded that for the remaining row the predicate does not hold, which means that `region = 'RU' and income >= 1000` holds for it. Whether this information is sensitive and whether it is even possible (the example is intentionally toy, in real conditions the table will likely contain more rows) is at the discretion of the data owner.


## Example { #example }

Suppose an ACL with the following ACE is set on the table:

```bash
{
    action = allow;
    subjects = [username];
    permissions = [read];
    row_access_predicate = "user_id = 12345";
}
```

Assume that there are no more row-level ACEs all the way up to the root from this table.

The following then holds:

1. Any user who has `read` access to this table will not be able to read any rows by default. However, such a user will be able to read the table attributes.

2. User `username` will be able to read only those rows where the `user_id` column equals `12345` (provided that this user also has `read` access to the entire table).

3. If a user attempts to read rows without the `omit_inaccessible_rows` flag, an authorization error will occur.

## Full read permission { #full_read }

The `full_read` permission allows reading the entire table regardless of other ACEs with `row_access_predicate`. This permission is necessary for some actions that either work at the control plane level or do not interpret (and therefore do not unpack data blocks) rows. Such actions include `copy`, `concatenate`, `move` commands, as well as the `RemoteCopy` operation.

Note: A trivial predicate (an expression that always returns `true`) is not equivalent to `full_read`. Since in the general case checking access to all rows requires reading all rows, {{product-name}} makes no attempts to check whether the predicate is trivial. Symmetrically, with an always-false predicate, the system will be forced to read all rows of the table and check for each one whether the expression still evaluates to false.

## Notes { #notes }

{% note warning "Attention" %}

Setting a row-level ACE restricts access to rows for all users not matching the predicate. Therefore, before restricting access at the row level, make sure that doing so will not break the processes working with the table.

{% endnote %}

1. In the current implementation, the feature applies only to row reading. Therefore, do not specify permissions other than `read` in the `permissions` attribute.
2. You can only use columns specified in schema in the predicate.
3. Row-level ACEs are inherited in a standard way and their inheritance is broken specifically in those nodes where `inherit_acl = %false` is set.
4. If the effective ACL of a table contains an ACE in which the expression is invalid (for example, refers to a non-existent column, the return type differs from boolean, or it adds a number and a string), any read operation will return an error. To avoid this, it is recommended to set such ACEs only in those directories in which all tables have a homogeneous schema.
5. To test a predicate, you can use the Merge operation with `input_query`, placing the expression after the `WHERE` keyword. Both mechanisms use the same syntax for expressions, so the result of the operation will match which rows the user affected by the corresponding ACE will see.
6. Only {{product-name}} administrators can manage row-level ACEs.
