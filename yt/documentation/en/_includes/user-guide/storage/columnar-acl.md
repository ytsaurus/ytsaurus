# Managing access to table columns

This section describes how ACLs on columns work and gives examples of their use.

## Brief description { #brief_desc }

ACLs on columns enable you to specify access rules for individual table columns. This enables you to store both public and private data in the same table. Processes and users who have access will be able to read all columns in the table, including private ones, while other users will only be able to read public columns.

## Operation principle { #details }

The general operation principles of the access control system in {{product-name}} are described in the [Access control](../../../user-guide/storage/access-control.md) section.

To set an ACL on columns, use ACEs of a special type where the additional `columns` attribute is specified in the ACLs of the nodes, both tables and directories. Such ACEs (**columnar**) do not take part in ordinary checks of permissions for objects in any way. Such ACEs are taken into account at the stage of checking access to individual table columns.

Suppose that the system is checking read access permissions for table `T` on behalf of user `U` and this user wants to read columns from set `Cs`. Such reads occur when the `read-table` command is executed or when table `T` is used as input to a MapReduce operation or others.

This feature is only available for [schematic data.](../../../user-guide/storage/static-schema.md). In particular, we assume that `Cs` is a subset of all columns explicitly specified in the schema of table `T`.

Set `Cs` is usually specified by the user when setting a column filter, for example, via [YPath](../../../user-guide/storage/ypath.md). When no filter is specified, `Cs` are considered a set of all columns described in the table schema. If the table schema is not strict, additional columns can be found in the data, but no special checks are made for them.

In addition to having a `read` permission for the entire table, the user must also have a `read` permission for each column of set `Cs`.

The latter property is checked as follows: let's consider the name of column `C` from `Cs`. Let's build an effective ACL of table `T`. From the ACEs that make it up, select those that are columnar and applicable to `C` (contain `C` in the `columns` attribute). Label the result `L`.

If `L` is empty, this means that no special restrictions apply to column `C` and checking access to this column is considered successful.

Otherwise, if at least one suitable ACE is found, the following logic applies. From the previously built list `L`, select those entries that concern the read operation (contain `read` in `permissions`) and relate to the current user `U` (`U` is either explicitly mentioned in `subjects` or belongs to one of the groups mentioned in `subjects`). Denote the new list by `L'`. If `L'` has at least one allowing entry and no denying entires, then the check is successful. Otherwise, access will be denied.

## Compatibility mode { #compatibility_mode }

Sometimes, even though they only need a small part of the columns, users order reading of the entire table (that is, no column filter is specified). If you set a columnar ACL on a table in a scenario like this, users who do not have access to the protected columns will receive an authorization error.

For such cases, there is a special `omit_inaccessible_columns` flag (the default value is `%false`) which can be specified for read operations. A setting with the same name is also available in the operation specification.

When this option is enabled, if you cannot get read access to any table column (if there is read access to the table as a whole), the system hides it completely from the user and the requested action is completed successfully. In the response to the read request (in so called `output parameters`), a list of columns that were skipped during reading due to lack of permissions is then passed. In case of an operation, such information is displayed in a warning from the scheduler.

This feature is actively used in the {{product-name}} web interface to enable you to view data in columns to which the user has access and warn that some columns are hidden due to lack of access.

## Example { #example }

Suppose an ACL with the following ACE is set on the table:

```bash
{
    action = allow;
    subjects = [username];
    permissions = [read];
    columns = [money];
}
```

Let's also assume that there are no more columnar ACEs all the way up to the root from this table.

The following is then performed:

1. Any user who has `read` access to this table will be able to read all columns, except `money`.

2. If a user other than `username` attempts to read the `money` column, an authorization error will occur. This will happen both when you explicitly specify this column name in the filter and when you attempt to read without any filter.

3. User `username` will be able to read the `money` column (provided that this user also has `read` access to the entire table).

## Notes { #notes }

{% note warning "Attention!" %}

Setting an ACE with a column read permission causes all users not specified in the ACE to automatically lose the read permission for the column. Therefore, before restricting access to the column, make sure that doing so will not break the processes working with the table.

{% endnote %}

1. In the current implementation, the feature applies only to column reading. Therefore, do not specify permissions other than `read` in the `permissions` attribute.
2. If the table has no schema or the schema is empty and weak, you cannot restrict access to its individual columns. You cannot restrict access to columns that are not specified in the schema if the schema is not strict, even if any restrictions for them are described in the ACL. Do not forget to schematize your data and describe the protected columns explicitly in the schema.
3. Columnar ACEs must be specified not only on the tables, but also on the directories. ACE data is inherited in a standard way and its inheritance is broken specifically in those nodes where `inherit_acl = %false` is set.
4. Only {{product-name}} administrators can manage columnar ACEs.
