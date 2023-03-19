# Administering a private clique

This article describes how to administer your own clique and which manifestations of its vital activity can and should be monitored.

## Accesses { #access }

Users are authenticated in CHYT in the same way as in {{product-name}} — using a token from {{product-name}} (for more information, see [Authentication](../../../../../user-guide/storage/auth.md)). CHYT checks permissions of two types.

First, when accepting a query, the clique checks that the user under which the query is made has the permission to use the clique, which is due to the ACL for the operation in which the clique has been  started. For the user to be able to run queries in the clique, the `read` permissions are required. This check prevents you from using the computing resources of other cliques. The `manage` permissions still mean that you can control the operation, for example, stop the clique or suspend the clique instances.

For example, if an ACL of the following form was specified when the operation was started:

```
acl = [
    {
        subjects = [user1; user2];
        action = allow;
        permissions = [read; manage];
    };
    {
        subjects = [second-floor-guys];
        action = allow;
        permissions = [read];
    };
];
```

Where `second-floor-guys` denotes a group in {{product-name}}, then `second-floor-guys`, `user1`, and `user2` will be able to make queries to this clique and the last two will also be able to stop it.

Second, when accessing any data in {{product-name}}, there is a check of read or write access to all tables mentioned in the query according to the standard ACL mechanism used in {{product-name}}. This check ensures secure data access.

## Changing the ACL of a running clique { #change-acl }

To change the ACL, use the `update_op_parameters` command (link?). To do this, you need to:

1. Enter the clique alias in the **Filter Opertaions** field, then click **Go to operation**. The alias starts with a `\*` symbol, this is the clique name.

   ![find_operation_by_alias](../../../../../../images/find_op_by_alias.png){ .center }

2. Go to the operation page, copy `Id`.

3. Execute the command that completely replaces the operation acl. Insert `Id` from point 3.

   ```bash
   yt --proxy <cluster_name> update-op-parameters --operation <operation_id> '{acl = [{subjects=[robot-1; robot-2; robot-3]; action=allow; permissions=[read]};{subjects=[<subject>]; action=allow; permissions=[read;manage]}]}'
   ```
