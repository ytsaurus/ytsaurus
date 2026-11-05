# Access permissions

Access permissions for a clique are stored in a dedicated system node — **Access Control Object** (ACO). For more information about this object, see the [section](../../../../user-guide/query-tracker/about.md#access-control).

## Authentication { #authentication }

Users are authenticated in CHYT in the same way as in {{product-name}} — using a token from {{product-name}} (for more information, see [Authentication](../../../../user-guide/storage/auth.md)).

## Types of permissions { #types }

When working with cliques, four types of permissions are used:

- `use` — allows you to run queries against the clique;
- `read` — grants access to read the clique configuration;
- `manage` — allows you to modify the clique configuration;
- `remove` — grants the permission to delete the clique.

## How permission checks work { #how-it-works }

When working with cliques, CHYT checks two types of permissions:

1. Data access permissions. When you access any data in {{product-name}}, the system checks read and write permissions for all tables mentioned in the query. This happens via the standard ACL mechanism built into {{product-name}}. This ensures secure data access.
2. Permissions to access clique resources. Before running a query, the clique checks whether the user has the required permissions:
   - SQL queries require the `use` permission;
   - modifying, viewing, or deleting the configuration requires the `manage`, `read`, and `remove` permissions.

Clique permissions only control access to the computational resources of the cliques.

## Access permission settings interface { #ui }

Access settings are located on the **ACL** (Access Control List) tab in the [interface](../../../../user-guide/data-processing/chyt/cliques/ui.md) of the clique. There, you’ll find:

- a list of people responsible for the clique (**Responsibles**);
- a list of users with different access levels (**Object permissions**);
- a list of available permissions: `use`, `manage`, `read`, `remove`;
- buttons for assigning permissions (**Request Permissions**) and managing responsibles (**Manage responsibles**).

For information on how to manage access permissions, see the [section](../../../../user-guide/data-processing/chyt/how-to-guides/acl.md).
