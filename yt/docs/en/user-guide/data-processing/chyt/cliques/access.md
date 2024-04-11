# Access permissions

This article describes the clique access rules and how to request them.

## Authentication { #authentication }

Users are authenticated in CHYT in the same way as in {{product-name}} — using a token from {{product-name}} (for more information, see [Authentication](../../../../user-guide/storage/auth.md)).

## Access types { #access_types }

Upon receiving a query, CHYT checks for two types of permissions:

1. **Permissions to access clique resources** </br>
   When receiving a query, the clique checks whether the user that made the query has the permission to use the clique.
   - To execute SQL queries, you need the `use` permission.
   - To modify, view, and delete a clique configuration, you need the `manage`, `read`, and `remove` permissions.

   Clique permissions are stored in a dedicated system node called Access Control Object. It's located at the path `//sys/access_control_object_namespaces/chyt/<alias>/principal` and is generated automatically for each clique created with the [CHYT Controller](../cliques/controller.md).

   Clique permissions only control access to the computational resources of the cliques themselves.

2. **Data access permissions** </br>
   When accessing any data in {{product-name}}, the user's read/write access to all referenced tables is verified according to the standard ACL mechanism implemented in {{product-name}}. This verification ensures secure data access.
