# General information

This section contains information about the system of access control to tables and other [Cypress](../../../user-guide/storage/cypress.md) nodes (users, groups, accounts, chunks, transactions, and other).

Any user request passes through proxies that [**authenticate**](https://{{lang}}.wikipedia.org/wiki/Authentication) the user, that is, verify their authenticity. Based on the token contained in the request and obtained using the [OAuth](https://{{lang}}.wikipedia.org/wiki/OAuth) protocol, the proxy determines the name of the user who initiated the request. If no token is specified in the request, it is assumed that the request was made by the `guest` user. After authentication, the token is not used at the subsequent user request processing stages. For more information, see [Authentication](../../../user-guide/storage/auth.md).

**Authorization** (granting permissions) is performed by the Cypress master server. The decision to grant or deny access depends on:

1. The access type (read, write, etc.).
2. The user who initiated the request.
3. The object to which access is requested.

If access is denied, a message with details is generated: the name of the user to whom access is denied, the object to which access was requested, and the access type.

If the proxy sends a request marked with the name of a non-existent user, the master server returns the error `No such user`. This can happen because obtaining a token and registering a user in the system are two separate actions performed by different services (OAuth provides a token, while the {{product-name}} master server handles registration).

The {{product-name}} system supports lists of **users** and **groups**. The common name for users and groups is **subjects**. Group members can be random subjects: both users and other groups. The system guarantees acyclic membership relation. In the process of authorization, decisions are made on the basis of a transitive closure. Thus, user `A` can be a member of group `C` either directly (being a member of group `С`) or indirectly (`A` being a member of group `B` and group `B` being a member of group `C`).

Each object in the {{product-name}} system has an **Access Control List** (**ACL**). This list is stored in the `@acl` attribute of the object and consists of individual entries (**Access Control Entry** or **ACE**) where each entry contains a list of subjects, access type, and a number of other parameters listed in the table in the [Authorization](../../../user-guide/storage/access-control.md#authorization) section. For more information, see [How to use the ACL correctly](#acl_usage).

## Users, groups { #users_groups }

Cypress stores:

* The list of all registered users. It is located at `//sys/users` and constitutes a `user_map` node.
* The list of all registered groups. It is located at `//sys/groups` and constitutes a `group_map` node.

{% note warning "Attention" %}

Each user and group is defined by a unique name. Since these entities share the same namespace, duplicate names are not allowed.

{% endnote %}

#### How to view lists of users and groups

{% list tabs %}

- In the web interface

  - To view a list of users, go to **Users** in the side panel or type `{{cluster-ui}}/users` in the browser address bar
  - To view a list of groups, go to **Groups** or type `{{cluster-ui}}/groups` in the browser address bar

- Via the CLI

  - Get a list of users:
    ```bash
    yt --proxy <cluster-name> list //sys/users
    ```
  - Get a list of groups:
    ```bash
    yt --proxy <cluster-name> list //sys/groups
    ```
{% endlist %}

{% note warning %}

The `//sys/users` and `//sys/groups` nodes are not tables, so you cannot get lists of users and groups using a `SELECT` query.

{% endnote %}

Users have the `user` system object type. Groups have the `group` object type.

#### How to get information about a user or group

{% list tabs %}

- In the web interface

  Go to the user or group page and click ![](../../../../images/attrs-icon.png =20x20) on the right.

- Via the CLI

  To get information about a particular user, use the command `yt --proxy <cluster-name> get //sys/users/<user-name>/@<attr-name>`. For example:

  ```
  $ yt --proxy <cluster-name> get //sys/users/john/@type
  "user"
  ```

  To see a list of all available attributes for a user, use the command:

  ```bash
  $ yt --proxy <cluster-name> get //sys/users/john/@
  {
    "id" = "ffffffff-fffffffe-101f5-1407420e";
    "type" = "user";
    "builtin" = %true;
    ...
  }
  ```

  Similarly, to get information about a particular group, use the command `yt --proxy <cluster-name> get //sys/groups/<group-name>/@<attr-name>`. For example:

  ```bash
  $ yt --proxy <cluster-name> get //sys/groups/admins/@type
  "group"
  ```

{% endlist %}
