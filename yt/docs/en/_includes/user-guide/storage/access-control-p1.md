# General information

This section contains information about the system of access control to tables and other [Cypress](../../../user-guide/storage/cypress.md) nodes (users, groups, accounts, chunks, transactions, etc.).

Any user request passes through proxies that [**authenticate**](https://{{lang}}.wikipedia.org/wiki/Authentication) the user, that is, verify their authenticity. Based on the token contained in the request and obtained using the [OAuth](https://{{lang}}.wikipedia.org/wiki/OAuth) protocol, the proxy determines the name of the user who initiated the request. If no token is specified in the request, it is assumed that the request was made by the `guest` user. After authentication, the token is not used at the subsequent user request processing stages. For more information, see [Authentication](../../../user-guide/storage/auth.md).

**Authorization** (granting permissions) is performed by the Cypress master server. The decision to grant or deny access depends on:

1. The access type (read, write, etc.).
2. The user who initiated the request.
3. The object to which access is requested.

If access is denied, a message with details is generated: the name of the user to whom access is denied, the object to which access was requested, and the access type.

The {{product-name}} system supports lists of **users** and **groups**. The common name for users and groups is **subjects**.

Each object in the {{product-name}} system has an **Access Control List** (**ACL**). This list is stored in the `@acl` attribute of the object and consists of individual entries (**Access Control Entry** or **ACE**) where each entry contains a list of subjects, access type, and a number of other parameters listed in the table in the [Authorization](../../../user-guide/storage/access-control.md#authorization) section.

## Users, groups { #users_groups }

A list of all registered users of the system is stored in Cypress at `//sys/users`. Each user has a unique name. If a request marked with the name of a non-existent user is received from a proxy, the master server returns a `No such user` error. This can happen because obtaining a token and registering a user in the system are two separate actions that are performed by different services (OAuth provides a token, while the {{product-name}} master server handles registration).

A list of all registered groups can be found at `//sys/groups`. Each group also has a unique name.

{% note warning "Attention" %}

Users and groups are located in the same namespace, which means that their names must not coincide.

{% endnote %}

Group members can be random subjects: both users and other groups. The system guarantees that the "membership in the group" relation does not contain cycles. In the process of authorization, decisions are made on the basis of a transitive closure. Thus, user `A` can be a member of group `C` either directly (being a member of group `С`) or indirectly (`A` being a member of group `B` and group `B` being a member of group `C`).
