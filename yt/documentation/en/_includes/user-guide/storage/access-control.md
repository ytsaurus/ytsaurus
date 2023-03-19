# General information

This section contains information about the system of access control to tables and other [Cypress](../../../user-guide/storage/cypress.md) nodes (users, groups, accounts, chunks, transactions, etc.).

Any user request passes through proxies that [**authenticate**](https://en.wikipedia.org/wiki/Authentication) the user, that is, verify their authenticity. Based on the token contained in the request and obtained using the [OAuth](https://ru.wikipedia.org/wiki/OAuth) protocol, the proxy determines the name of the user who initiated the request. If no token is specified in the request, it is assumed that the request was made by the `guest` user. After authentication, the token is not used at the subsequent user request processing stages. For more information, see [Authentication](../../../user-guide/storage/auth.md).

**Authorization** (granting permissions) is performed by the Cypress master server. The decision to grant or deny access depends on:

1. The access type (read, write, etc.).
2. The user who initiated the request.
3. The object to which access is requested.

If access is denied, a message with details is generated: the name of the user to whom access is denied, the object to which access was requested, and the access type.

The {{product-name}} system supports lists of **users** and **groups**. The common name for users and groups is **subjects**.

Each object in the {{product-name}} system has an **Access Control List** (**ACL**). This list is stored in the `@acl` attribute of the object and consists of individual entries (**Access Control Entry** or **ACE**) where each entry contains a list of subjects, access type, and a number of other parameters listed in the table in the [Authorization](../../../user-guide/storage/access-control.md#authorization) section.

## Users, groups { #users_groups }

A list of all registered users of the system is stored in Cypress at `//sys/users`. Each user has a unique name. If a request marked with the name of a non-existent user is received from a proxy, the master server returns a `No such user` error.

A list of all registered groups can be found at `//sys/groups`. Each group also has a unique name.

{% note warning "Attention!" %}

Users and groups are located in the same namespace, which means that their names must not coincide.

{% endnote %}

Group members can be random subjects: both users and other groups. The system guarantees that the "membership in the group" relation does not contain cycles. In the process of authorization, decisions are made on the basis of a transitive closure. Thus, user `A` can be a member of group `C` either directly (being a member of group `С`) or indirectly (`A` being a member of group `B` and group `B` being a member of group `C`).

## System subjects { #system_subjects }

In {{product-name}}, there is a set of subjects that perform system functions. You cannot delete such subjects. System functions include:

1. The `guest`, `root`, `scheduler`, and `job` users.
2. The `everyone`, `users`, and `superusers` groups.

## Subject attributes { #subject_attributes }

The table shows a list of attributes that all subjects have.

| **Attribute** | **Type** | **Description** |
| ------------------- | --------------- | ------------------------------------------------------------ |
| `name` | `string` | Subject name (non-empty string) |
| `member_of` | `array<string>` | A list of names of groups to which the subject directly belongs |
| `member_of_closure` | `array<string>` | A list of names of groups to which the subject belongs (directly or indirectly) |
| `aliases` | `array<string>` | A list of names that can be used in the ACL as a reference to the subject |

## User attributes { #user_attributes }

In addition to the attributes inherent to all subjects, users have the attributes presented in the table.

| **Attribute** | **Type** | **Description** | **Mandatory** |
| -------------------------- | --------------- | ------------------------------------------------------------ | ------------------- |
| `banned` | `bool` | Whether the user is blocked | No |
| `access_time` | `DateTime` | The time of the last request from the user. | Yes |
| `access_counter` | `integer` | The total number of requests made by the user. | Yes |
| `request_rate` | `double` | The number of requests per second from the user. | Yes |
| `request_rate_limit` | `double` | The limit on the number of requests per second from the user. The default value is 100. | Yes |
| `request_queue_size_limit` | `double` | The request queue size. The default value is 100. | Yes |
| `usable_accounts` | `array<string>` | A list of accounts that the user is allowed to use. | Yes |

## Group attributes { #group_attributes }

In addition to the attributes inherent to all subjects, groups have the attributes presented in the table.

| **Attribute** | **Type** | **Description** |
| ----------- | --------------- | --------------------------------------------------------- |
| `members` | `array<string>` | A list of names of group members (users and other groups). |

## Managing groups { #group_control }

{% note info "Note" %}

On large clusters, only {{product-name}} administrators can directly manage groups.

{% endnote %}

To create a new group, use the `create` command. When creating an object, you do not need to specify its path, but you need to specify the `name` attribute.
```bash
yt create group --attributes '{name=my_group}'
```

You can remove objects using the `remove` command. When a group is removed, it is automatically removed from all ACLs where it was present.
```bash
yt remove //sys/groups/my_group
```

## Authorization { #authorization }

The authorization module on the master server solves the following problem: should user `U` be granted access of type `P` for object `O`? There are two possible answers: to grant or not to grant. Let us review all three components (U, P, and O) separately.

Any system user can act as `U`. Note that `U` cannot be a group, although group membership is taken into account when making a decision. If `U` is `root`, the access request is automatically approved.

Access type `P` is also called **permission**. The permissions supported by {{product-name}} are shown in the table.

| Permission | Description |
| ------------ | ------------------------------------------------------------ |
| `read` | Means reading a value or getting information about an object or its attributes. |
| `write` | Means changing an object state or its attributes. |
| `use` | Applies to accounts, pools, and bundles and means usage (that is, the ability to insert new objects into the quota of a given account, run operations in a pool, or move a dynamic table to a bundle). |
| `administer` | Means changing the object access descriptor. |
| `create` | Applies only to schemas and means creating objects of this type. |
| `remove` | Means removing an object. |
| `mount` | Means mounting, unmounting, remounting, and resharding a dynamic table. |
| `manage` | Applies only to operations (not to Cypress nodes) and means managing that operation or its jobs. |

Object `O` means a random system object: Cypress node, user, group, account, chunk, transaction, and so on.

To make a decision, the system implicitly builds an **effective access control list** (effective ACL) for object `O`. An effective access control list is a node access control list and everything inherited from its parents. Any **access control list** (ACL) is a list of **access control entries** (ACE). The order of entries in this list does not matter. The object can inherit its ACL, which is controlled by the `inherit_acl = %true` attribute. When inheriting, the inheritance mode (the `inheritance_mode` key in the `acl` entry) is defined for each ACE. The latter can be `object_only`, `object_and_descendants`, `descendants_only`, and `immediate_descendants_only`.

The `object_only` value means that this entry affects only the object itself. The `object_and_descendants` value means that this entry affects the object and all its descendants, including indirect ones. The `descendants_only` value means that this entry affects only descendants, including indirect ones. The `immediate_descendants_only` value means that this entry affects only direct descendants (sons). Each entry has a structure presented in the table.

| **Attribute** | **Type** | **Description** |
| ------------------ | ------------------- | ------------------------------------------------------------ |
| `action` | `string` | Either `allow` (allowing entry) or `deny` (denying entry). |
| `subjects` | `array<string>` | A list of names of subjects to which the entry applies. |
| `permissions` | `array<Permission>` | A list of access permissions to which the action specified in the `action` attribute applies. |
| `inheritance_mode` | `InheritanceMode` | The inheritance mode of this ACE, by default `object_and_descendants`. |

Once an effective list is built, the decision to grant or deny access is made according to the following schema:

1. If there is at least one allowing entry for `U` and `P` and no denying entries for `U` and `P`, access is granted.
2. Otherwise, access is denied.

"Entry for `U` and `P`" means that `P` is mentioned in the `permissions` list and user `U` or at least one group in which user U is directly or indirectly a member is mentioned in the `subjects` list. From that description, it follows specifically that if the effective list is empty, access will be denied.

## Object owner and owner user { #owner}

When user `U` creates an object, user U also becomes the owner of that object, which is shown in the `owner` attribute.

{% note info "Note" %}

Only the superuser (member of the `superusers` group) can change the owner.

{% endnote %}

There is also a special fictitious `owner` user in the system. You cannot authenticate with it, but you can refer to it in the ACL as a subject. `owner` is replaced by the actual object owner when permissions are checked. This enables you to do such things as describe the "in this folder, only those who created the nodes can remove them" restriction by the ACE:`{action=allow; permissions=[remove]; subjects=[owner]; inheritance_mode = descendants_only}`. Remember to disable permission inheritance by specifying `inherit_acl = %false`.

## Managing operations

Any operation has an ACL associated with it, similar to Cypress nodes. You can obtain this ACL from the attribute at `runtime_parameters/acl` of the operation.
Access permissions have the following semantics:

- The `read` permission is responsible for reading the live preview of the output and intermediate data of the operation, as well as the "artifacts" of its jobs: stderr, fail context, and the input data of individual jobs.
- The `manage` permission is responsible for managing the state of the operation, i.e. the `Abort`, `Complete`, `Suspend`, and `Resume` actions. The corresponding buttons are located in the upper-right corner of the operation page in the web interface. It is also responsible for actions with jobs: `Abort`, `Abandon`, and `Send signal`. The corresponding buttons are located in the drop-down menu on the right side of the `Jobs` page in the {{product-name}} web interface.
- Using [Job Shell](../../../user-guide/problems/jobshell-and-slowjobs.md) requires `read` and `manage` permissions at the same time , since having access via the console enables you to read or randomly change the job state.

You can specify the ACL for an operation when it starts. To do this, in the operation specification, indicate an `"acl"` section as follows:

```python
yt.run_map(..., spec={"acl": [{
 "action": "allow",
 "subjects": ["u1", "u2"],
 "permissions": ["read", "manage"],
}]})
```

The user who started the operation and the default administrators are added to the resulting ACL of the operation.

## How to use the ACL correctly { #acl_usage}

The access management schema presented above is very general and flexible. When using it, try to assign roles at the highest level with inheritance once, rather than manage access at the level of individual tables. ACL inheritance encourages grouping data with common access rules into folders.

In normal mode, you should have only allowing entries. Denying entries can be set to solve the problem quickly if it turns out that access is overextended. Then, the ACL should be revised, and the problem should be solved by adjusting the allowing entries.

We do not recommend mentioning specific users in the ACL. When access is granted, we recommend using groups with the same type of needs instead of individual users.

ACL inheritance should be used everywhere, except in places where the nature of access changes radically. For example, on the Cypress root, the default entry is set to allow all users (except `guest`) to read. In those Cypress places where security-sensitive information is stored, such as token lists, access is redefined from scratch.

## Checking the ACL { #check_acl}

To check if a user has a certain permission to a certain Cypress node, use the `check-permission` command. Example:

```bash
yt check-permission yql write //tmp
{
  "action" = "allow";
  "object_id" = "1-3-411012f-1888ce1f";
  "object_name" = "node //tmp";
  "subject_id" = "c4-8aaa-41101f6-bec6113b";
  "subject_name" = "YTsaurus";
}
```

## Requesting access { #request_access }

To gain access to an existing account or directory in {{product-name}}, contact your system administrator.

