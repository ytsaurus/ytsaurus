## System subjects { #system_subjects }

In {{product-name}}, there is a set of subjects that perform system functions. You cannot delete such subjects. System functions include:

1. The `guest`, `root`, `scheduler`, and `job` users.
2. The `everyone`, `users`, and `superusers` groups.

## Subject attributes { #subject_attributes }

The table below shows a list of attributes that are inherent to all subjects.

| **Attribute** | **Type** | **Description** |
| ------------------- | --------------- | ------------------------------------------------------------ |
| `name` | `string` | Subject name (non-empty string) |
| `member_of` | `array<string>` | A list of names of groups to which the subject directly belongs |
| `member_of_closure` | `array<string>` | A list of names of groups to which the subject belongs (directly or indirectly) |
| `aliases` | `array<string>` | A list of names that can be used in the ACL as a reference to the subject |

## User attributes { #user_attributes }

In addition to the inherent attributes that are shared by all subjects, users have the attributes listed in the table below.

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

In addition to the inherent attributes that are shared by all subjects, groups have the attributes listed in the table below.

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

Access type `P` is also called **permission**. The permissions supported by {{product-name}} are shown in the table. The **Applies to** column lists the object types for which the permission is meaningful; for other object types the permission is ignored.

| Permission | Applies to | Description |
| ------------------------- | -------------------------------------------- | ------------------------------------------------------------ |
| `read` | All objects | Reading a value or getting information about an object and its attributes. See [details](#permission_read). |
| `full_read` | Tables (behaves as `read` for other objects) | Reading the whole table with no columnar or row-level restrictions. See [details](#permission_full_read). |
| `write` | All objects | Changing an object state or its attributes. |
| `use` | Accounts, pools, tablet cell bundles | Using an object's resources. See [details](#permission_use). |
| `administer` | All objects | Changing the object access descriptor (its ACL and related attributes). |
| `create` | Object schemas | Creating objects of a corresponding type. |
| `remove` | All objects | Removing an object. |
| `mount` | Dynamic tables | Mounting, unmounting, remounting, freezing, unfreezing, and resharding a dynamic table. |
| `manage` | Operations | Managing an operation and its jobs. See [Managing operations](#managing-operations). |
| `modify_children` | Composite Cypress nodes, accounts, pools | Adding and removing children of a composite object. See [details](#permission_modify_children). |
| `register_queue_consumer` | Queues | Registering a consumer for a queue. See [details](#permission_register_queue_consumer). |

Object `O` means a random system object: Cypress node, user, group, account, chunk, transaction, and so on.

To make a decision, the system implicitly builds an **effective access control list** (effective ACL) for object `O`. An effective access control list is a combination of the access control list specified on the node and the access control lists inherited from its parents. Any **access control list** (ACL) is a list of **access control entries** (ACE). The order of entries in this list does not matter. The object can inherit its ACL, which is controlled by the `inherit_acl = %true` attribute. When inheriting, the inheritance mode (the `inheritance_mode` key in the `acl` entry) is defined for each ACE. The latter can be `object_only`, `object_and_descendants`, `descendants_only`, and `immediate_descendants_only`.

The `object_only` value means that this entry affects only the object itself. The `object_and_descendants` value means that this entry affects the object and all its descendants, including indirect ones. The `descendants_only` value means that this entry affects only descendants, including indirect ones. The `immediate_descendants_only` value means that this entry affects only direct descendants (sons). Each entry has a structure presented in the table.

| **Attribute** | **Type** | **Description** |
| ------------------ | ------------------- | ------------------------------------------------------------ |
| `action` | `SecurityAction` | Either `allow` (allowing entry) or `deny` (denying entry). |
| `subjects` | `array<string>` | A list of names of subjects to which the entry applies. |
| `permissions` | `array<Permission>` | A list of access permissions to which the action specified in the `action` attribute applies. |
| `inheritance_mode` | `InheritanceMode` | The inheritance mode of this ACE, by default `object_and_descendants`. |

Once an effective list is built, the decision to grant or deny access is made according to the following schema:

1. If there is at least one allowing entry for `U` and `P` and no denying entries for `U` and `P`, access is granted.
2. Otherwise, access is denied.

“Entry for `U` and `P`” means that `P` is mentioned in the `permissions` list and either user `U` or at least one group in which user U is a direct or indirect member is mentioned in the `subjects` list. From that description, it follows specifically that if the effective list is empty, access will be denied.

### read { #permission_read }

For tables, `read` grants access to the data, but the visible set of columns and rows can be further narrowed by [columnar](../../../user-guide/storage/columnar-acl.md) and [row-level](../../../user-guide/storage/row-level-security.md) ACLs: even with `read` for the whole table, a user must be explicitly allowed to read particular columns.

### full_read { #permission_full_read }

Grants unrestricted read access to the entire table, ignoring any columnar or row-level restrictions. For all other object types, `full_read` is equivalent to `read`.

It is required for actions that need the complete data set, such as `copy`ing and `move`ing a table.

Because this permission implies access to every column, the `columns` attribute cannot be specified in the same ACE.

### use { #permission_use }

Grants the ability to consume the object's resources:

* for an account, to create new objects in that account's quota;
* for a pool, to run operations in that pool;
* for a tablet cell bundle, to create a dynamic table in that bundle.

### modify_children { #permission_modify_children }

Grants the ability to add and remove children of a composite object. For a Cypress map node, creating or removing a child requires either `write` or `modify_children` on the parent; granting only `modify_children` lets a subject manage the children of a node without granting broader `write` access to the node itself.

### register_queue_consumer { #permission_register_queue_consumer }

Grants the ability to [register a consumer for a queue](../../../user-guide/dynamic-tables/queues.md#register_queue_consumer). An ACE with this permission has two special rules: it must specify vitality via the `vital` attribute (`%true` or `%false`), and it must contain `register_queue_consumer` as its only permission. For example:

```bash
{
    action = allow;
    subjects = [my_group];
    permissions = [register_queue_consumer];
    vital = %true;
}
```

## Object owner and owner user { #owner}

When user `U` creates an object, user U also becomes the owner of that object, which is shown in the `owner` attribute.

{% note info "Note" %}

Only the superuser (member of the `superusers` group) can change the owner.

{% endnote %}

There is also a special fictitious `owner` user in the system. You cannot authenticate with it, but you can refer to it in the ACL as a subject. `owner` is replaced by the actual object owner when permissions are checked. This enables you to do such things as describe the "in this folder, only those who created the nodes can remove them" restriction by the ACE:`{action=allow; permissions=[remove]; subjects=[owner]; inheritance_mode = descendants_only}`. Remember to disable permission inheritance by specifying `inherit_acl = %false`.

## Managing operations { #managing-operations }

Any operation has an ACL associated with it, similar to Cypress nodes. You can obtain this ACL from the attribute at `runtime_parameters/acl` of the operation.

Access permissions have the following semantics:

- The `read` permission is responsible for reading the live preview of the output and intermediate data of the operation, as well as the "artifacts" of its jobs: stderr, fail context, and the input data of individual jobs.
- The `manage` permission is responsible for managing actions that change the state of the operation, including `Abort`, `Complete`, `Suspend`, and `Resume` (the corresponding buttons are located in the upper-right corner of the operation page in the web interface), as well as for managing actions with jobs, including `Abort`, `Abandon`, and `Send signal` (the corresponding buttons are located in the drop-down menu on the right side of the `Jobs` page in the {{product-name}} web interface).
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

ACL inheritance should be used everywhere, except in places where the nature of access changes radically. For example, on the Cypress root, the default entry is set to allow all users (except `guest`) to read. When overriding access to parts of Cypress that store security-sensitive information, such as token lists, the access definitions must be redefined from scratch.

## Checking the ACL { #check_acl}

To check if a user has a certain permission to a certain Cypress node, use the `check-permission` command.

Example:

```bash
yt check-permission <user_name> <permission> <folder>
```

Example of command launch:

```bash
$ yt check-permission pavel-kulenov write //tmp
{
  "action" = "allow";
  "object_id" = "1-3-411012f-1888ce1f";
  "object_name" = "node //tmp";
  "subject_id" = "c4-8aaa-41101f6-bec6113b";
  "subject_name" = "YTsaurus";
}
```

## Requesting access { #request_access }

To gain access to an existing account or directory in {{product-name}}, {% if audience == "public" %}contact your system administrator{%else%}see [Configuring access to data](../../../user-guide/storage/acl-manage.md){%endif%}.
