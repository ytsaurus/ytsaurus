# Import users and groups from the external systems.

Manual management {{product-name}}' users and groups is discussed in the section [Managing users, groups](../../admin-guide/cluster-operations.md#managing-users,-groups-and-access-controls).  
Sometimes it is required to keep users and group lists in sync with an external system.

There is an app [ytsaurus-identity-sync](https://github.com/tractoai/ytsaurus-identity-sync)
which might help when such an issue arises.

The application supports importing users and groups from the two sources:
- Microsoft Entra (previously Azure Active Directory) via [Microsoft Graph REST API](https://learn.microsoft.com/en-us/graph/azuread-users-concept-overview);
- [Lightweight Directory Access Protocol (LDAP)](https://en.wikipedia.org/wiki/Lightweight_Directory_Access_Protocol).

The application periodically collects users, groups and memberships from the external system and updates
{{product-name}} users and groups accordingly.

More information on installation and configuration can be found [application README](https://github.com/tractoai/ytsaurus-identity-sync?tab=readme-ov-file#installing). 

