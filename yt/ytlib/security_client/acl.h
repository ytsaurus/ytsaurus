#pragma once

#include "public.h"

#include <yt/core/yson/consumer.h>

#include <yt/core/ytree/permission.h>

#include <vector>

namespace NYT::NSecurityClient {

////////////////////////////////////////////////////////////////////////////////

struct TSerializableAccessControlEntry
{
    ESecurityAction Action = ESecurityAction::Undefined;
    std::vector<TString> Subjects;
    NYTree::EPermissionSet Permissions;
    EAceInheritanceMode InheritanceMode = EAceInheritanceMode::ObjectAndDescendants;
};

void Serialize(const TSerializableAccessControlEntry& acl, NYson::IYsonConsumer* consumer);
void Deserialize(TSerializableAccessControlEntry& acl, NYTree::INodePtr node);

struct TSerializableAccessControlList
{
    std::vector<TSerializableAccessControlEntry> Entries;
};

void Serialize(const TSerializableAccessControlList& acl, NYson::IYsonConsumer* consumer);
void Deserialize(TSerializableAccessControlList& acl, NYTree::INodePtr node);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSecurityClient
