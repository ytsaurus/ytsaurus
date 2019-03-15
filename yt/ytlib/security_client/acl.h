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
    std::optional<std::vector<TString>> Columns;

    TSerializableAccessControlEntry(
        ESecurityAction action,
        std::vector<TString> subjects,
        NYTree::EPermissionSet permissions,
        EAceInheritanceMode inheritanceMode = EAceInheritanceMode::ObjectAndDescendants);

    // Use only for deserialization.
    TSerializableAccessControlEntry();
};

bool operator == (const TSerializableAccessControlEntry& lhs, const TSerializableAccessControlEntry& rhs);
bool operator != (const TSerializableAccessControlEntry& lhs, const TSerializableAccessControlEntry& rhs);

void Serialize(const TSerializableAccessControlEntry& ace, NYson::IYsonConsumer* consumer);
void Deserialize(TSerializableAccessControlEntry& ace, NYTree::INodePtr node);

struct TSerializableAccessControlList
{
    std::vector<TSerializableAccessControlEntry> Entries;
};

bool operator == (const TSerializableAccessControlList& lhs, const TSerializableAccessControlList& rhs);
bool operator != (const TSerializableAccessControlList& lhs, const TSerializableAccessControlList& rhs);

void Serialize(const TSerializableAccessControlList& acl, NYson::IYsonConsumer* consumer);
void Deserialize(TSerializableAccessControlList& acl, NYTree::INodePtr node);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSecurityClient
