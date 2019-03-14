#include "acl.h"

#include <yt/core/ytree/fluent.h>
#include <yt/core/ytree/serialize.h>

namespace NYT::NSecurityClient {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TSerializableAccessControlEntry::TSerializableAccessControlEntry() = default;

TSerializableAccessControlEntry::TSerializableAccessControlEntry(
    ESecurityAction action,
    std::vector<TString> subjects,
    EPermissionSet permissions,
    EAceInheritanceMode inheritanceMode)
    : Action(action)
    , Subjects(std::move(subjects))
    , Permissions(permissions)
    , InheritanceMode(inheritanceMode)
{ }

bool operator == (const TSerializableAccessControlEntry& lhs, const TSerializableAccessControlEntry& rhs)
{
    return
        lhs.Action == rhs.Action &&
        lhs.Subjects == rhs.Subjects &&
        lhs.Permissions == rhs.Permissions &&
        lhs.InheritanceMode == rhs.InheritanceMode;
}

bool operator != (const TSerializableAccessControlEntry& lhs, const TSerializableAccessControlEntry& rhs)
{
    return !(lhs == rhs);
}

// NB(levysotsky): We don't use TYsonSerializable here
// because we want to mirror the TAccessControlList structure,
// and a vector of TYsonSerializable-s cannot be declared (as it has no move constructor).
void Serialize(const TSerializableAccessControlEntry& acl, NYson::IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer)
        .BeginMap()
            .Item("action").Value(acl.Action)
            .Item("subjects").Value(acl.Subjects)
            .Item("permissions").Value(acl.Permissions)
            .Item("inheritance_mode").Value(acl.InheritanceMode)
        .EndMap();
}

void Deserialize(TSerializableAccessControlEntry& acl, NYTree::INodePtr node)
{
    using NYTree::Deserialize;

    const auto mapNode = node->AsMap();

    Deserialize(acl.Action, mapNode->GetChild("action"));
    Deserialize(acl.Subjects, mapNode->GetChild("subjects"));
    Deserialize(acl.Permissions, mapNode->GetChild("permissions"));
    if (const auto inheritanceModeNode = mapNode->FindChild("inheritance_mode")) {
        Deserialize(acl.InheritanceMode, inheritanceModeNode);
    }
}

void TSerializableAccessControlEntry::Persist(const TStreamPersistenceContext& context)
{
    using NYT::Persist;

    Persist(context, Action);
    Persist(context, Subjects);
    Persist(context, Permissions);
    Persist(context, InheritanceMode);
}

bool operator == (const TSerializableAccessControlList& lhs, const TSerializableAccessControlList& rhs)
{
    return lhs.Entries == rhs.Entries;
}

bool operator != (const TSerializableAccessControlList& lhs, const TSerializableAccessControlList& rhs)
{
    return !(lhs == rhs);
}

void Serialize(const TSerializableAccessControlList& acl, NYson::IYsonConsumer* consumer)
{
    NYTree::Serialize(acl.Entries, consumer);
}

void Deserialize(TSerializableAccessControlList& acl, NYTree::INodePtr node)
{
    NYTree::Deserialize(acl.Entries, node);
}

void TSerializableAccessControlList::Persist(const TStreamPersistenceContext& context)
{
    NYT::Persist(context, Entries);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSecurityClient
