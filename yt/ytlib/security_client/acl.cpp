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
        lhs.InheritanceMode == rhs.InheritanceMode &&
        lhs.Columns == rhs.Columns;
}

bool operator != (const TSerializableAccessControlEntry& lhs, const TSerializableAccessControlEntry& rhs)
{
    return !(lhs == rhs);
}

// NB(levysotsky): We don't use TYsonSerializable here
// because we want to mirror the TAccessControlList structure,
// and a vector of TYsonSerializable-s cannot be declared (as it has no move constructor).
void Serialize(const TSerializableAccessControlEntry& ace, NYson::IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer)
        .BeginMap()
            .Item("action").Value(ace.Action)
            .Item("subjects").Value(ace.Subjects)
            .Item("permissions").Value(ace.Permissions)
            .Item("inheritance_mode").Value(ace.InheritanceMode)
            .DoIf(ace.Columns.has_value(), [&] (auto fluent) {
                fluent
                    .Item("columns").Value(ace.Columns);
            })
        .EndMap();
}

void Deserialize(TSerializableAccessControlEntry& ace, NYTree::INodePtr node)
{
    using NYTree::Deserialize;

    auto mapNode = node->AsMap();

    Deserialize(ace.Action, mapNode->GetChild("action"));
    Deserialize(ace.Subjects, mapNode->GetChild("subjects"));
    Deserialize(ace.Permissions, mapNode->GetChild("permissions"));
    if (auto inheritanceModeNode = mapNode->FindChild("inheritance_mode")) {
        Deserialize(ace.InheritanceMode, inheritanceModeNode);
    }
    if (auto columnsNode = mapNode->FindChild("columns")) {
        Deserialize(ace.Columns, columnsNode);
    }

    if (ace.Action == ESecurityAction::Undefined) {
        THROW_ERROR_EXCEPTION("%Qlv action is not allowed",
            ESecurityAction::Undefined);
    }

    if (ace.Columns) {
        for (auto permission : TEnumTraits<EPermission>::GetDomainValues()) {
            if (Any(ace.Permissions & permission) && permission != EPermission::Read) {
                THROW_ERROR_EXCEPTION("Columnar ACE cannot contain %Qlv permission",
                    permission);
            }
        }
    }
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

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSecurityClient
