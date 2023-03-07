#include "acl.h"

#include <yt/core/yson/pull_parser_deserialize.h>

#include <yt/core/ytree/fluent.h>
#include <yt/core/ytree/serialize.h>

namespace NYT::NSecurityClient {

using namespace NYTree;
using namespace NYson;

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
            .OptionalItem("columns", ace.Columns)
        .EndMap();
}

static void EnsureCorrect(const TSerializableAccessControlEntry& ace)
{
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

void Deserialize(TSerializableAccessControlEntry& ace, NYTree::INodePtr node)
{
    using NYTree::Deserialize;

    auto mapNode = node->AsMap();

    Deserialize(ace.Action, mapNode->GetChild("action"));
    Deserialize(ace.Subjects, mapNode->GetChild("subjects"));
    Deserialize(ace.Permissions, mapNode->GetChild("permissions"));
    if (auto inheritanceModeNode = mapNode->FindChild("inheritance_mode")) {
        Deserialize(ace.InheritanceMode, inheritanceModeNode);
    } else {
        ace.InheritanceMode = EAceInheritanceMode::ObjectAndDescendants;
    }
    if (auto columnsNode = mapNode->FindChild("columns")) {
        Deserialize(ace.Columns, columnsNode);
    } else {
        ace.Columns.reset();
    }
    EnsureCorrect(ace);
}

void Deserialize(TSerializableAccessControlEntry& ace, NYson::TYsonPullParserCursor* cursor)
{
    auto HasAction = false;
    auto HasSubjects = false;
    auto HasPermissions = false;
    ace.InheritanceMode = EAceInheritanceMode::ObjectAndDescendants;
    cursor->ParseMap([&] (TYsonPullParserCursor* cursor) {
        auto key = cursor->GetCurrent().UncheckedAsString();
        if (key == AsStringBuf("action")) {
            cursor->Next();
            HasAction = true;
            Deserialize(ace.Action, cursor);
        } else if (key == AsStringBuf("subjects")) {
            cursor->Next();
            HasSubjects = true;
            Deserialize(ace.Subjects, cursor);
        } else if (key == AsStringBuf("permissions")) {
            cursor->Next();
            HasPermissions = true;
            Deserialize(ace.Permissions, cursor);
        } else if (key == AsStringBuf("inheritance_mode")) {
            cursor->Next();
            Deserialize(ace.InheritanceMode, cursor);
        } else if (key == AsStringBuf("columns")) {
            cursor->Next();
            Deserialize(ace.Columns, cursor);
        } else {
            cursor->Next();
            cursor->SkipComplexValue();
        }
    });
    if (!(HasAction && HasSubjects && HasPermissions)) {
        THROW_ERROR_EXCEPTION("Error parsing ACE: \"action\", \"subject\" and \"permissions\" fields are required");
    }
    EnsureCorrect(ace);
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

void Deserialize(TSerializableAccessControlList& acl, NYson::TYsonPullParserCursor* cursor)
{
    Deserialize(acl.Entries, cursor);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSecurityClient
