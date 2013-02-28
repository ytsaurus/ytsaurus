#include "stdafx.h"
#include "acl.h"
#include "subject.h"
#include "security_manager.h"

#include <ytlib/ytree/fluent.h>
#include <ytlib/ytree/node.h>
#include <ytlib/ytree/yson_serializable.h>

#include <ytlib/ytree/permission.h>

#include <server/cell_master/serialization_context.h>

namespace NYT {
namespace NSecurityServer {

using namespace NCellMaster;
using namespace NYTree;
using namespace NYson;
using namespace NSecurityClient;
using namespace NObjectServer;

////////////////////////////////////////////////////////////////////////////////

TAccessControlEntry::TAccessControlEntry()
    : Action(ESecurityAction::Undefined)
{ }

TAccessControlEntry::TAccessControlEntry(
    ESecurityAction action,
    TSubject* subject,
    EPermissionSet permissions)
{
    Action = action;
    Subjects.push_back(subject);
    Permissions = permissions;
}

void Load(const TLoadContext& context, TAccessControlEntry& entry)
{
    auto* input = context.GetInput();
    LoadObjectRefs(context, entry.Subjects);
    Load(input, entry.Permissions);
    Load(input, entry.Action);
}

void Save(const TSaveContext& context, const TAccessControlEntry& entry)
{
    auto* output = context.GetOutput();
    SaveObjectRefs(context, entry.Subjects);
    Save(output, entry.Permissions);
    Save(output, entry.Action);
}

void Serialize(const TAccessControlEntry& ace, IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer)
        .BeginMap()
            .Item("action").Value(ace.Action)
            .Item("subjects").DoListFor(ace.Subjects, [] (TFluentList fluent, TSubject* subject) {
                fluent.Item().Value(subject->GetName());
            })
            .Item("permissions").Value(FormatPermissions(ace.Permissions))
        .EndMap();
}

////////////////////////////////////////////////////////////////////////////////

void Load(const TLoadContext& context, TAccessControlList& acl)
{
    Load(context, acl.Entries);
}

void Save(const TSaveContext& context, const TAccessControlList& acl)
{
    Save(context, acl.Entries);
}

void Serialize(const TAccessControlList& acl, IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer)
        .Value(acl.Entries);
}

struct TSerializableAccessControlEntry
    : public TYsonSerializable
{
    ESecurityAction Action;
    std::vector<Stroka> Subjects;
    std::vector<Stroka> Permissions;

    TSerializableAccessControlEntry()
    {
        Register("action", Action);
        Register("subjects", Subjects);
        Register("permissions", Permissions);
    }
};

typedef TIntrusivePtr<TSerializableAccessControlEntry> TSerializableAccessControlEntryPtr;

void Deserilize(
    TAccessControlList& acl,
    EPermissionSet supportedPermissions,
    INodePtr node,
    TSecurityManagerPtr securityManager)
{
    auto serializableAces = ConvertTo< std::vector<TSerializableAccessControlEntryPtr> >(node);
    FOREACH (const auto& serializableAce, serializableAces) {
        TAccessControlEntry ace;

        // Action
        ace.Action = serializableAce->Action;

        // Subject
        FOREACH (const auto& name, serializableAce->Subjects) {
            auto* subject = securityManager->FindSubjectByName(name);
            if (!subject) {
                THROW_ERROR_EXCEPTION("No such subject: %s", ~name);
            }
            ace.Subjects.push_back(subject);
        }
         
        // Permissions
        ace.Permissions = ParsePermissions(serializableAce->Permissions, supportedPermissions);

        acl.Entries.push_back(ace);
    }
}

////////////////////////////////////////////////////////////////////////////////

TAccessControlDescriptor::TAccessControlDescriptor(TObjectBase* object)
    : Inherit_(true)
    , Object_(object)
{ }

void TAccessControlDescriptor::AddEntry(const TAccessControlEntry& ace)
{
    FOREACH (auto* subject, ace.Subjects) {
        subject->ReferencingObjects().insert(Object_);
    }
    Acl_.Entries.push_back(ace);
}

void TAccessControlDescriptor::ClearEntries()
{
    FOREACH (const auto& ace, Acl_.Entries) {
        FOREACH (auto* subject, ace.Subjects) {
            YCHECK(subject->ReferencingObjects().erase(Object_) == 1);
        }
    }
    Acl_.Entries.clear();
}

void TAccessControlDescriptor::PurgeEntries(TSubject* subject)
{
    // Remove the subject from every ACE.
    FOREACH (auto& ace, Acl_.Entries) {
        auto it = std::remove_if(
            ace.Subjects.begin(),
            ace.Subjects.end(),
            [=] (TSubject* aceSubject) {
                return subject == aceSubject;
            });
        ace.Subjects.erase(it, ace.Subjects.end());
    }

    // Remove all empty ACEs.
    {
        auto it = std::remove_if(
            Acl_.Entries.begin(),
            Acl_.Entries.end(),
            [=] (const TAccessControlEntry& ace) {
                return ace.Subjects.empty();
            });
        Acl_.Entries.erase(it, Acl_.Entries.end());
    }
}

void Load(const TLoadContext& context, TAccessControlDescriptor& acd)
{
    auto* input = context.GetInput();
    Load(context, acd.Acl_);
    Load(input, acd.Inherit_);
}

void Save(const TSaveContext& context, const TAccessControlDescriptor& acd)
{
    auto* output = context.GetOutput();
    Save(context, acd.Acl_);
    Save(output, acd.Inherit_);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NSecurityServer
} // namespace NYT

