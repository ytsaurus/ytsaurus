#include "stdafx.h"
#include "acl.h"
#include "subject.h"
#include "security_manager.h"

#include <core/ytree/fluent.h>
#include <core/ytree/node.h>
#include <core/ytree/yson_serializable.h>

#include <core/ytree/permission.h>

#include <server/cell_master/serialize.h>

namespace NYT {
namespace NSecurityServer {

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

void TAccessControlEntry::Save(NCellMaster::TSaveContext& context) const
{
    using NYT::Save;
    Save(context, Subjects);
    Save(context, Permissions);
    Save(context, Action);
}

void TAccessControlEntry::Load(NCellMaster::TLoadContext& context)
{
    using NYT::Load;
    Load(context, Subjects);
    Load(context, Permissions);
    Load(context, Action);
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

void Load(NCellMaster::TLoadContext& context, TAccessControlList& acl)
{
    Load(context, acl.Entries);
}

void Save(NCellMaster::TSaveContext& context, const TAccessControlList& acl)
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
        RegisterParameter("action", Action);
        RegisterParameter("subjects", Subjects);
        RegisterParameter("permissions", Permissions);
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
    for (const auto& serializableAce : serializableAces) {
        TAccessControlEntry ace;

        // Action
        ace.Action = serializableAce->Action;

        // Subject
        for (const auto& name : serializableAce->Subjects) {
            auto* subject = securityManager->FindSubjectByName(name);
            if (!IsObjectAlive(subject)) {
                THROW_ERROR_EXCEPTION("No such subject %Qv",
                    name);
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
    , Owner_(nullptr)
{ }

void TAccessControlDescriptor::Clear()
{
    ClearEntries();
    SetOwner(nullptr);
}

TSubject* TAccessControlDescriptor::GetOwner() const
{
    return Owner_;
}

void TAccessControlDescriptor::SetOwner(TSubject* owner)
{
    if (Owner_ == owner)
        return;

    if (Owner_) {
        Owner_->UnlinkObject(Object_);
    }

    Owner_ = owner;

    if (Owner_) {
        Owner_->LinkObject(Object_);
    }
}

void TAccessControlDescriptor::AddEntry(const TAccessControlEntry& ace)
{
    for (auto* subject : ace.Subjects) {
        subject->LinkObject(Object_);
    }
    Acl_.Entries.push_back(ace);
}

void TAccessControlDescriptor::ClearEntries()
{
    for (const auto& ace : Acl_.Entries) {
        for (auto* subject : ace.Subjects) {
            subject->UnlinkObject(Object_);
        }
    }
    Acl_.Entries.clear();
}

void TAccessControlDescriptor::OnSubjectDestroyed(TSubject* subject, TSubject* defaultOwner)
{
    // Remove the subject from every ACE.
    for (auto& ace : Acl_.Entries) {
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

    // Reset owner to default, if needed.
    if (Owner_ == subject) {
        Owner_ = nullptr; // prevent updating LinkedObjects
        SetOwner(defaultOwner);
    }
}

void TAccessControlDescriptor::Save(NCellMaster::TSaveContext& context) const
{
    using NYT::Save;
    Save(context, Acl_);
    Save(context, Inherit_);
    Save(context, Owner_);
}

void TAccessControlDescriptor::Load(NCellMaster::TLoadContext& context)
{
    using NYT::Load;
    Load(context, Acl_);
    Load(context, Inherit_);
    Load(context, Owner_);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NSecurityServer
} // namespace NYT

