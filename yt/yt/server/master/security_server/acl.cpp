#include "acl.h"
#include "private.h"
#include "security_manager.h"
#include "subject.h"

#include <yt/yt/server/master/cell_master/serialize.h>

#include <yt/yt/server/master/cypress_server/serialize.h>

#include <yt/yt/client/security_client/acl.h>

#include <yt/yt/core/ytree/fluent.h>
#include <yt/yt/core/ytree/node.h>
#include <yt/yt/core/ytree/permission.h>
#include <yt/yt/core/ytree/yson_serializable.h>

namespace NYT::NSecurityServer {

using namespace NCellMaster;
using namespace NLogging;
using namespace NObjectServer;
using namespace NSecurityClient;
using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = SecurityServerLogger;

////////////////////////////////////////////////////////////////////////////////

TAccessControlEntry::TAccessControlEntry()
    : Action(ESecurityAction::Undefined)
    , InheritanceMode(EAceInheritanceMode::ObjectAndDescendants)
{ }

TAccessControlEntry::TAccessControlEntry(
    ESecurityAction action,
    TSubject* subject,
    EPermissionSet permissions,
    EAceInheritanceMode inheritanceMode)
    : Action(action)
    , Subjects{subject}
    , Permissions(permissions)
    , InheritanceMode(inheritanceMode)
{ }

void TAccessControlEntry::Persist(const NCellMaster::TPersistenceContext& context)
{
    using NYT::Persist;
    Persist(context, Subjects);
    Persist(context, Permissions);
    Persist(context, Action);
    Persist(context, InheritanceMode);
    // COMPAT(vovamelnikov)
    if (context.IsLoad() && context.GetVersion() < EMasterReign::AttributeBasedAccessControl) {
        SubjectTagFilter = std::nullopt;
    } else {
        Persist(context, SubjectTagFilter);
    }
    Persist(context, Columns);
    Persist(context, Vital);
}

void TAccessControlEntry::Persist(const NCypressServer::TCopyPersistenceContext& context)
{
    using NYT::Persist;
    Persist(context, Subjects);
    Persist(context, Permissions);
    Persist(context, Action);
    Persist(context, InheritanceMode);
    Persist(context, SubjectTagFilter);
    Persist(context, Columns);
    Persist(context, Vital);
}

bool TAccessControlEntry::operator==(const TAccessControlEntry& rhs) const
{
    if (Action != rhs.Action) {
        return false;
    }

    if (Permissions != rhs.Permissions) {
        return false;
    }

    if (InheritanceMode != rhs.InheritanceMode) {
        return false;
    }

    if (SubjectTagFilter != rhs.SubjectTagFilter) {
        return false;
    }

    if (Columns != rhs.Columns) {
        return false;
    }

    if (Vital != rhs.Vital) {
        return false;
    }

    auto lhsSubjects = Subjects;
    std::sort(lhsSubjects.begin(), lhsSubjects.end(), TObjectIdComparer());
    auto rhsSubjects = rhs.Subjects;
    std::sort(rhsSubjects.begin(), rhsSubjects.end(), TObjectIdComparer());
    return lhsSubjects == rhsSubjects;
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
            .Item("inheritance_mode").Value(ace.InheritanceMode)
            .OptionalItem("subject_tag_filter", ace.SubjectTagFilter)
            .OptionalItem("columns", ace.Columns)
            .OptionalItem("vital", ace.Vital)
        .EndMap();
}

////////////////////////////////////////////////////////////////////////////////

void TAccessControlList::Persist(const NCellMaster::TPersistenceContext& context)
{
    using NYT::Persist;
    Persist(context, Entries);
}

void TAccessControlList::Persist(const NCypressServer::TCopyPersistenceContext& context)
{
    using NYT::Persist;
    Persist(context, Entries);
}

void Serialize(const TAccessControlList& acl, IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer)
        .Value(acl.Entries);
}

static void DoDeserializeAclOrThrow(
    TAccessControlList& acl,
    const INodePtr& node,
    const ISecurityManagerPtr& securityManager,
    std::vector<TString>* missingSubjects)
{
    auto serializableAcl = ConvertTo<TSerializableAccessControlList>(node);
    std::vector<TString> tmpMissingSubjects;
    for (const auto& serializableAce : serializableAcl.Entries) {
        TAccessControlEntry ace;

        // Action
        ace.Action = serializableAce.Action;

        // Subject
        for (const auto& name : serializableAce.Subjects) {
            auto* subject = securityManager->FindSubjectByNameOrAlias(name, true /*activeLifeStageOnly*/);
            if (!IsObjectAlive(subject)) {
                tmpMissingSubjects.emplace_back(name);
                continue;
            }

            ace.Subjects.push_back(subject);
        }

        // Permissions
        ace.Permissions = serializableAce.Permissions;

        // Inheritance mode
        ace.InheritanceMode = serializableAce.InheritanceMode;

        // SubjectTagFilter
        if (serializableAce.SubjectTagFilter) {
            ace.SubjectTagFilter = MakeBooleanFormula(serializableAce.SubjectTagFilter);
        } else {
            ace.SubjectTagFilter.reset();
        }

        // Columns
        ace.Columns = std::move(serializableAce.Columns);

        // Vital
        ace.Vital = serializableAce.Vital;

        acl.Entries.push_back(ace);
    }

    if (!tmpMissingSubjects.empty()) {
        if (missingSubjects) {
            *missingSubjects = std::move(tmpMissingSubjects);
        } else {
            THROW_ERROR_EXCEPTION("Some subjects mentioned in ACL are missing")
                << TErrorAttribute("missing_subjects", tmpMissingSubjects);
        }
    }

    securityManager->ValidateAclSubjectTagFilters(acl);
}

TAccessControlList DeserializeAclOrThrow(
    const INodePtr& node,
    const ISecurityManagerPtr& securityManager)
{
    TAccessControlList result;
    DoDeserializeAclOrThrow(result, node, securityManager, nullptr);
    return result;
}

std::pair<TAccessControlList, std::vector<TString>>
DeserializeAclGatherMissingSubjectsOrThrow(
    const INodePtr& node,
    const ISecurityManagerPtr& securityManager)
{
    std::pair<TAccessControlList, std::vector<TString>> result;
    DoDeserializeAclOrThrow(result.first, node, securityManager, &result.second);
    return result;
}

TAccessControlList DeserializeAclOrAlert(
    const INodePtr& node,
    const ISecurityManagerPtr& securityManager)
{
    TAccessControlList result;
    // NB: It's not really feasible to invert things here and avoid exceptions
    // being thrown: at the very least, ConvertTo may throw any number of them.
    // This is how yson deserialization framework is designed.
    try {
        DoDeserializeAclOrThrow(result, node, securityManager, nullptr);
    } catch (const std::exception& error) {
        YT_LOG_ALERT(error, "Error deserializing ACL");
    }

    return result;
}

////////////////////////////////////////////////////////////////////////////////

TAccessControlDescriptor::TAccessControlDescriptor(TObject* object)
    : Object_(object)
{ }

void TAccessControlDescriptor::SetInherit(bool inherit)
{
    Inherit_ = inherit;
}

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

void TAccessControlDescriptor::SetEntries(const TAccessControlList& acl)
{
    ClearEntries();
    for (const auto& ace : acl.Entries) {
        AddEntry(ace);
    }
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
            [] (const TAccessControlEntry& ace) {
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

void TAccessControlDescriptor::Persist(const NCellMaster::TPersistenceContext& context)
{
    using NYT::Persist;
    Persist(context, Acl_);
    Persist(context, Inherit_);
    Persist(context, Owner_);
}

void TAccessControlDescriptor::Persist(const NCypressServer::TCopyPersistenceContext& context)
{
    using NYT::Persist;
    Persist(context, Acl_);
    Persist(context, Inherit_);
    Persist(context, Owner_);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSecurityServer

