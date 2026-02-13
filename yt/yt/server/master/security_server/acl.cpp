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

#include <library/cpp/yt/yson/public.h>

namespace NYT::NSecurityServer {

using namespace NCellMaster;
using namespace NLogging;
using namespace NObjectServer;
using namespace NSecurityClient;
using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

constinit const auto Logger = SecurityServerLogger;

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
    using NCellMaster::EMasterReign;
    using NYT::Persist;

    Persist(context, Subjects);
    Persist(context, Permissions);
    Persist(context, Action);
    Persist(context, InheritanceMode);
    Persist(context, SubjectTagFilter);
    Persist(context, Columns);
    Persist(context, Vital);
    if (context.GetVersion() >= EMasterReign::RowLevelSecurity) {
        Persist(context, RowAccessPredicate);
        Persist(context, InapplicableRowAccessPredicateMode);
    }
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
    Persist(context, RowAccessPredicate);
    Persist(context, InapplicableRowAccessPredicateMode);
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

    if (RowAccessPredicate != rhs.RowAccessPredicate) {
        return false;
    }

    if (InapplicableRowAccessPredicateMode != rhs.InapplicableRowAccessPredicateMode) {
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
            .OptionalItem(TSerializableAccessControlEntry::RowAccessPredicateKey, ace.RowAccessPredicate)
            .OptionalItem(TSerializableAccessControlEntry::InapplicableRowAccessPredicateModeKey, ace.InapplicableRowAccessPredicateMode)
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
    std::vector<std::string>* missingSubjects,
    std::vector<std::string>* pendingRemovalSubjects)
{
    auto serializableAcl = ConvertTo<TSerializableAccessControlList>(node);
    std::vector<std::string> gatheredMissingSubjects;
    std::vector<std::string> gatheredPendingRemovalSubjects;
    for (const auto& serializableAce : serializableAcl.Entries) {
        TAccessControlEntry ace;

        // Action
        ace.Action = serializableAce.Action;

        // Subject
        for (const auto& name : serializableAce.Subjects) {
            auto* subject = securityManager->FindSubjectByNameOrAlias(name, true /*activeLifeStageOnly*/);
            if (!IsObjectAlive(subject)) {
                gatheredMissingSubjects.emplace_back(name);
                continue;
            }
            if (subject->IsUser() && subject->AsUser()->GetPendingRemoval()) {
                gatheredPendingRemovalSubjects.emplace_back(name);
            }

            ace.Subjects.push_back(subject);
        }

        // Permissions
        ace.Permissions = serializableAce.Permissions;

        // Inheritance mode
        ace.InheritanceMode = serializableAce.InheritanceMode;

        // SubjectTagFilter
        ace.SubjectTagFilter = serializableAce.SubjectTagFilter;

        // Columns
        ace.Columns = std::move(serializableAce.Columns);

        // Vital
        ace.Vital = serializableAce.Vital;

        ace.RowAccessPredicate = serializableAce.RowAccessPredicate;
        ace.InapplicableRowAccessPredicateMode = serializableAce.InapplicableRowAccessPredicateMode;

        acl.Entries.push_back(ace);
    }

    if (!gatheredMissingSubjects.empty()) {
        SortUnique(gatheredMissingSubjects);

        if (missingSubjects) {
            *missingSubjects = std::move(gatheredMissingSubjects);
        } else {
            // NB: Missing subjects should be listed inside the error message
            // (not attributes) lest the scheduler conflates distinct errors.
            // See TError::GetSkeleton.
            THROW_ERROR_EXCEPTION("Some subjects mentioned in ACL are missing: %v",
                gatheredMissingSubjects);
        }
    }

    if (!gatheredPendingRemovalSubjects.empty()) {
        SortUnique(gatheredPendingRemovalSubjects);

        if (pendingRemovalSubjects) {
            *pendingRemovalSubjects = std::move(gatheredPendingRemovalSubjects);
        } else {
            YT_LOG_ALERT(
                "Some subjects mentioned in ACL are pending removal (PendingRemovalSubjects: %v)",
                gatheredPendingRemovalSubjects);
        }
    }

    securityManager->ValidateAclSubjectTagFilters(acl);
}

TAccessControlList DeserializeAclOrThrow(
    const INodePtr& node,
    const ISecurityManagerPtr& securityManager)
{
    TAccessControlList result;
    DoDeserializeAclOrThrow(
        result,
        node,
        securityManager,
        /*missingSubjects*/ nullptr,
        /*pendingRemovalSubjects*/ nullptr);
    return result;
}

TValidatedAccessControlList DeserializeAclGatherMissingAndPendingRemovalSubjectsOrThrow(
    const INodePtr& node,
    const ISecurityManagerPtr& securityManager,
    bool ignoreMissingSubjects,
    bool ignorePendingRemovalSubjects)
{
    TValidatedAccessControlList result;
    DoDeserializeAclOrThrow(
        result.Acl,
        node,
        securityManager,
        ignoreMissingSubjects ? &result.MissingSubjects : nullptr,
        ignorePendingRemovalSubjects ? &result.PendingRemovalSubjects : nullptr);
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
        DoDeserializeAclOrThrow(
            result,
            node,
            securityManager,
            /*missingSubjects*/ nullptr,
            /*pendingRemovalSubjects*/ nullptr);
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
    for (auto subject : ace.Subjects) {
        subject->LinkObject(Object_);
    }
    Acl_.Entries.push_back(ace);
}

void TAccessControlDescriptor::ClearEntries()
{
    for (const auto& ace : Acl_.Entries) {
        for (auto subject : ace.Subjects) {
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

namespace NDetail {

TMutableAccessControlDescriptorPtr::TMutableAccessControlDescriptorPtr(
    TAccessControlDescriptor* acd,
    ISecurityManager* securityManager)
    : Underlying_(acd)
    , SecurityManager_(securityManager)
{ }

TAccessControlDescriptor* TMutableAccessControlDescriptorPtr::operator->()
{
    Modified_ = true;
    return Underlying_;
}

TMutableAccessControlDescriptorPtr::~TMutableAccessControlDescriptorPtr()
{
    if (Modified_) {
        SecurityManager_->OnObjectAcdUpdated(Underlying_);
    }
}

TMutableAccessControlDescriptorPtr::operator bool() const
{
    return Underlying_;
}

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

TWrappedAccessControlDescriptorPtr::TWrappedAccessControlDescriptorPtr(
    TAccessControlDescriptor* acd,
    ISecurityManager* securityManager)
    : Underlying_(acd)
    , SecurityManager_(securityManager)
{ }

const TAccessControlDescriptor* TWrappedAccessControlDescriptorPtr::operator->() const
{
    return Underlying_;
}

NDetail::TMutableAccessControlDescriptorPtr TWrappedAccessControlDescriptorPtr::AsMutable()
{
    YT_VERIFY(NHydra::HasHydraContext());
    return NDetail::TMutableAccessControlDescriptorPtr(Underlying_, SecurityManager_);
}

TWrappedAccessControlDescriptorPtr::operator bool() const
{
    return Underlying_;
}

TAccessControlDescriptor* TWrappedAccessControlDescriptorPtr::Underlying()
{
    return Underlying_;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSecurityServer
