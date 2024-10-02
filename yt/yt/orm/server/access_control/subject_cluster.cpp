#include "subject_cluster.h"

#include "helpers.h"

#include <yt/yt/core/misc/collection_helpers.h>
#include <yt/yt/core/misc/error.h>

namespace NYT::NOrm::NServer::NAccessControl {

using namespace NClient::NObjects;

////////////////////////////////////////////////////////////////////////////////

TSnapshotSubject::TSnapshotSubject(TObjectId id, TObjectTypeValue type)
    : Id_(std::move(id))
    , Type_(type)
{ }

TSnapshotUser* TSnapshotSubject::AsUser()
{
    YT_VERIFY(Type_ == TObjectTypeValues::User);
    return static_cast<TSnapshotUser*>(this);
}

TSnapshotGroup* TSnapshotSubject::AsGroup()
{
    YT_VERIFY(Type_ == TObjectTypeValues::Group);
    return static_cast<TSnapshotGroup*>(this);
}

////////////////////////////////////////////////////////////////////////////////

TSnapshotUser::TSnapshotUser(
    TObjectId id,
    bool banned,
    std::optional<i64> requestWeightRateLimit,
    std::optional<i32> requestQueueSizeLimit,
    std::optional<std::string> executionPool)
    : TSnapshotSubject(std::move(id), TObjectTypeValues::User)
    , Banned_(banned)
    , RequestWeightRateLimit_(requestWeightRateLimit)
    , RequestQueueSizeLimit_(requestQueueSizeLimit)
    , ExecutionPool_(std::move(executionPool))
{ }

////////////////////////////////////////////////////////////////////////////////

TSnapshotGroup::TSnapshotGroup(TObjectId id, std::vector<TObjectId> members)
    : TSnapshotSubject(std::move(id), TObjectTypeValues::Group)
    , Members_(std::move(members))
{ }

bool TSnapshotGroup::ContainsUser(std::string_view userId) const
{
    return RecursiveUserIds_.contains(userId);
}

////////////////////////////////////////////////////////////////////////////////

TClusterSubjectSnapshot::TClusterSubjectSnapshot()
{
    auto everyoneGroup = std::make_unique<TSnapshotGroup>(NObjects::EveryoneSubjectId);
    EveryoneGroup_ = everyoneGroup.get();
    AddSubject(std::move(everyoneGroup));
}

void TClusterSubjectSnapshot::AddSubject(std::unique_ptr<TSnapshotSubject> subjectHolder)
{
    auto* subject = subjectHolder.get();
    auto id = subject->GetId();
    if (!IdToSubject_.emplace(id, std::move(subjectHolder)).second) {
        THROW_ERROR_EXCEPTION("Duplicate subject %Qv",
            id);
    }

    if (subject->GetType() == TObjectTypeValues::User) {
        InsertOrCrash(EveryoneGroup_->RecursiveUserIds(), id);
    }
}

std::vector<TSnapshotSubject*> TClusterSubjectSnapshot::GetSubjects(TObjectTypeValue type) const
{
    std::vector<TSnapshotSubject*> result;
    for (const auto& [key, value] : IdToSubject_) {
        if (value->GetType() == type) {
            result.push_back(value.get());
        }
    }
    return result;
}

bool TClusterSubjectSnapshot::IsSuperuser(std::string_view userId) const
{
    if (userId == NRpc::RootUserName) {
        return true;
    }

    if (SuperusersGroup_ && SuperusersGroup_->ContainsUser(userId)) {
        return true;
    }

    return false;
}

TSnapshotSubject* TClusterSubjectSnapshot::FindSubject(std::string_view id) const
{
    auto it = IdToSubject_.find(id);
    return it == IdToSubject_.end() ? nullptr : it->second.get();
}

TSnapshotGroup* TClusterSubjectSnapshot::GetSuperusersGroup() const
{
    return SuperusersGroup_;
}

TSnapshotGroup* TClusterSubjectSnapshot::GetEveryoneGroup() const
{
    return EveryoneGroup_;
}

void TClusterSubjectSnapshot::Prepare()
{
    THashSet<TSnapshotGroup*> visitedGroups;
    for (const auto& pair : IdToSubject_) {
        auto* subject = pair.second.get();
        if (subject->GetType() == TObjectTypeValues::Group) {
            auto* group = subject->AsGroup();
            visitedGroups.clear();
            ComputeRecursiveUsers(group, group, &visitedGroups);
        }
    }

    {
        auto* superusersSubject = FindSubject(NObjects::SuperusersGroupId);
        if (superusersSubject) {
            if (superusersSubject->GetType() != TObjectTypeValues::Group) {
                THROW_ERROR_EXCEPTION("%Qv must be a group",
                    NObjects::SuperusersGroupId);
            }
            SuperusersGroup_ = superusersSubject->AsGroup();
        }
    }
}

std::optional<std::tuple<EAccessControlAction, TObjectId>> TClusterSubjectSnapshot::ApplyAcl(
    const TAccessControlList& acl,
    const NYPath::TYPath& attributePath,
    TAccessControlPermissionValue permission,
    std::string_view userId)
{
    std::optional<std::tuple<EAccessControlAction, TObjectId>> result;
    ForEachAttributeAce(acl, attributePath, [&] (const auto& ace) {
        if (result && std::get<0>(*result) == EAccessControlAction::Deny) {
            return;
        }
        if (auto subresult = ApplyAce(ace, permission, userId); subresult) {
            result = std::move(subresult);
        }
    });
    return result;
}

void TClusterSubjectSnapshot::ComputeRecursiveUsers(
    TSnapshotGroup* forGroup,
    TSnapshotGroup* currentGroup,
    THashSet<TSnapshotGroup*>* visitedGroups)
{
    if (!visitedGroups->insert(currentGroup).second) {
        return;
    }

    for (const auto& subjectId : currentGroup->Members()) {
        auto* subject = FindSubject(subjectId);
        if (!subject) {
            continue;
        }
        switch (subject->GetType()) {
            case TObjectTypeValues::User:
                forGroup->RecursiveUserIds().insert(subject->GetId());
                break;
            case TObjectTypeValues::Group:
                ComputeRecursiveUsers(forGroup, subject->AsGroup(), visitedGroups);
                break;
            default:
                YT_ABORT();
        }
    }
}

std::optional<std::tuple<EAccessControlAction, TObjectId>> TClusterSubjectSnapshot::ApplyAce(
    const TAccessControlEntry& ace,
    TAccessControlPermissionValue permission,
    std::string_view userId)
{
    if (!ContainsPermission(ace, permission)) {
        return std::nullopt;
    }

    for (const auto& subjectId : ace.Subjects) {
        auto* subject = FindSubject(subjectId);
        if (!subject) {
            continue;
        }

        switch (subject->GetType()) {
            case TObjectTypeValues::User:
                if (subjectId == userId) {
                    return std::tuple(ace.Action, subjectId);
                }
                break;
            case TObjectTypeValues::Group: {
                auto* group = subject->AsGroup();
                if (group->ContainsUser(userId)) {
                    return std::tuple(ace.Action, subjectId);
                }
                break;
            }
            default:
                YT_ABORT();
        }
    }

    return std::nullopt;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NAccessControl
