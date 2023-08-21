#include "group.h"
#include "group_manager.h"
#include "public.h"
#include "cypress_integration.h"

#include <yt/yt/core/rpc/server.h>

#include <yt/yt/core/ytree/helpers.h>

namespace NYT::NDiscoveryServer {

using namespace NConcurrency;
using namespace NRpc;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TGroupManager::TGroupManager(
    const NLogging::TLogger& logger)
    : Logger(logger)
    , GroupTree_(New<TGroupTree>(Logger))
    , YPathService_(CreateDiscoveryYPathService(GroupTree_))
{ }

THashMap<TGroupId, TGroupPtr> TGroupManager::GetOrCreateGroups(const std::vector<TGroupId>& groupIds)
{
    return GroupTree_->GetOrCreateGroups(groupIds);
}

void TGroupManager::ProcessGossip(const std::vector<TGossipMemberInfo>& membersBatch)
{
    YT_LOG_DEBUG("Started processing gossip");

    std::vector<TGroupId> groupIds;
    groupIds.reserve(membersBatch.size());
    for (const auto& member : membersBatch) {
        groupIds.push_back(member.GroupId);
    }

    auto groups = GetOrCreateGroups(groupIds);
    for (const auto& member : membersBatch) {
        // All group ids in gossip should be correct.
        auto group = GetOrCrash(groups, member.GroupId);
        group->AddOrUpdateMember(member.MemberInfo, member.LeaseDeadline - TInstant::Now());
    }
}

void TGroupManager::ProcessHeartbeat(
    const TGroupId& groupId,
    const NDiscoveryClient::TMemberInfo& memberInfo,
    TDuration leaseTimeout)
{
    YT_LOG_DEBUG("Started processing heartbeat (GroupId: %v, MemberId: %v)",
        groupId,
        memberInfo.Id);

    if (memberInfo.Id.empty()) {
        THROW_ERROR_EXCEPTION(NDiscoveryClient::EErrorCode::InvalidMemberId,
            "Member id should not be empty");
    }

    auto groups = GetOrCreateGroups({groupId});
    // If groupId is incorrect, GetOrCreateGroups will omit it in result groups.
    if (groups.empty()) {
        THROW_ERROR_EXCEPTION(NDiscoveryClient::EErrorCode::InvalidGroupId,
            "Group id %v is incorrect",
            groupId);
    }

    auto group = GetOrCrash(groups, groupId);
    auto member = group->AddOrUpdateMember(memberInfo, leaseTimeout);

    {
        auto guard = Guard(ModifiedMembersLock_);
        ModifiedMembers_.insert(member);
    }
}

TGroupPtr TGroupManager::FindGroup(const TGroupId& id)
{
    return GroupTree_->FindGroup(id);
}

TGroupPtr TGroupManager::GetGroupOrThrow(const TGroupId& id)
{
    auto group = FindGroup(id);
    if (!group) {
        THROW_ERROR_EXCEPTION(NDiscoveryClient::EErrorCode::NoSuchGroup,
            "No such group %v",
            id);
    }
    return group;
}

THashSet<TMemberPtr> TGroupManager::GetModifiedMembers()
{
    THashSet<TMemberPtr> modifiedMembers;
    {
        auto guard = Guard(ModifiedMembersLock_);
        ModifiedMembers_.swap(modifiedMembers);
    }
    return modifiedMembers;
}

NYTree::IYPathServicePtr TGroupManager::GetYPathService()
{
    return YPathService_;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDiscoveryServer
