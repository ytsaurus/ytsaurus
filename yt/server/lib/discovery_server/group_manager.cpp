#include "group.h"
#include "group_manager.h"
#include "public.h"

#include <yt/core/concurrency/rw_spinlock.h>

#include <yt/core/rpc/server.h>

#include <yt/core/ytree/helpers.h>

namespace NYT::NDiscoveryServer {

using namespace NConcurrency;
using namespace NRpc;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TGroupManager::TGroupManager(
    const NLogging::TLogger& logger)
    : Logger(logger)
{ }

THashMap<TGroupId, TGroupPtr> TGroupManager::GetOrCreateGroups(const std::vector<TGroupId>& groupIds)
{
    THashMap<TGroupId, TGroupPtr> result;

    {
        TReaderGuard readerGuard(GroupsLock_);
        for (const auto& groupId : groupIds) {
            auto it = IdToGroup_.find(groupId);
            if (it != IdToGroup_.end()) {
                result.emplace(groupId, it->second);
            }
        }
    }

    {
        TWriterGuard writerGuard(GroupsLock_);
        for (const auto& groupId : groupIds) {
            if (result.contains(groupId)) {
                continue;
            }
            auto onGroupEmptied = BIND([=, this_ = MakeStrong(this)] {
                TWriterGuard guard(GroupsLock_);
                auto it = IdToGroup_.find(groupId);
                if (it == IdToGroup_.end()) {
                    YT_LOG_WARNING("Empty group is already deleted (GroupId: %v)", groupId);
                    return;
                }
                IdToGroup_.erase(it);
            });
            auto group = New<TGroup>(groupId, onGroupEmptied, Logger);
            IdToGroup_.emplace(groupId, group);
            result.emplace(groupId, group);
        }
    }
    return result;
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

    auto group = GetOrCrash(GetOrCreateGroups({groupId}), groupId);
    auto member = group->AddOrUpdateMember(memberInfo, leaseTimeout);

    {
        TGuard guard(ModifiedMembersLock_);
        ModifiedMembers_.insert(member);
    }
}

TGroupPtr TGroupManager::FindGroup(const TGroupId& id)
{
    TReaderGuard guard(GroupsLock_);
    auto it = IdToGroup_.find(id);
    if (it == IdToGroup_.end()) {
        return nullptr;
    }
    return it->second;
}

TGroupPtr TGroupManager::GetGroupOrThrow(const TGroupId& id)
{
    auto group = FindGroup(id);
    if (!group) {
        THROW_ERROR_EXCEPTION("No such group %Qv", id);
    }
    return group;
}

std::vector<TGroupPtr> TGroupManager::ListGroups()
{
    TReaderGuard guard(GroupsLock_);
    return GetValues(IdToGroup_);
}

THashSet<TMemberPtr> TGroupManager::GetModifiedMembers()
{
    THashSet<TMemberPtr> modifiedMembers;
    {
        TGuard guard(ModifiedMembersLock_);
        ModifiedMembers_.swap(modifiedMembers);
    }
    return modifiedMembers;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDiscoveryServer
