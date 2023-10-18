#pragma once

#include "config.h"
#include "helpers.h"
#include "public.h"
#include "group_tree.h"

#include <yt/yt/core/rpc/server.h>

#include <yt/yt/core/concurrency/action_queue.h>

#include <yt/yt/ytlib/discovery_client/helpers.h>

#include <library/cpp/yt/threading/spin_lock.h>

namespace NYT::NDiscoveryServer {

////////////////////////////////////////////////////////////////////////////////

class TGroupManager
    : public TRefCounted
{
public:
    explicit TGroupManager(const NLogging::TLogger& logger);

    void ProcessGossip(const std::vector<TGossipMemberInfo>& membersBatch);
    void ProcessHeartbeat(
        const TGroupId& groupId,
        const NDiscoveryClient::TMemberInfo& memberInfo,
        TDuration leaseTimeout);

    TGroupPtr GetGroupOrThrow(const TGroupId& id);
    TListGroupsResult ListGroupsOrThrow(const TGroupId& prefix, const NDiscoveryClient::TListGroupsOptions& options);

    THashSet<TMemberPtr> GetModifiedMembers();

    NYTree::IYPathServicePtr GetYPathService();

private:
    const NLogging::TLogger Logger;
    const TGroupTreePtr GroupTree_;
    const NYTree::IYPathServicePtr YPathService_;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, ModifiedMembersLock_);
    THashSet<TMemberPtr> ModifiedMembers_;

    THashMap<TGroupId, TGroupPtr> GetOrCreateGroups(const std::vector<TGroupId>& groupIds);
    TGroupPtr FindGroup(const TGroupId& id);
    TListGroupsResult ListGroups(const TGroupId& prefix, const NDiscoveryClient::TListGroupsOptions& options);
};

DEFINE_REFCOUNTED_TYPE(TGroupManager)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDiscoveryServer
