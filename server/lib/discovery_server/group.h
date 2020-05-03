#pragma once

#include "public.h"
#include "member.h"

#include <util/generic/set.h>

#include <yt/core/ytree/attributes.h>

#include <yt/ytlib/discovery_client/helpers.h>

namespace NYT::NDiscoveryServer {

////////////////////////////////////////////////////////////////////////////////

class TGroup
    : public TIntrinsicRefCounted
{
public:
    TGroup(
        const TGroupId& id,
        TClosure onGroupEmpty,
        const NLogging::TLogger& Logger);

    const TGroupId& GetId();
    int GetMemberCount();

    TMemberPtr AddOrUpdateMember(const NDiscoveryClient::TMemberInfo& memberInfo, TDuration leaseTimeout);
    std::vector<TMemberPtr> ListMembers(std::optional<int> limit = std::nullopt);
    bool HasMember(const TMemberId& memberId);
    TMemberPtr FindMember(const TMemberId& memberId);
    TMemberPtr GetMemberOrThrow(const TMemberId& memberId);

private:
    const TGroupId Id_;
    const TClosure OnGroupEmptied_;
    const NLogging::TLogger Logger;

    NConcurrency::TReaderWriterSpinLock MembersLock_;
    TSet<TMemberPtr, TMemberPtrComparer> Members_;
    THashMap<TMemberId, TMemberPtr> IdToMember_;

    TMemberPtr AddMember(const NDiscoveryClient::TMemberInfo& memberInfo, TDuration leaseTimeout);
    TMemberPtr UpdateMember(const NDiscoveryClient::TMemberInfo& memberInfo, TDuration leaseTimeout);
    void UpdateMemberInfo(const TMemberPtr& member, const NDiscoveryClient::TMemberInfo& memberInfo, TDuration leaseTimeout);
};

DEFINE_REFCOUNTED_TYPE(TGroup)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDiscoveryServer
