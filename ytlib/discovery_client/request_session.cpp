#include "request_session.h"
#include "discovery_client_service_proxy.h"
#include "helpers.h"

#include <yt/core/misc/public.h>

#include <yt/core/rpc/public.h>
#include <yt/core/rpc/retrying_channel.h>

#include <yt/core/concurrency/delayed_executor.h>

namespace NYT::NDiscoveryClient {

using namespace NConcurrency;
using namespace NRpc;
using namespace NYTree;

using NYT::ToProto;
using NYT::FromProto;

////////////////////////////////////////////////////////////////////////////////

TDiscoveryClientServiceProxy CreateProxy(
    const TDiscoveryClientConfigPtr& config,
    const IChannelFactoryPtr& channelFactory,
    const TString& address)
{
    auto channel = channelFactory->CreateChannel(address);
    TDiscoveryClientServiceProxy proxy(CreateRetryingChannel(config, std::move(channel)));
    proxy.SetDefaultTimeout(config->RpcTimeout);
    return proxy;
}

////////////////////////////////////////////////////////////////////////////////

TListGroupsRequestSession::TListGroupsRequestSession(
    TServerAddressPoolPtr addressPool,
    TDiscoveryClientConfigPtr config,
    IChannelFactoryPtr channelFactory,
    const NLogging::TLogger& logger)
    : TRequestSession<std::vector<TString>>(
        config->ReadQuorum,
        std::move(addressPool),
        logger)
    , Config_(std::move(config))
    , ChannelFactory_(std::move(channelFactory))
{ }

TFuture<void> TListGroupsRequestSession::MakeRequest(const TString& address)
{
    auto proxy = CreateProxy(Config_, ChannelFactory_, address);

    auto req = proxy.ListGroups();
    return req->Invoke().Apply(BIND([=, this_ = MakeStrong(this)] (const TDiscoveryClientServiceProxy::TRspListGroupsPtr& rsp) {
        auto rspGroups = FromProto<std::vector<TString>>(rsp->groups());
        TGuard guard(Lock_);
        GroupIds_.insert(rspGroups.begin(), rspGroups.end());
        if (++SuccessCount_ == RequiredSuccessCount_) {
            std::vector<TString> result{GroupIds_.begin(), GroupIds_.end()};
            guard.Release();
            Promise_.TrySet(std::move(result));
        }
    }));
}

////////////////////////////////////////////////////////////////////////////////

TListMembersRequestSession::TListMembersRequestSession(
    TServerAddressPoolPtr addressPool,
    TDiscoveryClientConfigPtr config,
    IChannelFactoryPtr channelFactory,
    const NLogging::TLogger& logger,
    TGroupId groupId,
    TListMembersOptions options)
    : TRequestSession<std::vector<TMemberInfo>>(
        config->ReadQuorum,
        std::move(addressPool),
        logger)
    , Config_(std::move(config))
    , ChannelFactory_(std::move(channelFactory))
    , GroupId_(std::move(groupId))
    , Options_(std::move(options))
{ }

TFuture<void> TListMembersRequestSession::MakeRequest(const TString& address)
{
    auto proxy = CreateProxy(Config_, ChannelFactory_, address);

    auto req = proxy.ListMembers();
    req->set_group_id(GroupId_);
    ToProto(req->mutable_options(), Options_);

    return req->Invoke().Apply(BIND([=, this_ = MakeStrong(this)] (const TDiscoveryClientServiceProxy::TRspListMembersPtr& rsp) {
        TGuard guard(Lock_);
        for (const auto& protoMemberInfo : rsp->members()) {
            auto member = FromProto<TMemberInfo>(protoMemberInfo);
            if (auto it = IdToMember_.find(member.Id); it == IdToMember_.end()) {
                YT_VERIFY(IdToMember_.emplace(member.Id, std::move(member)).second);
            } else if (it->second.Revision < member.Revision) {
                it->second = std::move(member);
            }
        }
        if (++SuccessCount_ == RequiredSuccessCount_) {
            std::vector<TMemberInfo> members;
            for (auto& [id, member] : IdToMember_) {
                members.emplace_back(std::move(member));
            }
            guard.Release();
            std::sort(members.begin(), members.end(), [] (const TMemberInfo& lhs, const TMemberInfo& rhs) {
                if (lhs.Priority != rhs.Priority) {
                    return lhs.Priority < rhs.Priority;
                }
                return lhs.Id < rhs.Id;
            });
            Promise_.TrySet(std::move(members));
        }
    }));
}

////////////////////////////////////////////////////////////////////////////////

TGetGroupSizeRequestSession::TGetGroupSizeRequestSession(
    TServerAddressPoolPtr addressPool,
    TDiscoveryClientConfigPtr config,
    IChannelFactoryPtr channelFactory,
    const NLogging::TLogger& logger,
    TString groupId)
    : TRequestSession<int>(
        config->ReadQuorum,
        std::move(addressPool),
        logger)
    , Config_(std::move(config))
    , ChannelFactory_(std::move(channelFactory))
    , GroupId_(std::move(groupId))
{ }

TFuture<void> TGetGroupSizeRequestSession::MakeRequest(const TString& address)
{
    auto proxy = CreateProxy(Config_, ChannelFactory_, address);

    auto req = proxy.GetGroupSize();
    req->set_group_id(GroupId_);
    return req->Invoke().Apply(BIND([=, this_ = MakeStrong(this)] (const TDiscoveryClientServiceProxy::TRspGetGroupSizePtr& rsp) {
        TGuard guard(Lock_);

        auto receivedGroupSize = rsp->group_size();
        GroupSize_ = std::max(GroupSize_, receivedGroupSize);

        if (++SuccessCount_ == RequiredSuccessCount_) {
            guard.Release();
            Promise_.TrySet(GroupSize_);
        }
    }));
}

////////////////////////////////////////////////////////////////////////////////

THeartbeatSession::THeartbeatSession(
    TServerAddressPoolPtr addressPool,
    TMemberClientConfigPtr config,
    IChannelFactoryPtr channelFactory,
    const NLogging::TLogger& logger,
    TGroupId groupId,
    TMemberId memberId,
    i64 priority,
    i64 revision,
    std::unique_ptr<IAttributeDictionary> attributes)
    : TRequestSession<void>(
        config->WriteQuorum,
        std::move(addressPool),
        logger)
    , Config_(std::move(config))
    , ChannelFactory_(std::move(channelFactory))
    , GroupId_(std::move(groupId))
    , MemberId_(std::move(memberId))
    , Priority_(priority)
    , Revision_(revision)
    , Attributes_(std::move(attributes))
{ }

TFuture<void> THeartbeatSession::MakeRequest(const TString& address)
{
    auto channel = ChannelFactory_->CreateChannel(address);
    TDiscoveryClientServiceProxy proxy(std::move(channel));
    proxy.SetDefaultTimeout(Config_->RpcTimeout);

    auto req = proxy.Heartbeat();

    req->set_group_id(GroupId_);
    auto* protoMemberInfo = req->mutable_member_info();
    protoMemberInfo->set_id(MemberId_);
    protoMemberInfo->set_priority(Priority_);
    protoMemberInfo->set_revision(Revision_);
    if (Attributes_) {
        ToProto(protoMemberInfo->mutable_attributes(), *Attributes_);
    }
    req->set_lease_timeout(ToProto<i64>(Config_->LeaseTimeout));

    return req->Invoke().Apply(BIND([=, this_ = MakeStrong(this)] (const TDiscoveryClientServiceProxy::TRspHeartbeatPtr& rsp) {
        if (++SuccessCount_ == RequiredSuccessCount_) {
            Promise_.TrySet();
        }
    }));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDiscoveryClient
