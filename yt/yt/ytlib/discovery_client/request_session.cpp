#include "request_session.h"
#include "discovery_client_service_proxy.h"
#include "helpers.h"

#include <yt/yt/core/misc/public.h>

#include <yt/yt/core/rpc/public.h>
#include <yt/yt/core/rpc/retrying_channel.h>
#include <yt/yt/core/rpc/config.h>
#include <yt/yt/core/rpc/dispatcher.h>

#include <yt/yt/core/concurrency/delayed_executor.h>
#include <yt/yt/core/concurrency/periodic_executor.h>

#include <yt/yt/core/service_discovery/service_discovery.h>

namespace NYT::NDiscoveryClient {

using namespace NConcurrency;
using namespace NRpc;
using namespace NYTree;
using namespace NServiceDiscovery;

using NYT::ToProto;
using NYT::FromProto;

////////////////////////////////////////////////////////////////////////////////

TServerAddressPool::TServerAddressPool(
    const NLogging::TLogger& logger,
    const TDiscoveryConnectionConfigPtr& config)
    : Logger(logger)
{
    EndpointsUpdateExecutor_ = New<TPeriodicExecutor>(
        TDispatcher::Get()->GetHeavyInvoker(),
        BIND(&TServerAddressPool::UpdateEndpoints, MakeWeak(this)));
    SetConfig(config);
}

int TServerAddressPool::GetAddressCount() const
{
    return AddressCount_.load();
}

std::vector<TString> TServerAddressPool::GetUpAddresses()
{
    auto guard = Guard(Lock_);
    return {UpAddresses_.begin(), UpAddresses_.end()};
}

std::vector<TString> TServerAddressPool::GetProbationAddresses()
{
    auto guard = Guard(Lock_);
    return {ProbationAddresses_.begin(), ProbationAddresses_.end()};
}

void TServerAddressPool::BanAddress(const TString& address)
{
    {
        auto guard = Guard(Lock_);
        auto it = UpAddresses_.find(address);
        if (it == UpAddresses_.end()) {
            YT_LOG_DEBUG("Cannot ban server: server is already banned (Address: %v)", address);
            return;
        }
        UpAddresses_.erase(it);
        DownAddresses_.insert(address);
    }

    TDelayedExecutor::Submit(
        BIND(&TServerAddressPool::OnBanTimeoutExpired, MakeWeak(this), address),
        Config_->ServerBanTimeout);

    YT_LOG_DEBUG("Server banned (Address: %v)", address);
}

void TServerAddressPool::UnbanAddress(const TString& address)
{
    auto guard = Guard(Lock_);

    auto it = ProbationAddresses_.find(address);
    if (it == ProbationAddresses_.end()) {
        return;
    }
    YT_LOG_DEBUG("Server unbanned (Address: %v)", address);
    ProbationAddresses_.erase(it);
    UpAddresses_.insert(address);
}

void TServerAddressPool::SetConfig(const TDiscoveryConnectionConfigPtr& config)
{
    YT_VERIFY(!config->Addresses != !config->Endpoints);

    auto guard = Guard(Lock_);

    if (config->Addresses && (!Config_ || config->Addresses != Config_->Addresses)) {
        YT_LOG_INFO("Server address pool config changed (Addresses: %v)",
            config->Addresses);

        YT_UNUSED_FUTURE(EndpointsUpdateExecutor_->Stop());
        ResolvedAddressesFuture_ = VoidFuture;

        SetAddresses(*config->Addresses);
    } else if (config->Endpoints && (!Config_ || config->Endpoints != Config_->Endpoints)) {
        YT_LOG_INFO("Server address pool config changed (EndpointSetId: %v)",
            config->Endpoints->EndpointSetId);

        ServiceDiscovery_ = TDispatcher::Get()->GetServiceDiscovery();
        if (!ServiceDiscovery_) {
            YT_LOG_ALERT("No service discovery present");
            return;
        }

        EndpointsUpdateExecutor_->SetPeriod(config->Endpoints->UpdatePeriod);
        EndpointsUpdateExecutor_->Start();
        ResolvedAddressesFuture_ = EndpointsUpdateExecutor_->GetExecutedEvent();
        EndpointsUpdateExecutor_->ScheduleOutOfBand();
    }

    Config_ = config;
}

void TServerAddressPool::UpdateEndpoints()
{
    std::vector<TFuture<TEndpointSet>> endpointSetFutures;

    {
        auto guard = Guard(Lock_);
        for (const auto& cluster : Config_->Endpoints->Clusters) {
            endpointSetFutures.push_back(ServiceDiscovery_->ResolveEndpoints(
                cluster,
                Config_->Endpoints->EndpointSetId));
        }
    }

    AllSet(endpointSetFutures)
        .Subscribe(BIND(&TServerAddressPool::OnEndpointsResolved, MakeWeak(this), Config_->Endpoints->EndpointSetId));
}

void TServerAddressPool::OnEndpointsResolved(
    const TString& endpointSetId,
    const TErrorOr<std::vector<TErrorOr<TEndpointSet>>>& endpointSetsOrError)
{
    YT_VERIFY(endpointSetsOrError.IsOK());
    const auto& endpointSets = endpointSetsOrError.Value();

    std::vector<TString> allAddresses;
    std::vector<TError> errors;
    for (const auto& endpointSetOrError : endpointSets) {
        if (!endpointSetOrError.IsOK()) {
            errors.push_back(endpointSetOrError);
            YT_LOG_WARNING(endpointSetOrError, "Could not resolve endpoints from cluster (EndpointSetId: %v)",
                endpointSetId);
            continue;
        }

        auto addresses = AddressesFromEndpointSet(endpointSetOrError.Value());
        allAddresses.insert(allAddresses.end(), addresses.begin(), addresses.end());
    }

    if (errors.size() == endpointSets.size()) {
        YT_LOG_ERROR("Endpoints could not be resolved in any cluster (EndpointSetId: %v)",
            endpointSetId);
        return;
    }

    auto guard = Guard(Lock_);
    SetAddresses(allAddresses);
}

void TServerAddressPool::SetAddresses(const std::vector<TString>& addresses)
{
    VERIFY_SPINLOCK_AFFINITY(Lock_);

    DownAddresses_.clear();
    UpAddresses_.clear();
    ProbationAddresses_ = {addresses.begin(), addresses.end()};
    AddressCount_ = std::ssize(addresses);
}

void TServerAddressPool::OnBanTimeoutExpired(const TString& address)
{
    auto guard = Guard(Lock_);

    auto it = DownAddresses_.find(address);
    if (it == DownAddresses_.end()) {
        return;
    }

    YT_LOG_DEBUG("Server moved to probation list (Address: %v)", address);
    DownAddresses_.erase(it);
    ProbationAddresses_.insert(address);
}

TFuture<void> TServerAddressPool::GetReadyEvent() const
{
    auto guard = Guard(Lock_);
    return ResolvedAddressesFuture_;
}

////////////////////////////////////////////////////////////////////////////////

TDiscoveryClientServiceProxy CreateProxy(
    const TDiscoveryClientConfigPtr& clientConfig,
    const TDiscoveryConnectionConfigPtr& connectionConfig,
    const IChannelFactoryPtr& channelFactory,
    const TString& address)
{
    auto channel = channelFactory->CreateChannel(address);
    TDiscoveryClientServiceProxy proxy(CreateRetryingChannel(clientConfig, std::move(channel)));
    proxy.SetDefaultTimeout(connectionConfig->RpcTimeout);
    return proxy;
}

////////////////////////////////////////////////////////////////////////////////

TListMembersRequestSession::TListMembersRequestSession(
    TServerAddressPoolPtr addressPool,
    TDiscoveryConnectionConfigPtr connectionConfig,
    TDiscoveryClientConfigPtr clientConfig,
    IChannelFactoryPtr channelFactory,
    const NLogging::TLogger& logger,
    TGroupId groupId,
    TListMembersOptions options)
    : TRequestSession<std::vector<TMemberInfo>>(
        clientConfig->ReadQuorum,
        std::move(addressPool),
        logger)
    , ConnectionConfig_(std::move(connectionConfig))
    , ClientConfig_(std::move(clientConfig))
    , ChannelFactory_(std::move(channelFactory))
    , GroupId_(std::move(groupId))
    , Options_(std::move(options))
{ }

TFuture<void> TListMembersRequestSession::MakeRequest(const TString& address)
{
    auto proxy = CreateProxy(
        ClientConfig_,
        ConnectionConfig_,
        ChannelFactory_,
        address);

    auto req = proxy.ListMembers();
    req->set_group_id(GroupId_);
    ToProto(req->mutable_options(), Options_);

    return req->Invoke().Apply(BIND([this, this_ = MakeStrong(this)] (const TErrorOr<TDiscoveryClientServiceProxy::TRspListMembersPtr>& rspOrError){
        if (!rspOrError.IsOK() && !rspOrError.FindMatching(NDiscoveryClient::EErrorCode::NoSuchGroup)) {
            return TError(rspOrError);
        }
        auto guard = Guard(Lock_);
        if (rspOrError.IsOK()) {
            const auto& rsp = rspOrError.Value();
            for (const auto& protoMemberInfo : rsp->members()) {
                auto member = FromProto<TMemberInfo>(protoMemberInfo);
                if (auto it = IdToMember_.find(member.Id); it == IdToMember_.end()) {
                    YT_VERIFY(IdToMember_.emplace(member.Id, std::move(member)).second);
                } else if (it->second.Revision < member.Revision) {
                    it->second = std::move(member);
                }
            }
        }
        if (++SuccessCount_ == GetRequiredSuccessCount()) {
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

            if (members.empty()) {
                Promise_.TrySet(TError(NDiscoveryClient::EErrorCode::NoSuchGroup, "Group %Qv does not exist", GroupId_));
            } else {
                Promise_.TrySet(std::move(members));
            }
        }
        return TError();
    }));
}

////////////////////////////////////////////////////////////////////////////////

TGetGroupMetaRequestSession::TGetGroupMetaRequestSession(
    TServerAddressPoolPtr addressPool,
    TDiscoveryConnectionConfigPtr connectionConfig,
    TDiscoveryClientConfigPtr clientConfig,
    IChannelFactoryPtr channelFactory,
    const NLogging::TLogger& logger,
    TGroupId groupId)
    : TRequestSession<TGroupMeta>(
        clientConfig->ReadQuorum,
        std::move(addressPool),
        logger)
    , ConnectionConfig_(std::move(connectionConfig))
    , ClientConfig_(std::move(clientConfig))
    , ChannelFactory_(std::move(channelFactory))
    , GroupId_(std::move(groupId))
{ }

TFuture<void> TGetGroupMetaRequestSession::MakeRequest(const TString& address)
{
    auto proxy = CreateProxy(
        ClientConfig_,
        ConnectionConfig_,
        ChannelFactory_,
        address);

    auto req = proxy.GetGroupMeta();
    req->set_group_id(GroupId_);
    return req->Invoke().Apply(BIND([this, this_ = MakeStrong(this)] (const TErrorOr<TDiscoveryClientServiceProxy::TRspGetGroupMetaPtr>& rspOrError) {
        if (!rspOrError.IsOK() && !rspOrError.FindMatching(NDiscoveryClient::EErrorCode::NoSuchGroup)) {
            return TError(rspOrError);
        }

        auto guard = Guard(Lock_);

        if (rspOrError.IsOK()) {
            auto groupMeta = FromProto<TGroupMeta>(rspOrError.Value()->meta());
            GroupMeta_.MemberCount = std::max(GroupMeta_.MemberCount, groupMeta.MemberCount);
        }

        if (++SuccessCount_ == GetRequiredSuccessCount()) {
            auto groupMeta = GroupMeta_;
            guard.Release();
            if (groupMeta.MemberCount == 0) {
                Promise_.TrySet(TError(NDiscoveryClient::EErrorCode::NoSuchGroup, "Group %Qv does not exist", GroupId_));
            } else {
                Promise_.TrySet(GroupMeta_);
            }
        }
        return TError();
    }));
}

////////////////////////////////////////////////////////////////////////////////

THeartbeatSession::THeartbeatSession(
    TServerAddressPoolPtr addressPool,
    TDiscoveryConnectionConfigPtr connectionConfig,
    TMemberClientConfigPtr clientConfig,
    IChannelFactoryPtr channelFactory,
    const NLogging::TLogger& logger,
    TGroupId groupId,
    TMemberId memberId,
    i64 priority,
    i64 revision,
    IAttributeDictionaryPtr attributes)
    : TRequestSession<void>(
        clientConfig->WriteQuorum,
        std::move(addressPool),
        logger)
    , ConnectionConfig_(std::move(connectionConfig))
    , ClientConfig_(std::move(clientConfig))
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
    proxy.SetDefaultTimeout(ConnectionConfig_->RpcTimeout);

    auto req = proxy.Heartbeat();

    req->set_group_id(GroupId_);
    auto* protoMemberInfo = req->mutable_member_info();
    protoMemberInfo->set_id(MemberId_);
    protoMemberInfo->set_priority(Priority_);
    protoMemberInfo->set_revision(Revision_);
    if (Attributes_) {
        ToProto(protoMemberInfo->mutable_attributes(), *Attributes_);
    }
    req->set_lease_timeout(ToProto<i64>(ClientConfig_->LeaseTimeout));

    return req->Invoke().Apply(BIND([this, this_ = MakeStrong(this)] (const TErrorOr<TDiscoveryClientServiceProxy::TRspHeartbeatPtr>& rspOrError) {
        if (!rspOrError.IsOK()) {
            if (rspOrError.FindMatching(NDiscoveryClient::EErrorCode::InvalidGroupId) ||
                rspOrError.FindMatching(NDiscoveryClient::EErrorCode::InvalidMemberId))
            {
                Promise_.TrySet(rspOrError);
                return TError();
            } else {
                return TError(rspOrError);
            }
        }

        if (++SuccessCount_ == GetRequiredSuccessCount()) {
            Promise_.TrySet();
        }
        return TError();
    }));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDiscoveryClient
