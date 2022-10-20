#pragma once

#include "public.h"
#include "helpers.h"
#include "config.h"

#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/service_discovery/service_discovery.h>

namespace NYT::NDiscoveryClient {

////////////////////////////////////////////////////////////////////////////////

class TServerAddressPool
    : public TRefCounted
{
public:
    TServerAddressPool(
        const NLogging::TLogger& Logger,
        const TDiscoveryClientBaseConfigPtr& config);

    std::vector<TString> GetUpAddresses();
    std::vector<TString> GetProbationAddresses();

    void BanAddress(const TString& address);
    void UnbanAddress(const TString& address);

    void SetBanTimeout(TDuration banTimeout);
    void SetConfig(const TDiscoveryClientBaseConfigPtr& config);

private:
    const NLogging::TLogger Logger;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, Lock_);
    TDiscoveryClientBaseConfigPtr Config_;
    THashSet<TString> UpAddresses_;
    THashSet<TString> ProbationAddresses_;
    THashSet<TString> DownAddresses_;

    NServiceDiscovery::IServiceDiscoveryPtr ServiceDiscovery_;
    NConcurrency::TPeriodicExecutorPtr EndpointsUpdateExecutor_;

    void OnBanTimeoutExpired(const TString& address);

    void UpdateEndpoints();
    void OnEndpointsResolved(
        const TString& endpointSetId,
        const TErrorOr<std::vector<TErrorOr<NServiceDiscovery::TEndpointSet>>>& endpointSetsOrError);

    void SetAddresses(const std::vector<TString>& addresses);
};

DEFINE_REFCOUNTED_TYPE(TServerAddressPool)

////////////////////////////////////////////////////////////////////////////////

template <class TResponse>
class TRequestSession
    : public TRefCounted
{
public:
    TRequestSession(
        int requiredSuccessCount,
        TServerAddressPoolPtr addressPool,
        const NLogging::TLogger& logger);

    TFuture<TResponse> Run();

protected:
    const int RequiredSuccessCount_;
    const TPromise<TResponse> Promise_;

    virtual TFuture<void> MakeRequest(const TString& address) = 0;

private:
    const TServerAddressPoolPtr AddressPool_;
    const NLogging::TLogger Logger;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, AddressesLock_);

    std::vector<TString> UpAddresses_;
    int CurrentUpAddressIndex_ = 0;

    bool HasExtraProbationRequest_ = false;
    std::vector<TString> ProbationAddresses_;
    int CurrentProbationAddressIndex_ = 0;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, ErrorsLock_);
    std::vector<TError> Errors_;

    void AddError(const TError& error);
    TError CreateError();
    void TryMakeNextRequest(bool forceProbation);
};

////////////////////////////////////////////////////////////////////////////////

TDiscoveryClientServiceProxy CreateProxy(
    const TDiscoveryClientConfigPtr& config,
    const NRpc::IChannelFactoryPtr& channelFactory,
    const TString& address);

////////////////////////////////////////////////////////////////////////////////

class TListMembersRequestSession
    : public TRequestSession<std::vector<TMemberInfo>>
{
public:
    TListMembersRequestSession(
        TServerAddressPoolPtr addressPool,
        TDiscoveryClientConfigPtr config,
        NRpc::IChannelFactoryPtr channelFactory,
        const NLogging::TLogger& logger,
        TGroupId groupId,
        TListMembersOptions options);

private:
    const TDiscoveryClientConfigPtr Config_;
    const NRpc::IChannelFactoryPtr ChannelFactory_;
    const TGroupId GroupId_;
    const TListMembersOptions Options_;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, Lock_);
    THashMap<TMemberId, TMemberInfo> IdToMember_;
    int SuccessCount_ = 0;

    TFuture<void> MakeRequest(const TString& address) override;
};

DEFINE_REFCOUNTED_TYPE(TListMembersRequestSession)

////////////////////////////////////////////////////////////////////////////////

class TGetGroupMetaRequestSession
    : public TRequestSession<TGroupMeta>
{
public:
    TGetGroupMetaRequestSession(
        TServerAddressPoolPtr addressPool,
        TDiscoveryClientConfigPtr config,
        NRpc::IChannelFactoryPtr channelFactory,
        const NLogging::TLogger& logger,
        TGroupId groupId);

private:
    const TDiscoveryClientConfigPtr Config_;
    const NRpc::IChannelFactoryPtr ChannelFactory_;
    const TGroupId GroupId_;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, Lock_);
    TGroupMeta GroupMeta_;
    int SuccessCount_ = 0;

    TFuture<void> MakeRequest(const TString& address) override;
};

DEFINE_REFCOUNTED_TYPE(TGetGroupMetaRequestSession)

////////////////////////////////////////////////////////////////////////////////

class THeartbeatSession
    : public TRequestSession<void>
{
public:
    THeartbeatSession(
        TServerAddressPoolPtr addressPool,
        TMemberClientConfigPtr config,
        NRpc::IChannelFactoryPtr channelFactory,
        const NLogging::TLogger& logger,
        TGroupId groupId,
        TMemberId memberId,
        i64 priority,
        i64 revision,
        NYTree::IAttributeDictionaryPtr attributes);

private:
    const TMemberClientConfigPtr Config_;
    const NRpc::IChannelFactoryPtr ChannelFactory_;
    const TGroupId GroupId_;
    const TMemberId MemberId_;
    const i64 Priority_;
    const i64 Revision_;
    const NYTree::IAttributeDictionaryPtr Attributes_;

    std::atomic<int> SuccessCount_ = 0;

    TFuture<void> MakeRequest(const TString& address) override;
};

DEFINE_REFCOUNTED_TYPE(THeartbeatSession)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDiscoveryClient

#define REQUEST_SESSION_INL_H_
#include "request_session-inl.h"
#undef REQUEST_SESSION_INL_H_
