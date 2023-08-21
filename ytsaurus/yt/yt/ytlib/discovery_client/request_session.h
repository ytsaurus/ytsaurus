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
        const TDiscoveryConnectionConfigPtr& config);

    int GetAddressCount() const;

    std::vector<TString> GetUpAddresses();
    std::vector<TString> GetProbationAddresses();

    void BanAddress(const TString& address);
    void UnbanAddress(const TString& address);

    void SetBanTimeout(TDuration banTimeout);
    //! After each reconfiguration with SetConfig GetReadyEvent's future
    //! should be awaited before server address pool usage.
    void SetConfig(const TDiscoveryConnectionConfigPtr& config);

    TFuture<void> GetReadyEvent() const;

private:
    const NLogging::TLogger Logger;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, Lock_);
    TDiscoveryConnectionConfigPtr Config_;
    THashSet<TString> UpAddresses_;
    THashSet<TString> ProbationAddresses_;
    THashSet<TString> DownAddresses_;

    std::atomic<int> AddressCount_ = 0;

    NServiceDiscovery::IServiceDiscoveryPtr ServiceDiscovery_;
    NConcurrency::TPeriodicExecutorPtr EndpointsUpdateExecutor_;
    TFuture<void> ResolvedAddressesFuture_;

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
        std::optional<int> requiredSuccessCount,
        TServerAddressPoolPtr addressPool,
        const NLogging::TLogger& logger);

    TFuture<TResponse> Run();

protected:
    const std::optional<int> OptionalRequiredSuccessCount_;
    const TPromise<TResponse> Promise_;

    virtual TFuture<void> MakeRequest(const TString& address) = 0;

    int GetRequiredSuccessCount() const;

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
        TDiscoveryConnectionConfigPtr connectionConfig,
        TDiscoveryClientConfigPtr clientConfig,
        NRpc::IChannelFactoryPtr channelFactory,
        const NLogging::TLogger& logger,
        TGroupId groupId,
        TListMembersOptions options);

private:
    const TDiscoveryConnectionConfigPtr ConnectionConfig_;
    const TDiscoveryClientConfigPtr ClientConfig_;
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
        TDiscoveryConnectionConfigPtr connectionConfig,
        TDiscoveryClientConfigPtr clientConfig,
        NRpc::IChannelFactoryPtr channelFactory,
        const NLogging::TLogger& logger,
        TGroupId groupId);

private:
    const TDiscoveryConnectionConfigPtr ConnectionConfig_;
    const TDiscoveryClientConfigPtr ClientConfig_;
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
        TDiscoveryConnectionConfigPtr connectionConfig,
        TMemberClientConfigPtr clientConfig,
        NRpc::IChannelFactoryPtr channelFactory,
        const NLogging::TLogger& logger,
        TGroupId groupId,
        TMemberId memberId,
        i64 priority,
        i64 revision,
        NYTree::IAttributeDictionaryPtr attributes);

private:
    const TDiscoveryConnectionConfigPtr ConnectionConfig_;
    const TMemberClientConfigPtr ClientConfig_;
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
