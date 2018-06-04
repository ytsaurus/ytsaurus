#pragma once

#include "public.h"
#include "discovering_channel.h"

#include <yt/core/concurrency/public.h>

#include <yt/core/rpc/public.h>

#include <yt/ytlib/api/proxy_connection.h>
#include <yt/core/logging/log.h>

namespace NYT {
namespace NRpcProxy {

////////////////////////////////////////////////////////////////////////////////

class TConnection
    : public NApi::IProxyConnection
{
public:
    explicit TConnection(TConnectionConfigPtr config);

    NRpc::IChannelPtr CreateChannelAndRegisterProvider(
        const NApi::TClientOptions& options,
        NRpc::IRoamingChannelProvider* provider);
    void UnregisterProvider(
        NRpc::IRoamingChannelProvider* provider);

    const TConnectionConfigPtr& GetConfig();

    // IConnection implementation
    virtual NObjectClient::TCellTag GetCellTag() override;

    virtual const NTabletClient::ITableMountCachePtr& GetTableMountCache() override;
    virtual const NTransactionClient::ITimestampProviderPtr& GetTimestampProvider() override;

    virtual IInvokerPtr GetInvoker() override;

    virtual NApi::IAdminPtr CreateAdmin(const NApi::TAdminOptions& options) override;
    virtual NApi::IClientPtr CreateClient(const NApi::TClientOptions& options) override;
    virtual NHiveClient::ITransactionParticipantPtr CreateTransactionParticipant(
        const NHiveClient::TCellId& cellId,
        const NApi::TTransactionParticipantOptions& options) override;

    virtual void ClearMetadataCaches() override;

    virtual void Terminate() override;

    // IProxyConnection implementation
    virtual TFuture<std::vector<NApi::TProxyInfo>> DiscoverProxies(
        const NApi::TDiscoverProxyOptions& options = {}) override;

private:
    friend class TClient;
    friend class TTransaction;
    friend class TTimestampProvider;

    const TConnectionConfigPtr Config_;
    const NConcurrency::TActionQueuePtr ActionQueue_;
    const NRpc::IChannelFactoryPtr ChannelFactory_;
    NTabletClient::ITableMountCachePtr TableMountCache_;

    const NLogging::TLogger Logger;

    TSpinLock TimestampProviderSpinLock_;
    std::atomic<bool> TimestampProviderInitialized_ = {false};
    NTransactionClient::ITimestampProviderPtr TimestampProvider_;

    const NConcurrency::TPeriodicExecutorPtr UpdateProxyListExecutor_;

    TSpinLock AddressSpinLock_;
    std::vector<TString> Addresses_; // Must be sorted.
    THashMap<TString, THashSet<NRpc::IRoamingChannelProvider*>> AddressToProviders_;
    THashMap<NRpc::IRoamingChannelProvider*, TString> ProviderToAddress_;

    NRpc::IChannelPtr DiscoveryChannel_;
    int ProxyListUpdatesFailedAttempts_ = 0;

    TFuture<std::vector<NApi::TProxyInfo>> DiscoverProxies(
        const NRpc::IChannelPtr& channel,
        const NApi::TDiscoverProxyOptions& options = {});

    void ResetAddresses();

    NRpc::IChannelPtr GetRandomPeerChannel(NRpc::IRoamingChannelProvider* provider = nullptr);
    TString GetLocalAddress();
    void OnProxyListUpdated();

    void SetProxyList(std::vector<TString> addresses);

    TFuture<std::vector<TError>> TerminateAddressProviders(const std::vector<TString>& addresses);
};

DEFINE_REFCOUNTED_TYPE(TConnection)

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpcProxy
} // namespace NYT
