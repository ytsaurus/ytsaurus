#pragma once

#include "public.h"
#include "dynamic_channel_pool.h"

#include <yt/client/api/connection.h>

#include <yt/core/concurrency/public.h>

#include <yt/core/rpc/public.h>

#include <yt/core/logging/log.h>

namespace NYT {
namespace NApi {
namespace NRpcProxy {

////////////////////////////////////////////////////////////////////////////////

class TConnection
    : public NApi::IConnection
{
public:
    explicit TConnection(TConnectionConfigPtr config);

    TFuture<NRpc::IChannelPtr> CreateChannelAndRegisterProvider(
        const NApi::TClientOptions& options,
        NRpc::IRoamingChannelProvider* provider);
    void UnregisterProvider(
        NRpc::IRoamingChannelProvider* provider);

    const TConnectionConfigPtr& GetConfig();

    // IConnection implementation
    virtual NObjectClient::TCellTag GetCellTag() override;

    virtual IInvokerPtr GetInvoker() override;

    virtual NApi::IAdminPtr CreateAdmin(const NApi::TAdminOptions& options) override;
    virtual NApi::IClientPtr CreateClient(const NApi::TClientOptions& options) override;
    virtual NHiveClient::ITransactionParticipantPtr CreateTransactionParticipant(
        const NHiveClient::TCellId& cellId,
        const NApi::TTransactionParticipantOptions& options) override;

    virtual void ClearMetadataCaches() override;

    virtual void Terminate() override;

private:
    friend class TClient;
    friend class TTransaction;
    friend class TTimestampProvider;

    const TConnectionConfigPtr Config_;
    const NConcurrency::TActionQueuePtr ActionQueue_;
    const NRpc::IChannelFactoryPtr ChannelFactory_;
    const TDynamicChannelPoolPtr ChannelPool_;

    const NLogging::TLogger Logger;

    const NConcurrency::TPeriodicExecutorPtr UpdateProxyListExecutor_;
    NRpc::IChannelPtr DiscoveryChannel_;
    TPromise<std::vector<TString>> DiscoveryPromise_;

    TSpinLock HttpDiscoveryLock_;
    // TODO(prime@): Create http endpoint for discovery that works without authentication.
    TNullable<NApi::TClientOptions> HttpCredentials_;

    std::vector<TString> DiscoverProxiesByRpc(const NRpc::IChannelPtr& channel);
    std::vector<TString> DiscoverProxiesByHttp(const NApi::TClientOptions& options);

    void OnProxyListUpdate();
};

DEFINE_REFCOUNTED_TYPE(TConnection)

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpcProxy
} // namespace NApi
} // namespace NYT
