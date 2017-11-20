#pragma once

#include "public.h"

#include <yt/core/concurrency/public.h>

#include <yt/core/rpc/public.h>

#include <yt/ytlib/api/proxy_connection.h>
#include <yt/core/logging/log.h>

namespace NYT {
namespace NRpcProxy {

////////////////////////////////////////////////////////////////////////////////

class TRpcProxyConnection
    : public NApi::IProxyConnection
{
public:
    TRpcProxyConnection(
        TRpcProxyConnectionConfigPtr config,
        NConcurrency::TActionQueuePtr actionQueue);
    ~TRpcProxyConnection();

    // IConnection methods.

    virtual NObjectClient::TCellTag GetCellTag() override;

    virtual const NTabletClient::ITableMountCachePtr& GetTableMountCache() override;
    virtual const NTransactionClient::ITimestampProviderPtr& GetTimestampProvider() override;
    virtual const IInvokerPtr& GetInvoker() override;

    virtual NApi::IAdminPtr CreateAdmin(const NApi::TAdminOptions& options) override;
    virtual NApi::IClientPtr CreateClient(const NApi::TClientOptions& options) override;
    virtual NHiveClient::ITransactionParticipantPtr CreateTransactionParticipant(
        const NHiveClient::TCellId& cellId,
        const NApi::TTransactionParticipantOptions& options) override;

    virtual void ClearMetadataCaches() override;

    virtual void Terminate() override;

    virtual TFuture<std::vector<NApi::TProxyInfo>> DiscoverProxies(
        const NApi::TDiscoverProxyOptions& options) override;

private:
    const TRpcProxyConnectionConfigPtr Config_;
    const NConcurrency::TActionQueuePtr ActionQueue_;
    const NRpc::IChannelFactoryPtr ChannelFactory_;

    const NLogging::TLogger Logger;

    TSpinLock SpinLock_;
    THashSet<TRpcProxyTransaction*> Transactions_;
    NTransactionClient::ITimestampProviderPtr TimestampProvider_;

    NConcurrency::TPeriodicExecutorPtr PingExecutor_;

protected:
    friend class TRpcProxyClient;
    friend class TRpcProxyTransaction;
    friend class TRpcProxyTimestampProvider;

    // Implementation-specific methods.

    NRpc::IChannelPtr GetRandomPeerChannel();

    void RegisterTransaction(TRpcProxyTransaction* transaction);
    void UnregisterTransaction(TRpcProxyTransaction* transaction);

    void OnPing();
    void OnPingCompleted(const TErrorOr<std::vector<TError>>& pingResults);
};

DEFINE_REFCOUNTED_TYPE(TRpcProxyConnection)

NApi::IProxyConnectionPtr CreateRpcProxyConnection(
    TRpcProxyConnectionConfigPtr config);

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpcProxy
} // namespace NYT
