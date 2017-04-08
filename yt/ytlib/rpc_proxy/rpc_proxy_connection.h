#pragma once

#include "public.h"

#include <yt/core/concurrency/public.h>

#include <yt/core/rpc/public.h>

#include <yt/ytlib/api/connection.h>

namespace NYT {
namespace NRpcProxy {

////////////////////////////////////////////////////////////////////////////////

class TRpcProxyConnection
    : public NApi::IConnection
{
public:
    TRpcProxyConnection(
        TRpcProxyConnectionConfigPtr config,
        NConcurrency::TActionQueuePtr actionQueue);
    ~TRpcProxyConnection();

    virtual NObjectClient::TCellTag GetCellTag() override;

    virtual NTabletClient::ITableMountCachePtr GetTableMountCache() override;
    virtual NTransactionClient::ITimestampProviderPtr GetTimestampProvider() override;

    virtual IInvokerPtr GetLightInvoker() override;
    virtual IInvokerPtr GetHeavyInvoker() override;

    virtual NApi::IAdminPtr CreateAdmin(const NApi::TAdminOptions& options) override;
    virtual NApi::IClientPtr CreateClient(const NApi::TClientOptions& options) override;
    virtual NHiveClient::ITransactionParticipantPtr CreateTransactionParticipant(
        const NHiveClient::TCellId& cellId,
        const NApi::TTransactionParticipantOptions& options) override;

    virtual void ClearMetadataCaches() override;

    virtual void Terminate() override;

private:
    const TRpcProxyConnectionConfigPtr Config_;
    const NConcurrency::TActionQueuePtr ActionQueue_;

    friend class TRpcProxyClient;
};

DEFINE_REFCOUNTED_TYPE(TRpcProxyConnection)

NApi::IConnectionPtr CreateRpcProxyConnection(
    TRpcProxyConnectionConfigPtr config);

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpcProxy
} // namespace NYT
