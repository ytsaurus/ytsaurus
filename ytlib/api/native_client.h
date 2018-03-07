#pragma once

#include "client.h"

namespace NYT {
namespace NApi {

////////////////////////////////////////////////////////////////////////////////

struct INativeClientBase
    : public virtual IClientBase
{
    virtual TFuture<INativeTransactionPtr> StartNativeTransaction(
        NTransactionClient::ETransactionType type,
        const TTransactionStartOptions& options = TTransactionStartOptions()) = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct INativeClient
    : public INativeClientBase
    , public IClient
{
    virtual const TClientOptions& GetOptions() = 0;
    virtual const INativeConnectionPtr& GetNativeConnection() = 0;
    virtual NQueryClient::IFunctionRegistryPtr GetFunctionRegistry() = 0;
    virtual NQueryClient::TFunctionImplCachePtr GetFunctionImplCache() = 0;

    virtual NRpc::IChannelPtr GetMasterChannelOrThrow(
        EMasterChannelKind kind,
        NObjectClient::TCellTag cellTag = NObjectClient::PrimaryMasterCellTag) = 0;
    virtual NRpc::IChannelPtr GetCellChannelOrThrow(
        const NElection::TCellId& cellId) = 0;

    virtual NRpc::IChannelPtr GetSchedulerChannel() = 0;
    virtual const NNodeTrackerClient::INodeChannelFactoryPtr& GetChannelFactory() = 0;

    virtual INativeTransactionPtr AttachNativeTransaction(
        const NTransactionClient::TTransactionId& transactionId,
        const TTransactionAttachOptions& options = TTransactionAttachOptions()) = 0;
};

DEFINE_REFCOUNTED_TYPE(INativeClient)

////////////////////////////////////////////////////////////////////////////////

INativeClientPtr CreateNativeClient(
    INativeConnectionPtr connection,
    const TClientOptions& options);

////////////////////////////////////////////////////////////////////////////////

} // namespace NApi
} // namespace NYT

