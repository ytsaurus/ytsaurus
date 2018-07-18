#pragma once

#include "public.h"

#include <yt/client/api/client.h>

namespace NYT {
namespace NApi {
namespace NNative {

////////////////////////////////////////////////////////////////////////////////

struct IClientBase
    : public virtual NApi::IClientBase
{
    virtual TFuture<ITransactionPtr> StartNativeTransaction(
        NTransactionClient::ETransactionType type,
        const TTransactionStartOptions& options = TTransactionStartOptions()) = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct IClient
    : public IClientBase
    , public NApi::IClient
{
    virtual const TClientOptions& GetOptions() = 0;
    virtual const IConnectionPtr& GetNativeConnection() = 0;
    virtual NQueryClient::IFunctionRegistryPtr GetFunctionRegistry() = 0;
    virtual NQueryClient::TFunctionImplCachePtr GetFunctionImplCache() = 0;

    virtual NRpc::IChannelPtr GetMasterChannelOrThrow(
        EMasterChannelKind kind,
        NObjectClient::TCellTag cellTag = NObjectClient::PrimaryMasterCellTag) = 0;
    virtual NRpc::IChannelPtr GetCellChannelOrThrow(
        const NElection::TCellId& cellId) = 0;

    virtual NRpc::IChannelPtr GetSchedulerChannel() = 0;
    virtual const NNodeTrackerClient::INodeChannelFactoryPtr& GetChannelFactory() = 0;

    virtual ITransactionPtr AttachNativeTransaction(
        const NTransactionClient::TTransactionId& transactionId,
        const TTransactionAttachOptions& options = TTransactionAttachOptions()) = 0;
};

DEFINE_REFCOUNTED_TYPE(IClient)

////////////////////////////////////////////////////////////////////////////////

IClientPtr CreateClient(
    IConnectionPtr connection,
    const TClientOptions& options);

////////////////////////////////////////////////////////////////////////////////

} // namespace NNative
} // namespace NApi
} // namespace NYT

