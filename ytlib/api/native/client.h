#pragma once

#include "public.h"

#include <yt/client/api/client.h>

#include <yt/ytlib/query_client/public.h>

namespace NYT::NApi::NNative {

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
        NElection::TCellId cellId) = 0;

    virtual NRpc::IChannelPtr GetSchedulerChannel() = 0;
    virtual const NNodeTrackerClient::INodeChannelFactoryPtr& GetChannelFactory() = 0;

    virtual ITransactionPtr AttachNativeTransaction(
        NTransactionClient::TTransactionId transactionId,
        const TTransactionAttachOptions& options = TTransactionAttachOptions()) = 0;
};

DEFINE_REFCOUNTED_TYPE(IClient)

////////////////////////////////////////////////////////////////////////////////

IClientPtr CreateClient(
    IConnectionPtr connection,
    const TClientOptions& options);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative

