#pragma once

#include "public.h"

#include <yt/yt/client/api/client.h>

#include <yt/yt/ytlib/chaos_client/public.h>

#include <yt/yt/ytlib/query_client/public.h>

namespace NYT::NApi::NNative {

////////////////////////////////////////////////////////////////////////////////

// COMPAT(kvk1920)
struct TNativeTransactionStartOptions
    : public TTransactionStartOptions
{
    bool RequirePortalExitSynchronization = false;
};

////////////////////////////////////////////////////////////////////////////////

struct IClientBase
    : public virtual NApi::IClientBase
{
    // COMPAT(kvk1920)
    virtual TFuture<ITransactionPtr> StartNativeTransaction(
        NTransactionClient::ETransactionType type,
        const TNativeTransactionStartOptions& options = {}) = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct TSyncAlienCellOptions
    : public TTimeoutOptions
    , public TMasterReadOptions
{
    bool FullSync = false;
};

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
        NObjectClient::TCellTag cellTag = NObjectClient::PrimaryMasterCellTagSentinel) = 0;
    virtual NRpc::IChannelPtr GetCellChannelOrThrow(
        NElection::TCellId cellId) = 0;

    virtual NRpc::IChannelPtr GetSchedulerChannel() = 0;
    virtual const NNodeTrackerClient::INodeChannelFactoryPtr& GetChannelFactory() = 0;

    virtual ITransactionPtr AttachNativeTransaction(
        NTransactionClient::TTransactionId transactionId,
        const TTransactionAttachOptions& options = TTransactionAttachOptions()) = 0;

    virtual TFuture<std::vector<NChaosClient::TAlienCellDescriptor>> SyncAlienCells(
        const std::vector<NChaosClient::TAlienCellDescriptorLite>& alienCellDescriptors,
        const TSyncAlienCellOptions& options = {}) = 0;
};

DEFINE_REFCOUNTED_TYPE(IClient)

////////////////////////////////////////////////////////////////////////////////

IClientPtr CreateClient(
    IConnectionPtr connection,
    const TClientOptions& options);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative

