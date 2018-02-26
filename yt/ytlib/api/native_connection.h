#pragma once

#include "connection.h"

#include <yt/ytlib/query_client/public.h>

#include <yt/ytlib/hive/public.h>

namespace NYT {
namespace NApi {

////////////////////////////////////////////////////////////////////////////////

struct INativeConnection
    : public IConnection
{
    virtual const TNativeConnectionConfigPtr& GetConfig() = 0;

    virtual const NNodeTrackerClient::TNetworkPreferenceList& GetNetworks() const = 0;

    virtual const NObjectClient::TCellId& GetPrimaryMasterCellId() const = 0;
    virtual NObjectClient::TCellTag GetPrimaryMasterCellTag() const = 0;
    virtual const NObjectClient::TCellTagList& GetSecondaryMasterCellTags() const = 0;

    virtual const NQueryClient::TEvaluatorPtr& GetQueryEvaluator() = 0;
    virtual const NQueryClient::TColumnEvaluatorCachePtr& GetColumnEvaluatorCache() = 0;
    virtual const NChunkClient::IBlockCachePtr& GetBlockCache() = 0;

    virtual const NHiveClient::TCellDirectoryPtr& GetCellDirectory() = 0;
    virtual const NHiveClient::TCellDirectorySynchronizerPtr& GetCellDirectorySynchronizer() = 0;

    virtual const NHiveClient::TClusterDirectoryPtr& GetClusterDirectory() = 0;
    virtual const NHiveClient::TClusterDirectorySynchronizerPtr& GetClusterDirectorySynchronizer() = 0;

    virtual NRpc::IChannelPtr GetMasterChannelOrThrow(
        EMasterChannelKind kind,
        NObjectClient::TCellTag cellTag = NObjectClient::PrimaryMasterCellTag) = 0;
    virtual NRpc::IChannelPtr GetMasterChannelOrThrow(
        EMasterChannelKind kind,
        const NObjectClient::TCellId& cellId) = 0;
    virtual const NRpc::IChannelPtr& GetSchedulerChannel() = 0;
    virtual const NRpc::IChannelFactoryPtr& GetChannelFactory() = 0;

    virtual INativeClientPtr CreateNativeClient(const TClientOptions& options = TClientOptions()) = 0;

    virtual INativeTransactionPtr RegisterStickyTransaction(INativeTransactionPtr transaction) = 0;
    virtual INativeTransactionPtr GetStickyTransaction(const NTransactionClient::TTransactionId& transactionId) = 0;

    virtual void Terminate() = 0;
    virtual bool IsTerminated() = 0;
};

DEFINE_REFCOUNTED_TYPE(INativeConnection)

////////////////////////////////////////////////////////////////////////////////

struct TNativeConnectionOptions
{
    bool RetryRequestQueueSizeLimitExceeded = false;
};

//! Native connection talks directly to the cluster via internal
//! (and typically not stable) RPC protocols.
INativeConnectionPtr CreateNativeConnection(
    TNativeConnectionConfigPtr config,
    const TNativeConnectionOptions& options = TNativeConnectionOptions());

////////////////////////////////////////////////////////////////////////////////

} // namespace NApi
} // namespace NYT

