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
    virtual TNativeConnectionConfigPtr GetConfig() = 0;

    virtual const NNodeTrackerClient::TNetworkPreferenceList& GetNetworks() const = 0;

    virtual const NObjectClient::TCellId& GetPrimaryMasterCellId() const = 0;
    virtual NObjectClient::TCellTag GetPrimaryMasterCellTag() const = 0;
    virtual const NObjectClient::TCellTagList& GetSecondaryMasterCellTags() const = 0;

    virtual NQueryClient::TEvaluatorPtr GetQueryEvaluator() = 0;
    virtual NQueryClient::TColumnEvaluatorCachePtr GetColumnEvaluatorCache() = 0;
    virtual NHiveClient::TCellDirectoryPtr GetCellDirectory() = 0;
    virtual NChunkClient::IBlockCachePtr GetBlockCache() = 0;

    virtual NRpc::IChannelPtr GetMasterChannelOrThrow(
        EMasterChannelKind kind,
        NObjectClient::TCellTag cellTag = NObjectClient::PrimaryMasterCellTag) = 0;
    virtual NRpc::IChannelPtr GetSchedulerChannel() = 0;
    // TODO(sandello): Consider joining these two in favor of a partitioned channel.

    virtual NRpc::IChannelFactoryPtr GetLightChannelFactory() = 0;
    virtual NRpc::IChannelFactoryPtr GetHeavyChannelFactory() = 0;

    virtual INativeClientPtr CreateNativeClient(const TClientOptions& options = TClientOptions()) = 0;

    virtual INativeTransactionPtr RegisterStickyTransaction(INativeTransactionPtr transaction) = 0;
    virtual INativeTransactionPtr GetStickyTransaction(const NTransactionClient::TTransactionId& transactionId) = 0;

    virtual NChunkClient::TMediumDirectoryPtr GetMediumDirectory() = 0;
    //! Synchronizes the medium directory with the cluster metadata.
    /*!
     *  The returned future is set once the sync is complete (either successfully or not).
     *  Periodic syncs start upon the first call to #Sync.
     */
    virtual TFuture<void> SynchronizeMediumDirectory() = 0;

    virtual void Terminate() = 0;
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

