#pragma once

#include "public.h"

#include <yt/client/api/connection.h>

#include <yt/ytlib/cell_master_client/public.h>

#include <yt/ytlib/query_client/public.h>

#include <yt/ytlib/chunk_client/public.h>

#include <yt/ytlib/node_tracker_client/public.h>

#include <yt/ytlib/job_prober_client/public.h>

#include <yt/ytlib/security_client/public.h>

#include <yt/ytlib/hive/public.h>

namespace NYT::NApi::NNative {

////////////////////////////////////////////////////////////////////////////////

struct IConnection
    : public NApi::IConnection
{
    virtual const TConnectionConfigPtr& GetConfig() = 0;

    virtual const NNodeTrackerClient::TNetworkPreferenceList& GetNetworks() const = 0;

    virtual NObjectClient::TCellId GetPrimaryMasterCellId() const = 0;
    virtual NObjectClient::TCellTag GetPrimaryMasterCellTag() const = 0;
    virtual const NObjectClient::TCellTagList& GetSecondaryMasterCellTags() const = 0;
    virtual NObjectClient::TCellId GetMasterCellId(NObjectClient::TCellTag cellTag) const = 0;

    virtual const NQueryClient::TEvaluatorPtr& GetQueryEvaluator() = 0;
    virtual const NQueryClient::TColumnEvaluatorCachePtr& GetColumnEvaluatorCache() = 0;
    virtual const NChunkClient::IBlockCachePtr& GetBlockCache() = 0;

    virtual const NCellMasterClient::TCellDirectoryPtr& GetMasterCellDirectory() = 0;
    virtual const NCellMasterClient::TCellDirectorySynchronizerPtr& GetMasterCellDirectorySynchronizer() = 0;

    virtual const NHiveClient::TCellDirectoryPtr& GetCellDirectory() = 0;
    virtual const NHiveClient::TCellDirectorySynchronizerPtr& GetCellDirectorySynchronizer() = 0;

    virtual const NHiveClient::TClusterDirectoryPtr& GetClusterDirectory() = 0;
    virtual const NHiveClient::TClusterDirectorySynchronizerPtr& GetClusterDirectorySynchronizer() = 0;

    virtual const NChunkClient::TMediumDirectoryPtr& GetMediumDirectory() = 0;
    virtual const NChunkClient::TMediumDirectorySynchronizerPtr& GetMediumDirectorySynchronizer() = 0;

    virtual const NNodeTrackerClient::TNodeDirectoryPtr& GetNodeDirectory() = 0;
    virtual const NNodeTrackerClient::TNodeDirectorySynchronizerPtr& GetNodeDirectorySynchronizer() = 0;

    virtual const NHiveClient::TCellTrackerPtr& GetDownedCellTracker() = 0;

    virtual NRpc::IChannelPtr GetMasterChannelOrThrow(
        EMasterChannelKind kind,
        NObjectClient::TCellTag cellTag = NObjectClient::PrimaryMasterCellTag) = 0;
    virtual NRpc::IChannelPtr GetMasterChannelOrThrow(
        EMasterChannelKind kind,
        NObjectClient::TCellId cellId) = 0;
    virtual const NRpc::IChannelPtr& GetSchedulerChannel() = 0;
    virtual const NRpc::IChannelFactoryPtr& GetChannelFactory() = 0;

    virtual const NTabletClient::ITableMountCachePtr& GetTableMountCache() = 0;
    virtual const NTransactionClient::ITimestampProviderPtr& GetTimestampProvider() = 0;

    virtual const NJobProberClient::TJobNodeDescriptorCachePtr& GetJobNodeDescriptorCache() = 0;

    virtual const NSecurityClient::TPermissionCachePtr& GetPermissionCache() = 0;

    virtual IClientPtr CreateNativeClient(const TClientOptions& options = TClientOptions()) = 0;

    virtual NYTree::IYPathServicePtr GetOrchidService() = 0;

    virtual void Terminate() = 0;
    virtual bool IsTerminated() = 0;

    virtual TFuture<void> SyncHiveCellWithOthers(
        const std::vector<NElection::TCellId>& srcCellIds,
        NElection::TCellId dstCellId) = 0;
};

DEFINE_REFCOUNTED_TYPE(IConnection)

////////////////////////////////////////////////////////////////////////////////

struct TConnectionOptions
{
    bool RetryRequestQueueSizeLimitExceeded = false;
};

//! Native connection talks directly to the cluster via internal
//! (and typically not stable) RPC protocols.
IConnectionPtr CreateConnection(
    TConnectionConfigPtr config,
    const TConnectionOptions& options = TConnectionOptions());

////////////////////////////////////////////////////////////////////////////////

IConnectionPtr FindRemoteConnection(
    const NApi::NNative::IConnectionPtr& connection,
    NObjectClient::TCellTag cellTag);

IConnectionPtr GetRemoteConnectionOrThrow(
    const NApi::NNative::IConnectionPtr& connection,
    NObjectClient::TCellTag cellTag);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative

