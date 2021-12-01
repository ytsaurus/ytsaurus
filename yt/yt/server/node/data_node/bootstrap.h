#pragma once

#include "public.h"

#include <yt/yt/server/node/cluster_node/bootstrap.h>

#include <yt/yt/server/lib/io/io_tracker.h>

#include <yt/yt/library/profiling/resource_tracker/resource_tracker.h>

namespace NYT::NDataNode {

////////////////////////////////////////////////////////////////////////////////

struct IBootstrap
    : public virtual NClusterNode::IBootstrapBase
{
    virtual ~IBootstrap() = default;

    virtual void Initialize() = 0;
    virtual void Run() = 0;

    // Chunk storage.
    virtual const TChunkStorePtr& GetChunkStore() const = 0;
    virtual const IAllyReplicaManagerPtr& GetAllyReplicaManager() const = 0;

    virtual const IChunkBlockManagerPtr& GetChunkBlockManager() const = 0;

    // Session stuff.
    virtual const TSessionManagerPtr& GetSessionManager() const = 0;

    // Master connection.
    virtual const IMasterConnectorPtr& GetMasterConnector() const = 0;

    virtual const TMediumUpdaterPtr& GetMediumUpdater() const = 0;

    // Throttlers.
    virtual const NConcurrency::IThroughputThrottlerPtr& GetThrottler(EDataNodeThrottlerKind kind) const = 0;
    virtual const NConcurrency::IThroughputThrottlerPtr& GetInThrottler(const TWorkloadDescriptor& descriptor) const = 0;
    virtual const NConcurrency::IThroughputThrottlerPtr& GetOutThrottler(const TWorkloadDescriptor& descriptor) const = 0;

    // Journal stuff.
    virtual const IJournalDispatcherPtr& GetJournalDispatcher() const = 0;

    // Invokers.
    virtual const IInvokerPtr& GetStorageLookupInvoker() const = 0;
    virtual const IInvokerPtr& GetMasterJobInvoker() const = 0;

    // P2P stuff.
    virtual const TBlockPeerTablePtr& GetBlockPeerTable() const = 0;
    virtual const TP2PBlockDistributorPtr& GetP2PBlockDistributor() const = 0;
    virtual const TP2PBlockCachePtr& GetP2PBlockCache() const = 0;
    virtual const TP2PSnooperPtr& GetP2PSnooper() const = 0;

    // Caches.
    virtual const TTableSchemaCachePtr& GetTableSchemaCache() const = 0;
    virtual const NTabletClient::IRowComparerProviderPtr& GetRowComparerProvider() const = 0;

    // IO tracker.
    virtual const NIO::IIOTrackerPtr& GetIOTracker() const = 0;
};

////////////////////////////////////////////////////////////////////////////////

class TBootstrap
    : public IBootstrap
    , public NClusterNode::TBootstrapForward
{
public:
    explicit TBootstrap(NClusterNode::IBootstrap* bootstrap);

    void Initialize() override;
    void Run() override;

    const TChunkStorePtr& GetChunkStore() const override;
    const IAllyReplicaManagerPtr& GetAllyReplicaManager() const override;
    const IChunkBlockManagerPtr& GetChunkBlockManager() const override;
    const TSessionManagerPtr& GetSessionManager() const override;
    const IMasterConnectorPtr& GetMasterConnector() const override;
    const TMediumUpdaterPtr& GetMediumUpdater() const override;
    const NConcurrency::IThroughputThrottlerPtr& GetThrottler(EDataNodeThrottlerKind kind) const override;
    const NConcurrency::IThroughputThrottlerPtr& GetInThrottler(const TWorkloadDescriptor& descriptor) const override;
    const NConcurrency::IThroughputThrottlerPtr& GetOutThrottler(const TWorkloadDescriptor& descriptor) const override;
    const IJournalDispatcherPtr& GetJournalDispatcher() const override;
    const IInvokerPtr& GetStorageLookupInvoker() const override;
    const IInvokerPtr& GetMasterJobInvoker() const override;
    const TBlockPeerTablePtr& GetBlockPeerTable() const override;
    const TP2PBlockDistributorPtr& GetP2PBlockDistributor() const override;
    const TP2PBlockCachePtr& GetP2PBlockCache() const override;
    const TP2PSnooperPtr& GetP2PSnooper() const override;
    const TTableSchemaCachePtr& GetTableSchemaCache() const override;
    const NTabletClient::IRowComparerProviderPtr& GetRowComparerProvider() const override;
    const NIO::IIOTrackerPtr& GetIOTracker() const override;

protected:
    NClusterNode::IBootstrap* const ClusterNodeBootstrap_;

    TChunkStorePtr ChunkStore_;
    IAllyReplicaManagerPtr AllyReplicaManager_;

    IChunkBlockManagerPtr ChunkBlockManager_;

    TSessionManagerPtr SessionManager_;

    IMasterConnectorPtr MasterConnector_;

    TMediumUpdaterPtr MediumUpdater_;

    TEnumIndexedVector<EDataNodeThrottlerKind, NConcurrency::IReconfigurableThroughputThrottlerPtr> RawThrottlers_;
    TEnumIndexedVector<EDataNodeThrottlerKind, NConcurrency::IThroughputThrottlerPtr> Throttlers_;

    IJournalDispatcherPtr JournalDispatcher_;

    NConcurrency::TThreadPoolPtr StorageLookupThreadPool_;
    NConcurrency::TThreadPoolPtr MasterJobThreadPool_;

    NConcurrency::TActionQueuePtr P2PActionQueue_;
    TBlockPeerTablePtr BlockPeerTable_;
    TP2PBlockDistributorPtr P2PBlockDistributor_;
    TP2PBlockCachePtr P2PBlockCache_;
    TP2PSnooperPtr P2PSnooper_;
    TP2PDistributorPtr P2PDistributor_;

    TTableSchemaCachePtr TableSchemaCache_;

    NTabletClient::IRowComparerProviderPtr RowComparerProvider_;

    NHttp::IServerPtr SkynetHttpServer_;

    NIO::IIOTrackerPtr IOTracker_;
    NProfiling::TDiskTrackerPtr DiskTracker_;

    void OnDynamicConfigChanged(
        const NClusterNode::TClusterNodeDynamicConfigPtr& /*oldConfig*/,
        const NClusterNode::TClusterNodeDynamicConfigPtr& newConfig);
};

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IBootstrap> CreateBootstrap(NClusterNode::IBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDataNode
