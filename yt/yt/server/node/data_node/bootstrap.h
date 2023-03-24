#pragma once

#include "public.h"

#include <yt/yt/server/node/cluster_node/bootstrap.h>

#include <yt/yt/library/query/row_comparer_api/row_comparer_generator.h>

namespace NYT::NDataNode {

////////////////////////////////////////////////////////////////////////////////

struct IBootstrap
    : public virtual NClusterNode::IBootstrapBase
{
    virtual void Initialize() = 0;
    virtual void Run() = 0;

    // Chunk storage.
    virtual const TChunkStorePtr& GetChunkStore() const = 0;
    virtual const IAllyReplicaManagerPtr& GetAllyReplicaManager() const = 0;
    virtual const TLocationManagerPtr& GetLocationManager() const = 0;

    // Session stuff.
    virtual const TSessionManagerPtr& GetSessionManager() const = 0;

    // Job controller.
    virtual const IJobControllerPtr& GetJobController() const = 0;

    // Master connection.
    virtual const IMasterConnectorPtr& GetMasterConnector() const = 0;

    // Medium stuff.
    virtual const TMediumDirectoryManagerPtr& GetMediumDirectoryManager() const = 0;
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
    virtual const TP2PBlockCachePtr& GetP2PBlockCache() const = 0;
    virtual const TP2PSnooperPtr& GetP2PSnooper() const = 0;

    // Caches.
    virtual const TTableSchemaCachePtr& GetTableSchemaCache() const = 0;
    virtual const NQueryClient::IRowComparerProviderPtr& GetRowComparerProvider() const = 0;

    virtual const IIOThroughputMeterPtr& GetIOThroughputMeter() const = 0;

    // Disk management.
    virtual const TLocationHealthCheckerPtr& GetLocationHealthChecker() const = 0;
};

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IBootstrap> CreateBootstrap(NClusterNode::IBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDataNode
