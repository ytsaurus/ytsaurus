#pragma once

#include "public.h"

#include <yt/yt/server/node/cluster_node/bootstrap.h>

#include <yt/yt/server/lib/security_server/public.h>

#include <yt/yt/library/query/row_comparer_api/row_comparer_generator.h>

namespace NYT::NTabletNode {

////////////////////////////////////////////////////////////////////////////////

struct IBootstrap
    : public virtual NClusterNode::IBootstrapBase
{
    virtual void Initialize() = 0;
    virtual void Run() = 0;

    // Tablet stuff.
    virtual const ITabletSnapshotStorePtr& GetTabletSnapshotStore() const = 0;

    virtual const IInMemoryManagerPtr& GetInMemoryManager() const = 0;

    virtual const NSecurityServer::IResourceLimitsManagerPtr& GetResourceLimitsManager() const = 0;

    virtual const IStructuredLoggerPtr& GetStructuredLogger() const = 0;

    virtual const IHintManagerPtr& GetHintManager() const = 0;

    virtual const IHedgingManagerRegistryPtr& GetHedgingManagerRegistry() const = 0;

    virtual const TTableDynamicConfigManagerPtr& GetTableDynamicConfigManager() const = 0;

    virtual const TErrorManagerPtr& GetErrorManager() const = 0;

    // Tablet cell stuff.
    virtual const ISlotManagerPtr& GetSlotManager() const = 0;

    virtual const NCellarAgent::ICellarManagerPtr& GetCellarManager() const = 0;

    // Invokers.
    virtual const IInvokerPtr& GetTransactionTrackerInvoker() const = 0;
    virtual const IInvokerPtr& GetTableReplicatorPoolInvoker() const = 0;
    virtual const IInvokerPtr& GetTabletLookupPoolInvoker() const = 0;
    virtual const IInvokerPtr& GetTabletFetchPoolInvoker() const = 0;
    virtual const IInvokerPtr& GetTableRowFetchPoolInvoker() const = 0;

    virtual IInvokerPtr GetQueryPoolInvoker(
        const TString& poolName,
        const NConcurrency::TFairShareThreadPoolTag& tag) const = 0;

    // Throttlers.
    virtual const NConcurrency::IThroughputThrottlerPtr& GetThrottler(NTabletNode::ETabletNodeThrottlerKind kind) const = 0;
    virtual const NConcurrency::IThroughputThrottlerPtr& GetInThrottler(EWorkloadCategory category) const = 0;
    virtual const NConcurrency::IThroughputThrottlerPtr& GetOutThrottler(EWorkloadCategory category) const = 0;

    // QL stuff.
    virtual const NQueryClient::IColumnEvaluatorCachePtr& GetColumnEvaluatorCache() const = 0;
    virtual const NQueryClient::IRowComparerProviderPtr& GetRowComparerProvider() const = 0;

    // Master connection stuff.
    virtual const IMasterConnectorPtr& GetMasterConnector() const = 0;
    virtual const NCellarNode::IMasterConnectorPtr& GetCellarNodeMasterConnector() const = 0;

    // Data Node stuff for local chunk readers.
    // NB: Might be null if node is not a Data Node.
    // TODO(gritukan): Remove it after node split.
    const NDataNode::IChunkRegistryPtr& GetChunkRegistry() const override = 0;

    virtual const TOverloadControllerPtr& GetOverloadController() const = 0;

    virtual const ICompressionDictionaryManagerPtr& GetCompressionDictionaryManager() const = 0;
};

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IBootstrap> CreateBootstrap(NClusterNode::IBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
