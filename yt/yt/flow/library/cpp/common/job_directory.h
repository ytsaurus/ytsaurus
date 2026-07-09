#pragma once

#include "public.h"

#include <yt/yt/flow/library/cpp/common/key.h>

#include <yt/yt/client/table_client/public.h>

#include <yt/yt/core/rpc/bus/public.h>

#include <yt/yt/core/actions/signal.h>

#include <yt/yt/core/logging/log.h>

#include <util/generic/hash.h>
#include <util/generic/hash_set.h>

#include <utility>
#include <vector>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

struct TMessageRoute
{
    TMessageRoute(TPartitionId partitionId, TJobId jobId, std::string workerAddress, TKeyRange keyRange);

    TPartitionId PartitionId;
    TJobId JobId;
    std::string WorkerAddress;
    TKeyRange KeyRange;
};

////////////////////////////////////////////////////////////////////////////////

//! An immutable view of the routing topology.
//! All accessors are const, lock-free and safe to call from any number of threads.
class TJobDirectorySnapshot
    : public TRefCounted
{
public:
    //! Routes of a computation, sorted by their partition's upper key (queried via upper_bound).
    using TSortedRoutes = std::vector<std::pair<TKey, TMessageRoute>>;

    struct TComputationRouting
    {
        TSortedRoutes UpperKeyToRoute;
        i64 ExecutingPartitionCount = 0;
    };

    TJobDirectorySnapshot(
        THashMap<TComputationId, NTableClient::TTableSchemaPtr> groupBySchemas,
        IPayloadConverterCachePtr converterCache,
        THashMap<TComputationId, TComputationRouting> computations,
        THashSet<TJobId> routableJobs,
        THashSet<TJobId> aliveJobs,
        THashSet<std::string> workers,
        NLogging::TLogger logger);

    //! The expensive, payload-parsing half of routing. nullopt for unknown computations.
    std::optional<TKey> ComputeMessageKey(const TComputationId& computationId, const TMessage& message) const;

    //! The cheap half of routing; pairs with ComputeMessageKey. Null if no key range matches.
    //! Valid while this snapshot is alive.
    const TMessageRoute* FindRouteByKey(const TComputationId& computationId, const TKey& key) const;

    //! Number of executing partitions of the computation.
    i64 GetPartitionCount(const TComputationId& computationId) const;

    //! Whether the job is in the active layout; anchors source-job liveness.
    bool IsJobAlive(TJobId jobId) const;

    //! Jobs usable as routing destinations (those whose partition has a key range).
    const THashSet<TJobId>& RoutableJobs() const;

    const THashSet<std::string>& Workers() const;

private:
    // Captured at build time so key computation needs no per-message lock or refcount bump,
    // and so a concurrent reconfigure cannot swap the schema under an in-flight snapshot.
    const THashMap<TComputationId, NTableClient::TTableSchemaPtr> GroupBySchemas_;
    const IPayloadConverterCachePtr ConverterCache_;
    const THashMap<TComputationId, TComputationRouting> Computations_;
    const THashSet<TJobId> RoutableJobs_;
    const THashSet<TJobId> AliveJobs_;
    const THashSet<std::string> Workers_;
    const NLogging::TLogger Logger;
};

DEFINE_REFCOUNTED_TYPE(TJobDirectorySnapshot);

////////////////////////////////////////////////////////////////////////////////

struct TJobDirectoryDiff
{
    std::vector<TJobId> AddedJobs;
    std::vector<TJobId> RemovedJobs;
    std::vector<std::string> RemovedWorkers;
};

//! Computes the diff between two snapshots in terms of alive jobs and workers.
//! Either argument may be null (treated as empty).
TJobDirectoryDiff ComputeJobDirectoryDiff(
    const TJobDirectorySnapshotPtr& from,
    const TJobDirectorySnapshotPtr& to);

////////////////////////////////////////////////////////////////////////////////

// XXX(babenko): pick a better name?
struct IJobDirectory
    : public TRefCounted
{
    //! Builds a new snapshot from the layout and pipeline spec, atomically publishes it and fires SnapshotPublished.
    virtual void Reconfigure(const TFlowLayoutPtr& flowLayout, const TPipelineSpecPtr& pipelineSpec) = 0;

    //! Returns the currently published snapshot. Never null.
    virtual TJobDirectorySnapshotPtr GetSnapshot() const = 0;

    // Convenience forwarders to the current snapshot.
    virtual i64 GetPartitionCount(const TComputationId& computationId) const = 0;
    //! Returns nullopt if no key range matches. Forwards through a freshly acquired snapshot,
    //! so it returns a copy rather than a pointer into a snapshot the caller does not hold.
    virtual std::optional<TMessageRoute> FindRouteByKey(const TComputationId& computationId, const TKey& key) const = 0;

    //! Fired right after a new snapshot is published, so subscribers need not depend on reconfiguration order.
    using TSnapshotPublishedSignature = void(const TJobDirectorySnapshotPtr& snapshot);
    DECLARE_INTERFACE_SIGNAL(TSnapshotPublishedSignature, SnapshotPublished);
};

DEFINE_REFCOUNTED_TYPE(IJobDirectory);

////////////////////////////////////////////////////////////////////////////////

IJobDirectoryPtr CreateJobDirectory(IPayloadConverterCachePtr converterCache, const NLogging::TLogger& logger);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
