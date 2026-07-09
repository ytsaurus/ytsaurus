#include "job_directory.h"

#include <yt/yt/flow/library/cpp/common/flow_view.h>
#include <yt/yt/flow/library/cpp/common/message.h>
#include <yt/yt/flow/library/cpp/common/payload_converter.h>
#include <yt/yt/flow/library/cpp/common/schema.h>
#include <yt/yt/flow/library/cpp/common/spec.h>

#include <yt/yt/core/misc/collection_helpers.h>

#include <library/cpp/yt/memory/atomic_intrusive_ptr.h>

#include <library/cpp/yt/string/guid.h>

#include <util/generic/algorithm.h>

#include <algorithm>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

TMessageRoute::TMessageRoute(TPartitionId partitionId, TJobId jobId, std::string workerAddress, TKeyRange keyRange)
    : PartitionId(partitionId)
    , JobId(jobId)
    , WorkerAddress(std::move(workerAddress))
    , KeyRange(std::move(keyRange))
{ }

////////////////////////////////////////////////////////////////////////////////

TJobDirectorySnapshot::TJobDirectorySnapshot(
    THashMap<TComputationId, NTableClient::TTableSchemaPtr> groupBySchemas,
    IPayloadConverterCachePtr converterCache,
    THashMap<TComputationId, TComputationRouting> computations,
    THashSet<TJobId> routableJobs,
    THashSet<TJobId> aliveJobs,
    THashSet<std::string> workers,
    NLogging::TLogger logger)
    : GroupBySchemas_(std::move(groupBySchemas))
    , ConverterCache_(std::move(converterCache))
    , Computations_(std::move(computations))
    , RoutableJobs_(std::move(routableJobs))
    , AliveJobs_(std::move(aliveJobs))
    , Workers_(std::move(workers))
    , Logger(std::move(logger))
{ }

std::optional<TKey> TJobDirectorySnapshot::ComputeMessageKey(
    const TComputationId& computationId,
    const TMessage& message) const
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    auto it = GroupBySchemas_.find(computationId);
    if (it == GroupBySchemas_.end()) {
        // TODO(gryzlov-ad): Suppress warning on each iteration to avoid message flood when controller dies.
        YT_LOG_WARNING("Trying to get job channel for an unknown computation (ComputationId: %v, StreamId: %v)",
            computationId,
            message.StreamId);
        return std::nullopt;
    }

    auto key = ConvertPayloadToNewSchema(
        message.Payload,
        message.PayloadSchema,
        it->second,
        ConverterCache_);

    return TKey(std::move(key).Underlying());
}

const TMessageRoute* TJobDirectorySnapshot::FindRouteByKey(const TComputationId& computationId, const TKey& key) const
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    auto computationIt = Computations_.find(computationId);
    if (computationIt == Computations_.end()) {
        return nullptr;
    }
    const auto& routes = computationIt->second.UpperKeyToRoute;
    auto it = std::upper_bound(
        routes.begin(),
        routes.end(),
        key,
        [] (const TKey& key, const auto& entry) {
            return key < entry.first;
        });
    if (it == routes.end() || !it->second.KeyRange.Contains(key)) {
        return nullptr;
    }
    return &it->second;
}

i64 TJobDirectorySnapshot::GetPartitionCount(const TComputationId& computationId) const
{
    YT_ASSERT_THREAD_AFFINITY_ANY();
    auto it = Computations_.find(computationId);
    return it == Computations_.end() ? 0 : it->second.ExecutingPartitionCount;
}

bool TJobDirectorySnapshot::IsJobAlive(TJobId jobId) const
{
    YT_ASSERT_THREAD_AFFINITY_ANY();
    return AliveJobs_.contains(jobId);
}

const THashSet<TJobId>& TJobDirectorySnapshot::RoutableJobs() const
{
    return RoutableJobs_;
}

const THashSet<std::string>& TJobDirectorySnapshot::Workers() const
{
    return Workers_;
}

////////////////////////////////////////////////////////////////////////////////

TJobDirectoryDiff ComputeJobDirectoryDiff(
    const TJobDirectorySnapshotPtr& from,
    const TJobDirectorySnapshotPtr& to)
{
    TJobDirectoryDiff diff;

    auto fromWorkers = from ? from->Workers() : THashSet<std::string>{};
    auto toWorkers = to ? to->Workers() : THashSet<std::string>{};

    // Jobs are derived from routable jobs: only those can have an active connection.
    auto fromJobs = from ? from->RoutableJobs() : THashSet<TJobId>{};
    auto toJobs = to ? to->RoutableJobs() : THashSet<TJobId>{};

    for (const auto& jobId : toJobs) {
        if (!fromJobs.contains(jobId)) {
            diff.AddedJobs.push_back(jobId);
        }
    }
    for (const auto& jobId : fromJobs) {
        if (!toJobs.contains(jobId)) {
            diff.RemovedJobs.push_back(jobId);
        }
    }
    for (const auto& address : fromWorkers) {
        if (!toWorkers.contains(address)) {
            diff.RemovedWorkers.push_back(address);
        }
    }

    return diff;
}

////////////////////////////////////////////////////////////////////////////////

class TJobDirectory
    : public IJobDirectory
{
public:
    TJobDirectory(IPayloadConverterCachePtr converterCache, NLogging::TLogger logger)
        : Logger(std::move(logger))
        , ConverterCache_(std::move(converterCache))
        , Snapshot_(New<TJobDirectorySnapshot>(
            THashMap<TComputationId, NTableClient::TTableSchemaPtr>{},
            ConverterCache_,
            THashMap<TComputationId, TJobDirectorySnapshot::TComputationRouting>{},
            THashSet<TJobId>{},
            THashSet<TJobId>{},
            THashSet<std::string>{},
            Logger))
    { }

    void Reconfigure(const TFlowLayoutPtr& flowLayout, const TPipelineSpecPtr& pipelineSpec) override
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        THashMap<TComputationId, NTableClient::TTableSchemaPtr> groupBySchemas;
        for (const auto& [computationId, computationSpec] : pipelineSpec->Computations) {
            groupBySchemas[computationId] = computationSpec->GroupBySchema;
        }

        THashMap<TComputationId, TJobDirectorySnapshot::TComputationRouting> computations;
        THashSet<TJobId> routableJobs;
        THashSet<TJobId> aliveJobs;
        THashSet<std::string> workers;

        for (const auto& [partitionId, partition] : flowLayout->Partitions) {
            if (partition->State == EPartitionState::Executing) {
                ++computations[partition->ComputationId].ExecutingPartitionCount;
            }
        }

        for (const auto& [jobId, job] : flowLayout->Jobs) {
            workers.insert(job->WorkerAddress);

            // Every layout job is alive regardless of partition state or key range: a job distributes
            // messages while Completing/Interrupting too, not only while Executing.
            aliveJobs.insert(jobId);

            auto partitionIt = flowLayout->Partitions.find(job->PartitionId);
            if (partitionIt == flowLayout->Partitions.end() || partitionIt->second->State != EPartitionState::Executing) {
                continue;
            }
            const auto& partition = partitionIt->second;

            if (!partition->LowerKey || !partition->UpperKey) {
                continue;
            }

            computations[partition->ComputationId].UpperKeyToRoute.emplace_back(
                *partition->UpperKey,
                TMessageRoute(
                    partition->PartitionId,
                    jobId,
                    job->WorkerAddress,
                    TKeyRange(*partition->LowerKey, *partition->UpperKey)));
            routableJobs.insert(jobId);

            YT_LOG_DEBUG("Add job to job directory (ComputationId: %v, JobId: %v, PartitionId: %v, "
                "LowerKey: %v, UpperKey: %v, WorkerAddress: %v, PartitionState: %v)",
                partition->ComputationId,
                jobId,
                partition->PartitionId,
                partition->LowerKey,
                partition->UpperKey,
                job->WorkerAddress,
                partition->State);
        }

        // Jobs are iterated in unspecified order; sort each computation's routes by upper key for binary search.
        for (auto& [computationId, routing] : computations) {
            SortBy(routing.UpperKeyToRoute, [] (const auto& entry) -> const auto& {
                return entry.first;
            });
        }

        auto snapshot = New<TJobDirectorySnapshot>(
            std::move(groupBySchemas),
            ConverterCache_,
            std::move(computations),
            std::move(routableJobs),
            std::move(aliveJobs),
            std::move(workers),
            Logger);
        Snapshot_.Store(snapshot);
        SnapshotPublished_.Fire(snapshot);
    }

    TJobDirectorySnapshotPtr GetSnapshot() const override
    {
        return Snapshot_.Acquire();
    }

    i64 GetPartitionCount(const TComputationId& computationId) const override
    {
        return Snapshot_.Acquire()->GetPartitionCount(computationId);
    }

    std::optional<TMessageRoute> FindRouteByKey(const TComputationId& computationId, const TKey& key) const override
    {
        auto snapshot = Snapshot_.Acquire();
        if (const auto* route = snapshot->FindRouteByKey(computationId, key)) {
            return *route;
        }
        return std::nullopt;
    }

    DEFINE_SIGNAL_OVERRIDE(TSnapshotPublishedSignature, SnapshotPublished);

private:
    const NLogging::TLogger Logger;
    const IPayloadConverterCachePtr ConverterCache_;

    TAtomicIntrusivePtr<TJobDirectorySnapshot> Snapshot_;
};

////////////////////////////////////////////////////////////////////////////////

IJobDirectoryPtr CreateJobDirectory(IPayloadConverterCachePtr converterCache, const NLogging::TLogger& logger)
{
    return New<TJobDirectory>(std::move(converterCache), logger);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
