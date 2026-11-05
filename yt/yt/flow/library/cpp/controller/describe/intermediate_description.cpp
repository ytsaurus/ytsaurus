#include "intermediate_description.h"

namespace NYT::NFlow::NDescribe {

////////////////////////////////////////////////////////////////////////////////

void TPartitionIntermediateDescription::Register(TRegistrar registrar)
{
    registrar.Parameter("partition", &TThis::Partition)
        .Default();
    registrar.Parameter("job", &TThis::Job)
        .Default();
    registrar.Parameter("partition_job_status", &TThis::PartitionJobStatus)
        .Default();
    registrar.Parameter("partition_ephemeral_state", &TThis::PartitionEphemeralState)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

TPartitionIntermediateDescription GetPartitionIntermediateDescription(const TFlowViewPtr& flowView, const TPartitionId& partitionId)
{
    const auto& layout = flowView->State->ExecutionSpec->Layout;
    auto& partitions = layout->Partitions; // find(), end() are not const methods.
    auto partitionIt = partitions.find(partitionId);
    THROW_ERROR_EXCEPTION_IF(partitionIt == partitions.end(), "Partition %Qv not found", partitionId);
    return GetPartitionIntermediateDescription(flowView, partitionIt->second);
}

template <class TWorkerIdPtr>
TPartitionIntermediateDescription GetPartitionIntermediateDescription(
    const TFlowViewPtr& flowView,
    const TPartitionPtr& partition,
    const TWorkerIdPtr workerIdPtr)
{
    const auto& layout = flowView->State->ExecutionSpec->Layout;

    TPartitionIntermediateDescription description;
    description.Partition = partition;

    if (partition->CurrentJobId) {
        if (auto jobIt = layout->Jobs.find(*partition->CurrentJobId); jobIt != layout->Jobs.end()) {
            description.Job = jobIt->second;
        }
    }
    if constexpr (!std::is_same_v<TWorkerIdPtr, std::nullptr_t>) {
        if (!description.Job || description.Job->WorkerAddress != *workerIdPtr) {
            return {};
        }
    }

    description.PartitionJobStatus = GetOrDefault(flowView->Feedback->PartitionJobStatuses, partition->PartitionId);
    description.PartitionEphemeralState = GetOrDefault(flowView->EphemeralState->Partitions, partition->PartitionId);

    return description;
}

TPartitionIntermediateDescription GetPartitionIntermediateDescription(
    const TFlowViewPtr& flowView,
    const TPartitionPtr& partition)
{
    return GetPartitionIntermediateDescription(flowView, partition, nullptr);
}

////////////////////////////////////////////////////////////////////////////////

template <class TComputationIdPtr, class TWorkerIdPtr, class TCollector>
void GetAllPartitionIntermediateDescriptions(
    const TFlowViewPtr& flowView,
    const TComputationIdPtr computationPtr,
    const TWorkerIdPtr workerPtr,
    TCollector&& collector)
{
    for (const auto& [partitionId, partition] : flowView->State->ExecutionSpec->Layout->Partitions) {
        if constexpr (!std::is_same_v<TComputationIdPtr, std::nullptr_t>) {
            if (partition->ComputationId != *computationPtr) {
                continue;
            }
        }

        auto description = GetPartitionIntermediateDescription(flowView, partition, workerPtr);
        if constexpr (!std::is_same_v<TWorkerIdPtr, std::nullptr_t>) {
            if (!description.Partition) {
                continue;
            }
        }
        collector(std::move(description));
    }
}

////////////////////////////////////////////////////////////////////////////////

THashMap<std::string, std::vector<TPartitionIntermediateDescription>> GetWorkersPartitionIntermediateDescriptions(
    const TFlowViewPtr& flowView,
    const std::optional<std::string>& worker)
{
    THashMap<std::string, std::vector<TPartitionIntermediateDescription>> result;
    if (worker.has_value()) {
        auto& workerResult = result[*worker];
        GetAllPartitionIntermediateDescriptions(flowView, nullptr, &*worker, [&] (auto&& partitionDesc) {
            workerResult.push_back(std::forward<decltype(partitionDesc)>(partitionDesc));
        });
    } else {
        GetAllPartitionIntermediateDescriptions(flowView, nullptr, nullptr, [&] (auto&& partitionDesc) {
            if (!partitionDesc.Job) {
                return;
            }
            result[partitionDesc.Job->WorkerAddress].push_back(std::forward<decltype(partitionDesc)>(partitionDesc));
        });
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

THashMap<TComputationId, std::vector<TPartitionIntermediateDescription>> GetComputationPartitionIntermediateDescriptions(
    const TFlowViewPtr& flowView,
    const std::optional<TComputationId>& computation)
{
    THashMap<TComputationId, std::vector<TPartitionIntermediateDescription>> result;
    if (computation.has_value()) {
        auto& computationResult = result[*computation];
        GetAllPartitionIntermediateDescriptions(flowView, &*computation, nullptr, [&] (auto&& partitionDesc) {
            computationResult.push_back(std::forward<decltype(partitionDesc)>(partitionDesc));
        });
    } else {
        GetAllPartitionIntermediateDescriptions(flowView, nullptr, nullptr, [&] (auto&& partitionDesc) {
            result[partitionDesc.Partition->ComputationId].push_back(std::forward<decltype(partitionDesc)>(partitionDesc));
        });
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NDescribe
