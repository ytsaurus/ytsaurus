#include "data_flow_graph.h"

#include <yt/server/chunk_pools/chunk_pool.h>

#include <yt/core/ytree/fluent.h>

namespace NYT {
namespace NControllerAgent {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

void TEdgeDescriptor::Persist(const TPersistenceContext& context)
{
    using NYT::Persist;

    Persist(context, DestinationPool);
    Persist(context, RequiresRecoveryInfo);
    Persist(context, TableWriterOptions);
    Persist(context, TableUploadOptions);
    Persist(context, TableWriterConfig);
    Persist(context, Timestamp);
    Persist(context, CellTag);
    Persist(context, ImmediatelyUnstageChunkLists);
}

////////////////////////////////////////////////////////////////////////////////

void TDataFlowGraph::BuildYson(NYson::IYsonConsumer* consumer) const
{
    BuildYsonMapFluently(consumer)
        .Item("vertices").BeginMap()
            .DoFor(GetTopologicalOrder(), [&] (TFluentMap fluent, EJobType jobType) {
                fluent
                    .Item(FormatEnum(jobType)).BeginMap()
                        .Item("job_counter").Value(JobCounters_.at(jobType))
                    .EndMap();
            })
            .Item("total").BeginMap()
                .Item("job_counter").Value(TotalJobCounter_)
            .EndMap()
        .EndMap();
}

const TProgressCounterPtr& TDataFlowGraph::JobCounter(EJobType jobType)
{
    auto& progressCounter = JobCounters_[jobType];
    if (!progressCounter) {
        progressCounter = New<TProgressCounter>(0);
        progressCounter->SetParent(TotalJobCounter_);
    }
    return progressCounter;
}

std::vector<EJobType> TDataFlowGraph::GetTopologicalOrder() const
{
    // TODO(max42): implement a correct topological order here.
    std::vector<EJobType> result;
    result.reserve(JobCounters_.size());
    for (const auto& item : JobCounters_) {
        result.emplace_back(item.first);
    }
    return result;
}

void TDataFlowGraph::Persist(const TPersistenceContext& context)
{
    using NYT::Persist;
    Persist(context, JobCounters_);
    Persist(context, TotalJobCounter_);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NControllerAgent
} // namespace NYT