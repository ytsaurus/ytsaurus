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
                    .Item(FormatEnum(jobType)).Value(ProgressCounters_.at(jobType));
            })
        .EndMap();
}

const TProgressCounterPtr& TDataFlowGraph::ProgressCounter(EJobType jobType)
{
    return ProgressCounters_[jobType];
}

std::vector<EJobType> TDataFlowGraph::GetTopologicalOrder() const
{
    // TODO(max42): implement a correct topological order here.
    std::vector<EJobType> result;
    result.reserve(ProgressCounters_.size());
    for (const auto& item : ProgressCounters_) {
        result.emplace_back(item.first);
    }
    return result;
}

void TDataFlowGraph::Persist(const TPersistenceContext& context)
{
    using NYT::Persist;

    Persist(context, ProgressCounters_);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NControllerAgent
} // namespace NYT