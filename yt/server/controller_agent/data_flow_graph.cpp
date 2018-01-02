#include "data_flow_graph.h"

#include "serialize.h"

#include <yt/server/chunk_pools/chunk_pool.h>

#include <yt/ytlib/chunk_client/data_statistics.h>

#include <yt/core/ytree/fluent.h>

namespace NYT {
namespace NControllerAgent {

using namespace NYTree;
using namespace NChunkClient::NProto;

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
    Persist(context, IsFinalOutput);
}

TEdgeDescriptor& TEdgeDescriptor::operator =(const TEdgeDescriptor& other)
{
    DestinationPool = other.DestinationPool;
    RequiresRecoveryInfo = other.RequiresRecoveryInfo;
    TableWriterOptions = CloneYsonSerializable(other.TableWriterOptions);
    TableUploadOptions = other.TableUploadOptions;
    TableWriterConfig = other.TableWriterConfig;
    Timestamp = other.Timestamp;
    CellTag = other.CellTag;
    ImmediatelyUnstageChunkLists = other.ImmediatelyUnstageChunkLists;
    IsFinalOutput = other.IsFinalOutput;

    return *this;
}

////////////////////////////////////////////////////////////////////////////////

void TDataFlowGraph::BuildYson(TFluentMap fluent) const
{
    auto topologicalOrdering = GetTopologicalOrdering();
    fluent
        .Item("vertices").BeginMap()
            .DoFor(topologicalOrdering, [&] (TFluentMap fluent, TVertexDescriptor vertexDescriptor) {
                auto it = JobCounters_.find(vertexDescriptor);
                if (it != JobCounters_.end()) {
                    fluent
                        .Item(FormatEnum(vertexDescriptor)).BeginMap()
                            .Item("job_counter").Value(JobCounters_.at(vertexDescriptor))
                        .EndMap();
                }
            })
            .Item("total").BeginMap()
                .Item("job_counter").Value(TotalJobCounter_)
            .EndMap()
        .EndMap()
        .Item("edges")
            .DoMapFor(topologicalOrdering, [&] (TFluentMap fluent, TVertexDescriptor from) {
                auto it = Flow_.find(from);
                if (it != Flow_.end()) {
                    fluent.Item(FormatEnum(from))
                        .DoMapFor(it->second, [&] (TFluentMap fluent, const TFlowMap::value_type::second_type::value_type& pair) {
                            auto to = pair.first;
                            const auto& flow = pair.second;
                            fluent
                                .Item(FormatEnum(to)).BeginMap()
                                    .Item("statistics").Value(flow)
                                .EndMap();
                        });
                }
            })
        .Item("topological_ordering").List(topologicalOrdering);
}

const TProgressCounterPtr& TDataFlowGraph::JobCounter(TVertexDescriptor vertexDescriptor)
{
    auto& progressCounter = JobCounters_[vertexDescriptor];
    if (!progressCounter) {
        progressCounter = New<TProgressCounter>(0);
        progressCounter->SetParent(TotalJobCounter_);
    }
    return progressCounter;
}

std::vector<TDataFlowGraph::TVertexDescriptor> TDataFlowGraph::GetTopologicalOrdering() const
{
    const auto& topologicalOrdering = TopologicalOrdering_.GetOrdering();
    std::vector<TVertexDescriptor> result;
    result.reserve(topologicalOrdering.size());

    for (const auto& element : topologicalOrdering) {
        result.emplace_back(static_cast<TVertexDescriptor>(element));
    }

    return result;
}

void TDataFlowGraph::Persist(const TPersistenceContext& context)
{
    using NYT::Persist;
    Persist(context, JobCounters_);
    Persist(context, TotalJobCounter_);
    Persist(context, Flow_);
    Persist(context, TopologicalOrdering_);
}

void TDataFlowGraph::RegisterFlow(
    TDataFlowGraph::TVertexDescriptor from,
    TDataFlowGraph::TVertexDescriptor to,
    const TDataStatistics& statistics)
{
    TopologicalOrdering_.AddEdge(static_cast<int>(from), static_cast<int>(to));
    auto& flow = Flow_[from][to];
    flow += statistics;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NControllerAgent
} // namespace NYT