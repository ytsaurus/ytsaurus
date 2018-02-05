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

TString TDataFlowGraph::SourceDescriptor("source");
TString TDataFlowGraph::SinkDescriptor("sink");

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
            .DoFor(topologicalOrdering, [&] (TFluentMap fluent, const TVertexDescriptor& vertexDescriptor) {
                auto it = JobCounters_.find(vertexDescriptor);
                if (it != JobCounters_.end()) {
                    fluent
                        .Item(vertexDescriptor).BeginMap()
                            .Item("job_counter").Value(JobCounters_.at(vertexDescriptor))
                            .Item("job_type").Value(JobTypes_.at(vertexDescriptor))
                        .EndMap();
                }
            })
            .Item("total").BeginMap()
                .Item("job_counter").Value(TotalJobCounter_)
            .EndMap()
        .EndMap()
        .Item("edges")
            .DoMapFor(topologicalOrdering, [&] (TFluentMap fluent, const TVertexDescriptor& from) {
                auto it = Flow_.find(from);
                if (it != Flow_.end()) {
                    fluent.Item(from)
                        .DoMapFor(it->second, [&] (TFluentMap fluent, const TFlowMap::value_type::second_type::value_type& pair) {
                            auto to = pair.first;
                            const auto& flow = pair.second;
                            fluent
                                .Item(to).BeginMap()
                                    .Item("statistics").Value(flow)
                                .EndMap();
                        });
                }
            })
        .Item("topological_ordering").List(topologicalOrdering);
}

const std::vector<TDataFlowGraph::TVertexDescriptor>& TDataFlowGraph::GetTopologicalOrdering() const
{
    return TopologicalOrdering_.GetOrdering();
}

void TDataFlowGraph::Persist(const TPersistenceContext& context)
{
    using NYT::Persist;
    Persist(context, JobCounters_);
    Persist(context, JobTypes_);
    Persist(context, TotalJobCounter_);
    Persist(context, Flow_);
    Persist(context, TopologicalOrdering_);
}

void TDataFlowGraph::RegisterFlow(
    const TDataFlowGraph::TVertexDescriptor& from,
    const TDataFlowGraph::TVertexDescriptor& to,
    const TDataStatistics& statistics)
{
    TopologicalOrdering_.AddEdge(from, to);
    auto& flow = Flow_[from][to];
    flow += statistics;
}

void TDataFlowGraph::RegisterTask(
    const TVertexDescriptor& vertex,
    const TProgressCounterPtr& taskCounter,
    EJobType jobType)
{
    auto& vertexCounter = JobCounters_[vertex];
    if (!vertexCounter) {
        vertexCounter = New<TProgressCounter>(0);
        vertexCounter->SetParent(TotalJobCounter_);
        JobTypes_[vertex] = jobType;
    }
    taskCounter->SetParent(vertexCounter);

    // Ensure that job type for each vertex is unique.
    auto it = JobTypes_.find(vertex);
    YCHECK(it != JobTypes_.end());
    YCHECK(it->second == jobType);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NControllerAgent
} // namespace NYT
