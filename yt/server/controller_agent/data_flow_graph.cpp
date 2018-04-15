#include "data_flow_graph.h"

#include "input_chunk_mapping.h"
#include "serialize.h"

#include <yt/server/chunk_pools/chunk_pool.h>

#include <yt/server/table_server/virtual.h>

#include <yt/ytlib/chunk_client/data_statistics.h>
#include <yt/ytlib/chunk_client/public.h>

#include <yt/core/ytree/fluent.h>
#include <yt/core/ytree/virtual.h>

namespace NYT {
namespace NControllerAgent {

using namespace NYTree;
using namespace NChunkClient::NProto;
using namespace NChunkClient;
using namespace NTableServer;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

using TVertexDescriptor = TString;

TString TDataFlowGraph::SourceDescriptor("source");
TString TDataFlowGraph::SinkDescriptor("sink");

DECLARE_REFCOUNTED_CLASS(TVertex)
DECLARE_REFCOUNTED_CLASS(TEdge)
DECLARE_REFCOUNTED_CLASS(TLivePreview)

////////////////////////////////////////////////////////////////////////////////

void TEdgeDescriptor::Persist(const TPersistenceContext& context)
{
    using NYT::Persist;

    Persist(context, ChunkMapping);
    Persist(context, DestinationPool);
    Persist(context, RequiresRecoveryInfo);
    Persist(context, TableWriterOptions);
    Persist(context, TableUploadOptions);
    Persist(context, TableWriterConfig);
    Persist(context, Timestamp);
    Persist(context, CellTag);
    Persist(context, ImmediatelyUnstageChunkLists);
    Persist(context, IsFinalOutput);
    Persist(context, LivePreviewIndex);
}

TEdgeDescriptor& TEdgeDescriptor::operator =(const TEdgeDescriptor& other)
{
    DestinationPool = other.DestinationPool;
    ChunkMapping = other.ChunkMapping;
    RequiresRecoveryInfo = other.RequiresRecoveryInfo;
    TableWriterOptions = CloneYsonSerializable(other.TableWriterOptions);
    TableUploadOptions = other.TableUploadOptions;
    TableWriterConfig = other.TableWriterConfig;
    Timestamp = other.Timestamp;
    CellTag = other.CellTag;
    ImmediatelyUnstageChunkLists = other.ImmediatelyUnstageChunkLists;
    IsFinalOutput = other.IsFinalOutput;
    LivePreviewIndex = other.LivePreviewIndex;

    return *this;
}

////////////////////////////////////////////////////////////////////////////////

class TLivePreview
    : public TRefCounted
{
public:
    DEFINE_BYVAL_RO_PROPERTY(NYTree::IYPathServicePtr, Service);
    DEFINE_BYREF_RW_PROPERTY(THashSet<NChunkClient::TInputChunkPtr>, Chunks);

public:
    TLivePreview()
    {
        Initialize();
    }

    void Persist(const TPersistenceContext& context)
    {
        using NYT::Persist;

        Persist<TSetSerializer<TDefaultSerializer, TUnsortedTag>>(context, Chunks_);

        if (context.IsLoad()) {
            Initialize();
        }
    }

private:
    void Initialize()
    {
        Service_ = New<TVirtualStaticTable>(Chunks_);
    }
};

DEFINE_REFCOUNTED_TYPE(TLivePreview)

////////////////////////////////////////////////////////////////////////////////

class TEdge
    : public TRefCounted
{
public:
    DEFINE_BYVAL_RO_PROPERTY(IYPathServicePtr, Service);
    DEFINE_BYREF_RW_PROPERTY(TDataStatistics, Statistics);

public:
    TEdge()
    {
        Initialize();
    }

    void Persist(const TPersistenceContext& context)
    {
        using NYT::Persist;

        Persist(context, Statistics_);

        if (context.IsLoad()) {
            Initialize();
        }
    }

private:
    void Initialize()
    {
        auto service = New<TCompositeMapService>()
            ->AddChild("statistics", IYPathService::FromProducer(BIND([weakThis = MakeWeak(this)] (IYsonConsumer* consumer) {
                if (auto this_ = weakThis.Lock()) {
                    BuildYsonFluently(consumer)
                        .Value(this_->Statistics_);
                }
            })));
        service->SetOpaque(false);
        Service_ = std::move(service);
    }
};

DEFINE_REFCOUNTED_TYPE(TEdge)

////////////////////////////////////////////////////////////////////////////////

class TVertex
    : public TRefCounted
{
public:
    DEFINE_BYVAL_RO_PROPERTY(NYTree::IYPathServicePtr, Service);
    DEFINE_BYREF_RW_PROPERTY(TProgressCounterPtr, JobCounter, New<TProgressCounter>(0));
    DEFINE_BYVAL_RW_PROPERTY(EJobType, JobType);

    using TLivePreviewList = std::vector<TLivePreviewPtr>;
    DEFINE_BYVAL_RO_PROPERTY(TLivePreviewList, LivePreviews);

    using TEdgeMap = THashMap<TVertexDescriptor, TEdgePtr>;
    DEFINE_BYREF_RO_PROPERTY(TEdgeMap, Edges);

public:
    TVertex()
    {
        Initialize();
    }

    const TEdgePtr& GetOrRegisterEdge(const TVertexDescriptor& to)
    {
        auto it = Edges_.find(to);
        if (it == Edges_.end()) {
            auto& edge = Edges_[to];
            edge = New<TEdge>();
            return edge;
        } else {
            return it->second;
        }
    }

    void Persist(const TPersistenceContext& context)
    {
        using NYT::Persist;

        Persist(context, JobCounter_);
        Persist(context, JobType_);
        Persist(context, LivePreviews_);

        if (context.IsLoad()) {
            Initialize();
        }
    }

    void RegisterLivePreviewChunk(int index, TInputChunkPtr chunk)
    {
        if (index >= LivePreviews_.size()) {
            LivePreviews_.resize(index + 1);
        }
        if (!LivePreviews_[index]) {
            LivePreviews_[index] = New<TLivePreview>();
        }

        YCHECK(LivePreviews_[index]->Chunks().insert(std::move(chunk)).second);
    }

    void UnregisterLivePreviewChunk(int index, TInputChunkPtr chunk)
    {
        YCHECK(0 <= index && index < LivePreviews_.size());
        YCHECK(LivePreviews_[index]);

        YCHECK(LivePreviews_[index]->Chunks().erase(std::move(chunk)));
    }

private:
    void Initialize()
    {
        using TEdgeMapService = NYTree::TCollectionBoundMapService<TEdgeMap>;
        auto edgeMapService = New<TEdgeMapService>(Edges_);
        edgeMapService->SetOpaque(false);

        using TLivePreviewListService = NYTree::TCollectionBoundListService<TLivePreviewList>;
        auto livePreviewService = New<TLivePreviewListService>(LivePreviews_);

        auto service = New<TCompositeMapService>();
        service->AddChild("edges", edgeMapService);
        service->AddChild("live_previews", livePreviewService);
        service->SetOpaque(false);

        Service_ = std::move(service);
    }
};

DEFINE_REFCOUNTED_TYPE(TVertex)

////////////////////////////////////////////////////////////////////////////////

class TDataFlowGraph::TImpl
    : public TRefCounted
{
public:
    TImpl()
    {
        Initialize();
    }

    IYPathServicePtr GetService() const
    {
        return Service_;
    }

    const std::vector<TVertexDescriptor>& GetTopologicalOrdering() const
    {
        return TopologicalOrdering_.GetOrdering();
    }

    void Persist(const TPersistenceContext& context)
    {
        using NYT::Persist;

        Persist(context, TotalJobCounter_);
        Persist(context, Vertices_);
        Persist(context, TopologicalOrdering_);

        if (context.IsLoad()) {
            Initialize();
        }
    }

    void RegisterFlow(
        const TDataFlowGraph::TVertexDescriptor& from,
        const TDataFlowGraph::TVertexDescriptor& to,
        const TDataStatistics& statistics)
    {
        TopologicalOrdering_.AddEdge(from, to);
        const auto& edge = GetOrRegisterEdge(from, to);
        edge->Statistics() += statistics;
    }

    void RegisterCounter(
        const TVertexDescriptor& descriptor,
        const TProgressCounterPtr& counter,
        EJobType jobType)
    {
        const auto& vertex = GetOrRegisterVertex(descriptor);
        vertex->SetJobType(jobType);
        counter->SetParent(vertex->JobCounter());
    }

    void RegisterLivePreviewChunk(const TVertexDescriptor& descriptor, int index, TInputChunkPtr chunk)
    {
        const auto& vertex = GetOrRegisterVertex(descriptor);
        vertex->RegisterLivePreviewChunk(index, std::move(chunk));
    }

    void UnregisterLivePreviewChunk(const TVertexDescriptor& descriptor, int index, TInputChunkPtr chunk)
    {
        const auto& vertex = GetOrRegisterVertex(descriptor);
        vertex->UnregisterLivePreviewChunk(index, std::move(chunk));
    }

    void BuildLegacyYson(TFluentMap fluent) const
    {
        auto topologicalOrdering = GetTopologicalOrdering();
        fluent
            .Item("vertices").BeginMap()
                .DoFor(topologicalOrdering, [&] (TFluentMap fluent, const TVertexDescriptor& vertexDescriptor) {
                    auto it = Vertices_.find(vertexDescriptor);
                    if (it != Vertices_.end()) {
                        fluent
                            .Item(vertexDescriptor).BeginMap()
                                .Item("job_counter").Value(it->second->JobCounter())
                                .Item("job_type").Value(it->second->GetJobType())
                            .EndMap();
                    }
                })
                .Item("total").BeginMap()
                    .Item("job_counter").Value(TotalJobCounter_)
                .EndMap()
            .EndMap()
            .Item("edges")
                .DoMapFor(topologicalOrdering, [&] (TFluentMap fluent, const TVertexDescriptor& from) {
                    auto it = Vertices_.find(from);
                    if (it != Vertices_.end()) {
                        fluent.Item(from)
                            .DoMapFor(it->second->Edges(), [&] (TFluentMap fluent, const auto& pair) {
                                auto to = pair.first;
                                const auto& edge = pair.second;
                                fluent
                                    .Item(to).BeginMap()
                                        .Item("statistics").Value(edge->Statistics())
                                    .EndMap();
                            });
                    }
                })
            .Item("topological_ordering").List(topologicalOrdering);
    }

private:
    using TVertexMap = THashMap<TVertexDescriptor, TVertexPtr>;
    TVertexMap Vertices_;

    TProgressCounterPtr TotalJobCounter_ = New<TProgressCounter>(0);

    TIncrementalTopologicalOrdering<TVertexDescriptor> TopologicalOrdering_;

    NYTree::IYPathServicePtr Service_;

    void Initialize()
    {
        using TVertexMapService = TCollectionBoundMapService<TVertexMap>;

        auto vertexMapService = New<TVertexMapService>(Vertices_);
        vertexMapService->SetOpaque(false);
        auto service = New<TCompositeMapService>()
            ->AddChild("vertices", std::move(vertexMapService))
            ->AddChild("topological_ordering", IYPathService::FromProducer(BIND([weakThis = MakeWeak(this)] (IYsonConsumer* consumer) {
                if (auto this_ = weakThis.Lock()) {
                    BuildYsonFluently(consumer)
                        .List(this_->GetTopologicalOrdering());
                }
            })));
        service->SetOpaque(false);
        Service_ = std::move(service);
    }

    const TVertexPtr& GetOrRegisterVertex(const TVertexDescriptor& descriptor)
    {
        auto it = Vertices_.find(descriptor);
        if (it == Vertices_.end()) {
            auto& vertex = Vertices_[descriptor];
            vertex = New<TVertex>();
            vertex->JobCounter()->SetParent(TotalJobCounter_);
            return vertex;
        } else {
            return it->second;
        }
    }

    const TEdgePtr& GetOrRegisterEdge(const TVertexDescriptor& from, const TVertexDescriptor& to)
    {
        auto& vertex = GetOrRegisterVertex(from);
        return vertex->GetOrRegisterEdge(to);
    }
};

////////////////////////////////////////////////////////////////////////////////

TDataFlowGraph::TDataFlowGraph()
    : Impl_(New<TImpl>())
{ }

TDataFlowGraph::~TDataFlowGraph() = default;

IYPathServicePtr TDataFlowGraph::GetService() const
{
    return Impl_->GetService();
}

void TDataFlowGraph::Persist(const TPersistenceContext& context)
{
    Impl_->Persist(context);
}

void TDataFlowGraph::RegisterFlow(
    const TVertexDescriptor& from,
    const TVertexDescriptor& to,
    const NChunkClient::NProto::TDataStatistics& statistics)
{
    Impl_->RegisterFlow(from, to, statistics);
}

void TDataFlowGraph::RegisterCounter(
    const TVertexDescriptor& vertex,
    const TProgressCounterPtr& counter,
    EJobType jobType)
{
    Impl_->RegisterCounter(vertex, counter, jobType);
}

void TDataFlowGraph::RegisterLivePreviewChunk(const TVertexDescriptor& descriptor, int index, TInputChunkPtr chunk)
{
    Impl_->RegisterLivePreviewChunk(descriptor, index, std::move(chunk));
}

void TDataFlowGraph::UnregisterLivePreviewChunk(const TVertexDescriptor& descriptor, int index, TInputChunkPtr chunk)
{
    Impl_->UnregisterLivePreviewChunk(descriptor, index, std::move(chunk));
}

void TDataFlowGraph::BuildLegacyYson(TFluentMap fluent) const
{
    Impl_->BuildLegacyYson(fluent);
}

/////////////////////////////////////////////////////////////////////////////////

} // namespace NControllerAgent
} // namespace NYT
