#include "data_flow_graph.h"
#include "live_preview.h"

#include <yt/yt/server/controller_agent/virtual.h>

#include <yt/yt/server/lib/chunk_pools/chunk_pool.h>
#include <yt/yt/server/lib/chunk_pools/input_chunk_mapping.h>

#include <yt/yt/server/lib/misc/job_table_schema.h>

#include <yt/yt/client/chunk_client/data_statistics.h>
#include <yt/yt/client/node_tracker_client/node_directory.h>

#include <yt/yt/ytlib/chunk_client/input_chunk.h>

#include <yt/yt/ytlib/controller_agent/serialize.h>

#include <yt/yt/ytlib/table_client/config.h>

#include <yt/yt/core/ytree/fluent.h>
#include <yt/yt/core/ytree/virtual.h>

#include <util/generic/cast.h>

namespace NYT::NControllerAgent::NControllers {

using namespace NYTree;
using namespace NChunkClient::NProto;
using namespace NChunkClient;
using namespace NLogging;
using namespace NTableClient;
using namespace NYson;
using namespace NNodeTrackerClient;
using namespace NServer;

////////////////////////////////////////////////////////////////////////////////

using TVertexDescriptor = TString;

const TVertexDescriptor TDataFlowGraph::SourceDescriptor("source");
const TVertexDescriptor TDataFlowGraph::SinkDescriptor("sink");
const TVertexDescriptor TDataFlowGraph::CoreDescriptor("core");
const TVertexDescriptor TDataFlowGraph::StderrDescriptor("stderr");

DECLARE_REFCOUNTED_CLASS(TVertex)
DECLARE_REFCOUNTED_CLASS(TEdge)

////////////////////////////////////////////////////////////////////////////////

void TStreamDescriptorBase::Persist(const TPersistenceContext &context)
{
    using NYT::Persist;

    Persist<TVectorSerializer<TNonNullableIntrusivePtrSerializer<>>>(context, StreamSchemas);
}

////////////////////////////////////////////////////////////////////////////////

TInputStreamDescriptorPtr TInputStreamDescriptor::FromOutputStreamDescriptor(
    const TOutputStreamDescriptorPtr& outputStreamDescriptor)
{
    auto inputStreamDescriptor = New<TInputStreamDescriptor>();
    inputStreamDescriptor->StreamSchemas = outputStreamDescriptor->StreamSchemas;
    return inputStreamDescriptor;
}

TInputStreamDescriptor::TInputStreamDescriptor(TStreamDescriptorBase base)
    : TStreamDescriptorBase(std::move(base))
{ }

TInputStreamDescriptorPtr TInputStreamDescriptor::Clone() const
{
    return New<TInputStreamDescriptor>(static_cast<const TStreamDescriptorBase&>(*this));
}

void TInputStreamDescriptor::Persist(const TPersistenceContext& context)
{
    TStreamDescriptorBase::Persist(context);
}

////////////////////////////////////////////////////////////////////////////////

TOutputStreamDescriptor::TOutputStreamDescriptor(TOutputStreamDescriptorBase base)
    : TOutputStreamDescriptorBase(std::move(base))
{ }

TOutputStreamDescriptorPtr TOutputStreamDescriptor::Clone() const
{
    return New<TOutputStreamDescriptor>(static_cast<const TOutputStreamDescriptorBase&>(*this));
}

void TOutputStreamDescriptor::Persist(const TPersistenceContext& context)
{
    TStreamDescriptorBase::Persist(context);

    using NYT::Persist;

    Persist(context, ChunkMapping);
    Persist(context, DestinationPool);
    Persist(context, RequiresRecoveryInfo);
    Persist(context, TableWriterOptions);
    Persist(context, SlowMedium);
    Persist(context, TableUploadOptions);
    Persist(context, TableWriterConfig);
    Persist(context, Timestamp);
    Persist(context, CellTags);
    Persist(context, ImmediatelyUnstageChunkLists);
    Persist(context, IsOutputTableDynamic);
    Persist(context, IsFinalOutput);
    Persist(context, LivePreviewIndex);
    Persist(context, TargetDescriptor);
    Persist(context, PartitionTag);
}

////////////////////////////////////////////////////////////////////////////////

class TEdge
    : public TRefCounted
{
public:
    DEFINE_BYVAL_RO_PROPERTY(IYPathServicePtr, Service);
    DEFINE_BYREF_RW_PROPERTY(TVertexDescriptor, SourceName);
    DEFINE_BYREF_RW_PROPERTY(TVertexDescriptor, TargetName);
    DEFINE_BYREF_RW_PROPERTY(TDataStatistics, JobDataStatistics);
    DEFINE_BYREF_RW_PROPERTY(TDataStatistics, TeleportDataStatistics);

public:
    //! For persistence only.
    TEdge() = default;

    TEdge(TVertexDescriptor sourceName, TVertexDescriptor targetName)
        : SourceName_(std::move(sourceName))
        , TargetName_(std::move(targetName))
    {
        Initialize();
    }

    void Persist(const TPersistenceContext& context)
    {
        using NYT::Persist;

        Persist(context, SourceName_);
        Persist(context, TargetName_);
        Persist(context, JobDataStatistics_);
        Persist(context, TeleportDataStatistics_);

        if (context.IsLoad()) {
            Initialize();
        }
    }

    void BuildDirectionYson(TFluentMap fluent)
    {
        auto getVertexName = [] (const TVertexDescriptor& descriptor) -> TString {
            if (descriptor == TDataFlowGraph::SourceDescriptor) {
                return "input";
            } else if (descriptor == TDataFlowGraph::SinkDescriptor) {
                return "output";
            } else {
                return descriptor;
            }
        };

        fluent
            .Item("source_name").Value(getVertexName(SourceName_))
            .Item("target_name").Value(getVertexName(TargetName_))
            .Item("job_data_statistics").Value(JobDataStatistics_)
            .Item("teleport_data_statistics").Value(TeleportDataStatistics_);
    }

private:
    void Initialize()
    {
        auto service = New<TCompositeMapService>()
            // COMPAT(gritukan): Drop it in favour of job_data_statistics.
            ->AddChild("statistics", IYPathService::FromProducer(BIND_NO_PROPAGATE([this, weakThis = MakeWeak(this)] (IYsonConsumer* consumer) {
                if (auto this_ = weakThis.Lock()) {
                    BuildYsonFluently(consumer)
                        .Value(JobDataStatistics_ + TeleportDataStatistics_);
                }
            })))
            ->AddChild("job_data_statistics", IYPathService::FromProducer(BIND_NO_PROPAGATE([this, weakThis = MakeWeak(this)] (IYsonConsumer* consumer) {
                if (auto this_ = weakThis.Lock()) {
                    BuildYsonFluently(consumer)
                        .Value(JobDataStatistics_);
                }
            })))
            ->AddChild("teleport_data_statistics", IYPathService::FromProducer(BIND_NO_PROPAGATE([this, weakThis = MakeWeak(this)] (IYsonConsumer* consumer) {
                if (auto this_ = weakThis.Lock()) {
                    BuildYsonFluently(consumer)
                        .Value(TeleportDataStatistics_);
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
    DEFINE_BYREF_RW_PROPERTY(TVertexDescriptor, VertexDescriptor);
    DEFINE_BYVAL_RO_PROPERTY(NYTree::IYPathServicePtr, Service);
    DEFINE_BYREF_RW_PROPERTY(TProgressCounterPtr, JobCounter, New<TProgressCounter>());
    DEFINE_BYVAL_RW_PROPERTY(EJobType, JobType);

    using TLivePreviewList = std::vector<TLivePreviewPtr>;
    DEFINE_BYVAL_RO_PROPERTY(std::shared_ptr<TLivePreviewList>, LivePreviews, std::make_shared<TLivePreviewList>());

    using TEdgeMap = THashMap<TVertexDescriptor, TEdgePtr>;
    DEFINE_BYREF_RO_PROPERTY(std::shared_ptr<TEdgeMap>, Edges, std::make_shared<TEdgeMap>());

public:
    //! For persistence only.
    TVertex() = default;

    TVertex(
        TVertexDescriptor vertexDescriptor,
        TNodeDirectoryPtr nodeDirectory,
        TLogger logger)
        : VertexDescriptor_(std::move(vertexDescriptor))
        , NodeDirectory_(std::move(nodeDirectory))
        , Logger(std::move(logger))
    {
        Initialize();
    }

    const TEdgePtr& GetOrRegisterEdge(const TVertexDescriptor& to)
    {
        auto it = Edges_->find(to);
        if (it == Edges_->end()) {
            auto& edge = (*Edges_)[to];
            edge = New<TEdge>(VertexDescriptor_, to);
            return edge;
        } else {
            return it->second;
        }
    }

    void Persist(const TPersistenceContext& context)
    {
        using NYT::Persist;

        Persist(context, VertexDescriptor_);
        Persist(context, JobCounter_);
        Persist(context, JobType_);
        Persist(context, *LivePreviews_);
        Persist(context, *Edges_);
        Persist(context, NodeDirectory_);

        if (context.GetVersion() >= ESnapshotVersion::ValidateLivePreviewChunks) {
            Persist(context, Logger);
        } else {
            Logger = ControllerLogger();
        }

        if (context.IsLoad()) {
            Initialize();
        }
    }

    TError TryRegisterLivePreviewChunk(int index, TInputChunkPtr chunk)
    {
        if (index >= std::ssize(*LivePreviews_)) {
            LivePreviews_->resize(index + 1);
        }
        if (!(*LivePreviews_)[index]) {
            // TODO(gritukan): Pass schemas from controller.
            auto schema = New<TTableSchema>();
            if (VertexDescriptor_ == TDataFlowGraph::CoreDescriptor) {
                schema = GetCoreBlobTableSchema().ToTableSchema();
            } else if (VertexDescriptor_ == TDataFlowGraph::StderrDescriptor) {
                schema = GetStderrBlobTableSchema().ToTableSchema();
            }

            (*LivePreviews_)[index] = New<TLivePreview>(std::move(schema), NodeDirectory_, Logger);
        }

        return (*LivePreviews_)[index]->TryInsertChunk(std::move(chunk));
    }

    TError TryUnregisterLivePreviewChunk(int index, const TInputChunkPtr& chunk)
    {
        YT_VERIFY(0 <= index && index < std::ssize(*LivePreviews_));
        YT_VERIFY((*LivePreviews_)[index]);

        return (*LivePreviews_)[index]->TryEraseChunk(chunk);
    }

private:
    TNodeDirectoryPtr NodeDirectory_;
    TSerializableLogger Logger;

    void Initialize()
    {
        using TEdgeMapService = NYTree::TCollectionBoundMapService<TEdgeMap>;
        auto edgeMapService = New<TEdgeMapService>(std::weak_ptr<TEdgeMap>(Edges_));
        edgeMapService->SetOpaque(false);

        using TLivePreviewListService = NYTree::TCollectionBoundListService<TLivePreviewList>;
        auto livePreviewService = New<TLivePreviewListService>(std::weak_ptr<TLivePreviewList>(LivePreviews_));

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
    explicit TImpl(TLogger logger)
        : Logger(std::move(logger))
    { }

    void Initialize()
    {
        using TVertexMapService = TCollectionBoundMapService<TVertexMap>;

        auto vertexMapService = New<TVertexMapService>(std::weak_ptr<TVertexMap>(Vertices_));
        vertexMapService->SetOpaque(false);
        auto service = New<TCompositeMapService>()
            ->AddChild("vertices", std::move(vertexMapService))
            ->AddChild("topological_ordering", IYPathService::FromProducer(BIND([this, weakThis = MakeWeak(this)] (IYsonConsumer* consumer) {
                if (auto this_ = weakThis.Lock()) {
                    BuildYsonFluently(consumer)
                        .List(GetTopologicalOrdering());
                }
            })));
        service->SetOpaque(false);
        Service_ = std::move(service);
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
        Persist(context, *Vertices_);
        Persist(context, TopologicalOrdering_);
        Persist(context, NodeDirectory_);

        if (context.GetVersion() >= ESnapshotVersion::ValidateLivePreviewChunks) {
            Persist(context, Logger);
        } else {
            Logger = ControllerLogger();
        }

        if (context.IsLoad()) {
            Initialize();
        }
    }

    void RegisterVertex(const TDataFlowGraph::TVertexDescriptor& vertex)
    {
        GetOrRegisterVertex(vertex);
    }

    void RegisterEdge(
        const TDataFlowGraph::TVertexDescriptor& from,
        const TDataFlowGraph::TVertexDescriptor& to)
    {
        TopologicalOrdering_.AddEdge(from, to);
        GetOrRegisterEdge(from, to);
    }

    void UpdateEdgeJobDataStatistics(
        const TDataFlowGraph::TVertexDescriptor& from,
        const TDataFlowGraph::TVertexDescriptor& to,
        const TDataStatistics& jobDataStatistics)
    {
        TopologicalOrdering_.AddEdge(from, to);
        const auto& edge = GetOrRegisterEdge(from, to);
        edge->JobDataStatistics() += jobDataStatistics;
    }

    void UpdateEdgeTeleportDataStatistics(
        const TDataFlowGraph::TVertexDescriptor& from,
        const TDataFlowGraph::TVertexDescriptor& to,
        const TDataStatistics& teleportDataStatistics)
    {
        TopologicalOrdering_.AddEdge(from, to);
        const auto& edge = GetOrRegisterEdge(from, to);
        edge->TeleportDataStatistics() += teleportDataStatistics;
    }

    void RegisterCounter(
        const TVertexDescriptor& descriptor,
        const TProgressCounterPtr& counter,
        EJobType jobType)
    {
        const auto& vertex = GetOrRegisterVertex(descriptor);
        vertex->SetJobType(jobType);
        counter->AddParent(vertex->JobCounter());
    }

    TError TryRegisterLivePreviewChunk(
        const TVertexDescriptor& descriptor,
        int index,
        TInputChunkPtr chunk)
    {
        const auto& vertex = GetOrRegisterVertex(descriptor);
        return vertex->TryRegisterLivePreviewChunk(index, std::move(chunk));
    }

    TError TryUnregisterLivePreviewChunk(
        const TVertexDescriptor& descriptor,
        int index,
        const TInputChunkPtr& chunk)
    {
        const auto& vertex = GetOrRegisterVertex(descriptor);
        return vertex->TryUnregisterLivePreviewChunk(index, chunk);
    }

    void BuildDataFlowYson(TFluentList fluent) const
    {
        std::vector<TEdgePtr> edges;
        for (const auto& [sourceDescriptor, vertex] : *Vertices_) {
            for (const auto& [targetDescriptor, edge] : *vertex->Edges()) {
                edges.push_back(edge);
            }
        }

        fluent
            .DoFor(edges, [&] (TFluentList fluent, const TEdgePtr& edge) {
                fluent.Item()
                    .BeginMap()
                        .Do(BIND(&TEdge::BuildDirectionYson, edge))
                    .EndMap();
            });
    }

    void BuildLegacyYson(TFluentMap fluent) const
    {
        auto topologicalOrdering = GetTopologicalOrdering();
        fluent
            .Item("vertices").BeginMap()
                .DoFor(topologicalOrdering, [&] (TFluentMap fluent, const TVertexDescriptor& vertexDescriptor) {
                    auto it = Vertices_->find(vertexDescriptor);
                    if (it != Vertices_->end()) {
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
                    auto it = Vertices_->find(from);
                    if (it != Vertices_->end()) {
                        fluent.Item(from)
                            .DoMapFor(*(it->second->Edges()), [&] (TFluentMap fluent, const auto& pair) {
                                auto to = pair.first;
                                const auto& edge = pair.second;
                                fluent
                                    .Item(to).BeginMap()
                                        .Item("statistics").Value(edge->JobDataStatistics() + edge->TeleportDataStatistics())
                                    .EndMap();
                            });
                    }
                })
            .Item("topological_ordering").List(topologicalOrdering);
    }

    void SetNodeDirectory(TNodeDirectoryPtr nodeDirectory)
    {
        NodeDirectory_ = std::move(nodeDirectory);
    }

private:
    using TVertexMap = THashMap<TVertexDescriptor, TVertexPtr>;
    const std::shared_ptr<TVertexMap> Vertices_ = std::make_shared<TVertexMap>();

    TProgressCounterPtr TotalJobCounter_ = New<TProgressCounter>();

    TIncrementalTopologicalOrdering<TVertexDescriptor> TopologicalOrdering_;

    TNodeDirectoryPtr NodeDirectory_;

    NYTree::IYPathServicePtr Service_;

    TSerializableLogger Logger;

    const TVertexPtr& GetOrRegisterVertex(const TVertexDescriptor& descriptor)
    {
        auto it = Vertices_->find(descriptor);
        if (it == Vertices_->end()) {
            auto& vertex = (*Vertices_)[descriptor];
            vertex = New<TVertex>(descriptor, NodeDirectory_, Logger);
            vertex->JobCounter()->AddParent(TotalJobCounter_);
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
    : Impl_(New<TImpl>(ControllerLogger()))
{ }

TDataFlowGraph::TDataFlowGraph(TLogger logger)
    : Impl_(New<TImpl>(std::move(logger)))
{ }

TDataFlowGraph::~TDataFlowGraph() = default;

void TDataFlowGraph::Initialize()
{
    Impl_->Initialize();
}

IYPathServicePtr TDataFlowGraph::GetService() const
{
    return Impl_->GetService();
}

void TDataFlowGraph::Persist(const TPersistenceContext& context)
{
    Impl_->Persist(context);
}

void TDataFlowGraph::RegisterVertex(const TVertexDescriptor& vertex)
{
    Impl_->RegisterVertex(vertex);
}

void TDataFlowGraph::RegisterEdge(
    const TVertexDescriptor& from,
    const TVertexDescriptor& to)
{
    Impl_->RegisterEdge(from, to);
}

void TDataFlowGraph::UpdateEdgeJobDataStatistics(
    const TVertexDescriptor& from,
    const TVertexDescriptor& to,
    const NChunkClient::NProto::TDataStatistics& jobDataStatistics)
{
    Impl_->UpdateEdgeJobDataStatistics(from, to, jobDataStatistics);
}

void TDataFlowGraph::UpdateEdgeTeleportDataStatistics(
    const TVertexDescriptor& from,
    const TVertexDescriptor& to,
    const NChunkClient::NProto::TDataStatistics& teleportDataStatistics)
{
    Impl_->UpdateEdgeTeleportDataStatistics(from, to, teleportDataStatistics);
}

void TDataFlowGraph::RegisterCounter(
    const TVertexDescriptor& vertex,
    const TProgressCounterPtr& counter,
    EJobType jobType)
{
    Impl_->RegisterCounter(vertex, counter, jobType);
}

TError TDataFlowGraph::TryRegisterLivePreviewChunk(
    const TVertexDescriptor& descriptor,
    int index,
    TInputChunkPtr chunk)
{
    return Impl_->TryRegisterLivePreviewChunk(descriptor, index, std::move(chunk));
}

TError TDataFlowGraph::TryUnregisterLivePreviewChunk(
    const TVertexDescriptor& descriptor,
    int index,
    const TInputChunkPtr& chunk)
{
    return Impl_->TryUnregisterLivePreviewChunk(descriptor, index, chunk);
}

void TDataFlowGraph::BuildDataFlowYson(TFluentList fluent) const
{
    Impl_->BuildDataFlowYson(fluent);
}

void TDataFlowGraph::BuildLegacyYson(TFluentMap fluent) const
{
    Impl_->BuildLegacyYson(fluent);
}

const std::vector<TVertexDescriptor>& TDataFlowGraph::GetTopologicalOrdering() const
{
    return Impl_->GetTopologicalOrdering();
}

void TDataFlowGraph::SetNodeDirectory(TNodeDirectoryPtr nodeDirectory)
{
    Impl_->SetNodeDirectory(std::move(nodeDirectory));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent::NControllers
