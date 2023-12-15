#pragma once

#include "private.h"

#include <yt/yt/server/lib/chunk_pools/public.h>

#include <yt/yt/ytlib/table_client/public.h>
#include <yt/yt/ytlib/table_client/table_upload_options.h>

#include <yt/yt/ytlib/chunk_client/public.h>

#include <yt/yt/ytlib/node_tracker_client/public.h>

#include <yt/yt/core/misc/topological_ordering.h>

#include <yt/yt/core/ytree/fluent.h>
#include <yt/yt/core/ytree/virtual.h>

namespace NYT::NControllerAgent::NControllers {

////////////////////////////////////////////////////////////////////////////////

class TDataFlowGraph
    : public TRefCounted
{
public:
    using TVertexDescriptor = TString;

    static const TVertexDescriptor SourceDescriptor;
    static const TVertexDescriptor SinkDescriptor;
    static const TVertexDescriptor CoreDescriptor;
    static const TVertexDescriptor StderrDescriptor;

    TDataFlowGraph();
    ~TDataFlowGraph();

    void Initialize();

    NYTree::IYPathServicePtr GetService() const;

    void Persist(const TPersistenceContext& context);

    void RegisterVertex(const TVertexDescriptor& vertex);

    void RegisterEdge(
        const TVertexDescriptor& from,
        const TVertexDescriptor& to);

    void UpdateEdgeJobDataStatistics(
        const TVertexDescriptor& from,
        const TVertexDescriptor& to,
        const NChunkClient::NProto::TDataStatistics& jobDataStatistics);

    void UpdateEdgeTeleportDataStatistics(
        const TVertexDescriptor& from,
        const TVertexDescriptor& to,
        const NChunkClient::NProto::TDataStatistics& teleportDataStatistics);

    void RegisterCounter(
        const TVertexDescriptor& vertex,
        const TProgressCounterPtr& counter,
        EJobType jobType);

    void RegisterLivePreviewChunk(const TVertexDescriptor& descriptor, int index, NChunkClient::TInputChunkPtr chunk);
    void UnregisterLivePreviewChunk(const TVertexDescriptor& descriptor, int index, NChunkClient::TInputChunkPtr chunk);

    void BuildDataFlowYson(NYTree::TFluentList fluent) const;

    void BuildLegacyYson(NYTree::TFluentMap fluent) const;

    const std::vector<TVertexDescriptor>& GetTopologicalOrdering() const;

    void SetNodeDirectory(NNodeTrackerClient::TNodeDirectoryPtr nodeDirectory);

private:
    class TImpl;
    const TIntrusivePtr<TImpl> Impl_;
};

DEFINE_REFCOUNTED_TYPE(TDataFlowGraph)

////////////////////////////////////////////////////////////////////////////////

// NB: store all the fields for TStreamDescriptor in a non-refcounted
// TStreamDescriptor base to copy all the fields in Clone() function at once.
struct TStreamDescriptorBase
{
    std::vector<NTableClient::TTableSchemaPtr> StreamSchemas;

    void Persist(const TPersistenceContext& context);
};

////////////////////////////////////////////////////////////////////////////////

struct TInputStreamDescriptor
    : public TRefCounted
    , public TStreamDescriptorBase
{
    static TInputStreamDescriptorPtr FromOutputStreamDescriptor(
        TOutputStreamDescriptorPtr outputStreamDescriptor);

    TInputStreamDescriptor(TStreamDescriptorBase base);
    TInputStreamDescriptor() = default;

    TInputStreamDescriptorPtr Clone() const;

    void Persist(const TPersistenceContext& context);
};

DEFINE_REFCOUNTED_TYPE(TInputStreamDescriptor)

////////////////////////////////////////////////////////////////////////////////

struct TOutputStreamDescriptorBase
    : public TStreamDescriptorBase
{
    TOutputStreamDescriptorBase() = default;

    // Keep fields below in sync with operator =.
    NChunkPools::IPersistentChunkPoolInputPtr DestinationPool;
    // May be null if recovery info is not required.
    NChunkPools::TInputChunkMappingPtr ChunkMapping;
    bool RequiresRecoveryInfo = false;
    NTableClient::TTableWriterOptionsPtr TableWriterOptions;
    TString SlowMedium;
    NTableClient::TTableUploadOptions TableUploadOptions;
    NYson::TYsonString TableWriterConfig;
    std::optional<NTransactionClient::TTimestamp> Timestamp;
    // Cell tags to allocate chunk lists. For each job cell tag is chosen
    // randomly.
    NObjectClient::TCellTagList CellTags;
    bool ImmediatelyUnstageChunkLists = false;
    bool IsFinalOutput = false;
    bool IsOutputTableDynamic = false;

    // In most situations coincides with the index of an stream descriptor,
    // but in some situations may differ. For example, an auto merge task
    // may have the only output descriptor, but we would like to attach
    // its output chunks to the live preview with an index corresponding to the
    // output table index.
    int LivePreviewIndex = 0;
    TDataFlowGraph::TVertexDescriptor TargetDescriptor;

    std::optional<int> PartitionTag;

};

struct TOutputStreamDescriptor
    : public TRefCounted
    , public TOutputStreamDescriptorBase
{
    TOutputStreamDescriptor(TOutputStreamDescriptorBase base);
    TOutputStreamDescriptor() = default;

    TOutputStreamDescriptorPtr Clone() const;

    void Persist(const TPersistenceContext& context);
};

DEFINE_REFCOUNTED_TYPE(TOutputStreamDescriptor)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent::NControllers
