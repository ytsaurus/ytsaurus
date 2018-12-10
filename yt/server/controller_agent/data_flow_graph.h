#pragma once

#include "private.h"
#include "progress_counter.h"

#include <yt/server/chunk_pools/public.h>

#include <yt/server/table_server/public.h>

#include <yt/ytlib/table_client/helpers.h>

#include <yt/ytlib/chunk_client/public.h>

#include <yt/ytlib/node_tracker_client/public.h>

#include <yt/core/misc/topological_ordering.h>

#include <yt/core/ytree/fluent.h>
#include <yt/core/ytree/virtual.h>

namespace NYT {
namespace NControllerAgent {

////////////////////////////////////////////////////////////////////////////////

struct TEdgeDescriptor
{
    TEdgeDescriptor() = default;

    // Keep fields below in sync with operator =.
    NChunkPools::IChunkPoolInput* DestinationPool = nullptr;
    // May be null if recovery info is not required.
    TInputChunkMappingPtr ChunkMapping;
    bool RequiresRecoveryInfo = false;
    NTableClient::TTableWriterOptionsPtr TableWriterOptions;
    NTableClient::TTableUploadOptions TableUploadOptions;
    NYson::TYsonString TableWriterConfig;
    std::optional<NTransactionClient::TTimestamp> Timestamp;
    // Cell tag to allocate chunk lists.
    NObjectClient::TCellTag CellTag;
    bool ImmediatelyUnstageChunkLists = false;
    bool IsFinalOutput = false;
    // In most situations coincides with the index of an edge descriptor,
    // but in some situations may differ. For example, an auto merge task
    // may have the only output descriptor, but we would like to attach
    // its output chunks to the live preview with an index corresponding to the
    // output table index.
    int LivePreviewIndex = 0;

    void Persist(const TPersistenceContext& context);

    TEdgeDescriptor& operator =(const TEdgeDescriptor& other);
};

////////////////////////////////////////////////////////////////////////////////

class TDataFlowGraph
    : public TRefCounted
{
public:
    using TVertexDescriptor = TString;

    static TVertexDescriptor SourceDescriptor;
    static TVertexDescriptor SinkDescriptor;

    TDataFlowGraph();
    TDataFlowGraph(NNodeTrackerClient::TNodeDirectoryPtr nodeDirectory);
    ~TDataFlowGraph();

    NYTree::IYPathServicePtr GetService() const;

    void Persist(const TPersistenceContext& context);

    void UpdateEdgeStatistics(
        const TVertexDescriptor& from,
        const TVertexDescriptor& to,
        const NChunkClient::NProto::TDataStatistics& statistics);

    void RegisterCounter(
        const TVertexDescriptor& vertex,
        const TProgressCounterPtr& counter,
        EJobType jobType);

    void RegisterLivePreviewChunk(const TVertexDescriptor& descriptor, int index, NChunkClient::TInputChunkPtr chunk);
    void UnregisterLivePreviewChunk(const TVertexDescriptor& descriptor, int index, NChunkClient::TInputChunkPtr chunk);

    void BuildLegacyYson(NYTree::TFluentMap fluent) const;

private:
    class TImpl;
    const TIntrusivePtr<TImpl> Impl_;
};

DEFINE_REFCOUNTED_TYPE(TDataFlowGraph);

////////////////////////////////////////////////////////////////////////////////

} // namespace NControllerAgent
} // namespace NYT
