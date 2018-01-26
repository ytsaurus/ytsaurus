#pragma once

#include "private.h"
#include "progress_counter.h"

#include <yt/server/chunk_pools/public.h>

#include <yt/ytlib/table_client/helpers.h>

#include <yt/ytlib/chunk_client/public.h>

#include <yt/core/misc/topological_ordering.h>

#include <yt/core/ytree/fluent.h>

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
    TNullable<NTransactionClient::TTimestamp> Timestamp;
    // Cell tag to allocate chunk lists.
    NObjectClient::TCellTag CellTag;
    bool ImmediatelyUnstageChunkLists = false;
    bool IsFinalOutput = false;

    void Persist(const TPersistenceContext& context);

    TEdgeDescriptor& operator =(const TEdgeDescriptor& other);
};

////////////////////////////////////////////////////////////////////////////////

class TDataFlowGraph
{
public:
    using TVertexDescriptor = TString;

    static TVertexDescriptor SourceDescriptor;
    static TVertexDescriptor SinkDescriptor;

    TDataFlowGraph() = default;

    void BuildYson(NYTree::TFluentMap fluent) const;

    void Persist(const TPersistenceContext& context);

    const std::vector<TVertexDescriptor>& GetTopologicalOrdering() const;

    void RegisterFlow(
        const TVertexDescriptor& from,
        const TVertexDescriptor& to,
        const NChunkClient::NProto::TDataStatistics& statistics);

    void RegisterTask(
        const TVertexDescriptor& vertex,
        const TProgressCounterPtr& taskCounter,
        EJobType jobType);

private:
    yhash<TVertexDescriptor, TProgressCounterPtr> JobCounters_;
    TProgressCounterPtr TotalJobCounter_ = New<TProgressCounter>(0);

    yhash<TVertexDescriptor, EJobType> JobTypes_;

    using TFlowMap = yhash<TVertexDescriptor, yhash<TVertexDescriptor, NChunkClient::NProto::TDataStatistics>>;
    TFlowMap Flow_;

    TIncrementalTopologicalOrdering<TVertexDescriptor> TopologicalOrdering_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NControllerAgent
} // namespace NYT
