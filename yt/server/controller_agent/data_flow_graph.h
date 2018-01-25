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
    DEFINE_BYREF_RO_PROPERTY(TProgressCounterPtr, TotalJobCounter, New<TProgressCounter>(0));

    // NB: It is correct until the moment we have two tasks in the same controller
    // that share the job type. Such situation didn't happen yet though :)
    using TVertexDescriptor = EJobType;

public:
    TDataFlowGraph() = default;

    void BuildYson(NYTree::TFluentMap fluent) const;

    const TProgressCounterPtr& JobCounter(TVertexDescriptor vertexDescriptor);

    void Persist(const TPersistenceContext& context);

    std::vector<TVertexDescriptor> GetTopologicalOrdering() const;

    void RegisterFlow(TVertexDescriptor from, TVertexDescriptor to, const NChunkClient::NProto::TDataStatistics& statistics);

private:
    THashMap<TVertexDescriptor, TProgressCounterPtr> JobCounters_;

    using TFlowMap = THashMap<TVertexDescriptor, THashMap<TVertexDescriptor, NChunkClient::NProto::TDataStatistics>>;
    TFlowMap Flow_;

    TIncrementalTopologicalOrdering TopologicalOrdering_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NControllerAgent
} // namespace NYT
