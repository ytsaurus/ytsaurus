#pragma once

#include "public.h"

#include <core/misc/error.h>

#include <core/logging/tagged_logger.h>

#include <ytlib/chunk_client/data_node_service_proxy.h>
#include <ytlib/chunk_client/chunk_spec.h>

#include <ytlib/node_tracker_client/public.h>

#include <ytlib/table_client/public.h>
#include <ytlib/table_client/table_chunk_meta.pb.h>

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////////////////

//! Fetches samples for a bunch of table chunks by requesting
//! them directly from data nodes.
class TChunkSplitsFetcher
    : public TRefCounted
{
public:
    typedef NChunkClient::TDataNodeServiceProxy::TRspGetChunkSplitsPtr TResponsePtr;

    TChunkSplitsFetcher(
        TSchedulerConfigPtr config,
        TMergeOperationSpecBasePtr spec,
        const TOperationId& operationId,
        const NTableClient::TKeyColumns& keyColumns);

    void Prepare(const std::vector<NChunkClient::TRefCountedChunkSpecPtr>& chunks);

    void CreateNewRequest(const NNodeTrackerClient::TNodeDescriptor& descriptor);

    //! If |false| is returned then this chunk has not been added to the request since
    //! the chunk is already too small for splitting.
    bool AddChunkToRequest(
        NNodeTrackerClient::TNodeId nodeId,
        NChunkClient::TRefCountedChunkSpecPtr chunkSpec);

    TFuture<TResponsePtr> InvokeRequest();

    TError ProcessResponseItem(
        TResponsePtr rsp,
        int index,
        NChunkClient::TRefCountedChunkSpecPtr chunkSpec);

    const std::vector<NChunkClient::TRefCountedChunkSpecPtr>& GetChunkSplits();

    NLog::TTaggedLogger& GetLogger();

private:
    TSchedulerConfigPtr Config;
    TMergeOperationSpecBasePtr Spec;

    NTableClient::TKeyColumns KeyColumns;
    int PartitionTag;

    NLog::TTaggedLogger Logger;

    //! All samples fetched so far.
    std::vector<NChunkClient::TRefCountedChunkSpecPtr> ChunkSplits;

    NChunkClient::TDataNodeServiceProxy::TReqGetChunkSplitsPtr CurrentRequest;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
