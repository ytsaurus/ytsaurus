#pragma once

#include "public.h"

#include <ytlib/misc/error.h>

#include <ytlib/logging/tagged_logger.h>

#include <ytlib/chunk_client/chunk_holder_service_proxy.h>

#include <ytlib/table_client/public.h>
#include <ytlib/table_client/table_chunk_meta.pb.h>
#include <ytlib/table_client/table_reader.pb.h>

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////////////////

//! Fetches samples for a bunch of table chunks by requesting
//! them directly from data nodes.
class TChunkSplitsFetcher
    : public TRefCounted
{
public:
    typedef NChunkClient::TChunkHolderServiceProxy::TRspGetChunkSplitsPtr TResponsePtr;

    TChunkSplitsFetcher(
        TSchedulerConfigPtr config,
        TMergeOperationSpecBasePtr spec,
        const TOperationId& operationId,
        const NTableClient::TKeyColumns& keyColumns);

    // If returns false, no further collecting is required.
    bool Prepare(const std::vector<NTableClient::TRefCountedInputChunkPtr>& chunks);

    void CreateNewRequest(const Stroka& address);

    // Returns false if samples from this chunk are not required.
    bool AddChunkToRequest(NTableClient::TRefCountedInputChunkPtr inputChunk);
    TFuture<TResponsePtr> InvokeRequest();

    TError ProcessResponseItem(
        TResponsePtr rsp, 
        int index,
        NTableClient::TRefCountedInputChunkPtr inputChunk);

    std::vector<NTableClient::TRefCountedInputChunkPtr>& GetChunkSplits();

    NLog::TTaggedLogger& GetLogger();

private:
    TSchedulerConfigPtr Config;
    TMergeOperationSpecBasePtr Spec;

    NTableClient::TKeyColumns KeyColumns;

    // Number of splits shouldn't exceed MaxChunkCount.
    // If initial number of chunks is greater or equal to MaxChunkCount,
    // collecting is not performed.
    i64 MinSplitSize;

    NLog::TTaggedLogger Logger;

    //! All samples fetched so far.
    std::vector<NTableClient::TRefCountedInputChunkPtr> ChunkSplits;

    NChunkClient::TChunkHolderServiceProxy::TReqGetChunkSplitsPtr CurrentRequest;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
