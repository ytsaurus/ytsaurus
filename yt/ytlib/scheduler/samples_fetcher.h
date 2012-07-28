#pragma once

#include "public.h"

#include <ytlib/misc/error.h>
#include <ytlib/logging/tagged_logger.h>
#include <ytlib/chunk_holder/chunk_holder_service_proxy.h>
#include <ytlib/table_client/table_chunk_meta.pb.h>
#include <ytlib/table_client/table_reader.pb.h>

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////////////////

//! Fetches samples for a bunch of table chunks by requesting
//! them directly from data nodes.
class TSamplesFetcher
    : public TRefCounted
{
public:
    TSamplesFetcher(
        TSchedulerConfigPtr config,
        TSortOperationSpecPtr spec,
        IInvokerPtr invoker,
        const TOperationId& operationId);

    void AddChunk(const NTableClient::NProto::TInputChunk& chunk);

    TFuture< TValueOrError<void> > Run(int desiredSampleCount);

    const std::vector<NTableClient::NProto::TKey>& GetSamples() const;

private:
    TSchedulerConfigPtr Config;
    TSortOperationSpecPtr Spec;
    IInvokerPtr Invoker;

    int WeightBetweenSamples;
    i64 TotalWeight;

    NLog::TTaggedLogger Logger;
    TPromise< TValueOrError<void> > Promise;

    //! All chunks for which samples are to be fetched.
    std::vector<NTableClient::NProto::TInputChunk> Chunks;
    
    //! Indexes of chunks for which no samples are fetched yet.
    yhash_set<int> UnfetchedChunkIndexes;

    //! All samples fetched so far.
    std::vector<NTableClient::NProto::TKey> Samples;

    //! Addresses of nodes that failed to reply.
    yhash_set<Stroka> DeadNodes;

    //! |(address, chunkId)| pairs for which an error was returned from the node.
    // XXX(babenko): need to specialize hash to use yhash_set
    std::set< TPair<Stroka, NChunkHolder::TChunkId> > DeadChunks;

    void SendRequests();
    void OnResponse(
        const Stroka& addresss,
        std::vector<int> chunkIndexes,
        NChunkHolder::TChunkHolderServiceProxy::TRspGetTableSamplesPtr rsp);
    void OnEndRound();

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
