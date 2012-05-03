#pragma once

#include "public.h"

#include <ytlib/misc/error.h>
#include <ytlib/rpc/channel.h>
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
        IInvoker::TPtr invoker,
        const TOperationId& operationId);

    void AddChunk(const NTableClient::NProto::TInputChunk& chunk);

    TFuture< TValueOrError<void> > Run();

    const std::vector<NTableClient::NProto::TKeySample>& GetSamples() const;

private:
    TSchedulerConfigPtr Config;
    TSortOperationSpecPtr Spec;
    IInvoker::TPtr Invoker;

    NLog::TTaggedLogger Logger;
    TPromise< TValueOrError<void> > Promise;

    //! All chunks for which samples are to be fetched.
    std::vector<NTableClient::NProto::TInputChunk> Chunks;
    
    //! Chunks for which no samples are fetched yet.
    yhash_set<NTableClient::NProto::TInputChunk*> UnfetchedChunks;

    //! All samples fetched so far.
    std::vector<NTableClient::NProto::TKeySample> Samples;

    //! Addresses of nodes that failed to reply.
    yhash_set<Stroka> DeadNodes;

    //! |(address, chunkId)| pairs for which an error was returned from the node.
    // XXX(babenko): need to specialize hash to use yhash_set
    std::set< TPair<Stroka, NChunkHolder::TChunkId> > DeadChunks;

    void SendRequests();
    void OnResponse(
        const Stroka& addresss,
        std::vector<NTableClient::NProto::TInputChunk*> chunks,
        NChunkHolder::TChunkHolderServiceProxy::TRspGetTableSamples::TPtr rsp);
    void OnEndRound();

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
