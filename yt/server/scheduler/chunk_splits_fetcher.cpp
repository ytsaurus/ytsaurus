#include "stdafx.h"
#include "chunk_splits_fetcher.h"
#include "private.h"

#include <ytlib/table_client/key.h>

#include <ytlib/chunk_client/chunk_meta_extensions.h>

#include <ytlib/rpc/channel_cache.h>

#include <ytlib/misc/protobuf_helpers.h>

#include <ytlib/scheduler/config.h>

namespace NYT {
namespace NScheduler {

using namespace NChunkClient;
using namespace NTableClient;
using namespace NTableClient::NProto;
using namespace NChunkClient::NProto;

////////////////////////////////////////////////////////////////////

static NRpc::TChannelCache ChannelCache;

////////////////////////////////////////////////////////////////////

TChunkSplitsFetcher::TChunkSplitsFetcher(
    TSchedulerConfigPtr config,
    TMergeOperationSpecBasePtr spec,
    const TOperationId& operationId,
    const TKeyColumns& keyColumns)
    : Config(config)
    , Spec(spec)
    , KeyColumns(keyColumns)
    , Logger(OperationLogger)

{
    YCHECK(Spec->JobSliceDataSize > 0);
    Logger.AddTag(Sprintf("OperationId: %s", ~operationId.ToString()));
}

NLog::TTaggedLogger& TChunkSplitsFetcher::GetLogger()
{
    return Logger;
}

bool TChunkSplitsFetcher::Prepare(const std::vector<NTableClient::TRefCountedInputChunkPtr>& chunks)
{
    return true;
}

std::vector<TRefCountedInputChunkPtr>& TChunkSplitsFetcher::GetChunkSplits()
{
    return ChunkSplits;
}

void TChunkSplitsFetcher::CreateNewRequest(const Stroka& address)
{
    auto channel = ChannelCache.GetChannel(address);
    TChunkHolderServiceProxy proxy(channel);
    proxy.SetDefaultTimeout(Config->NodeRpcTimeout);

    CurrentRequest = proxy.GetChunkSplits();
    CurrentRequest->set_min_split_size(Spec->JobSliceDataSize);
    ToProto(CurrentRequest->mutable_key_columns(), KeyColumns);
}

bool TChunkSplitsFetcher::AddChunkToRequest(NTableClient::TRefCountedInputChunkPtr& chunk)
{
    if (chunk->uncompressed_data_size() < Spec->JobSliceDataSize) {
        LOG_DEBUG("Added chunk split (ChunkId: %s, TableIndex: %d)", 
            ~ToString(TChunkId::FromProto(chunk->slice().chunk_id())), chunk->TableIndex);

        ChunkSplits.push_back(chunk);
        return false;
    } else {
        *CurrentRequest->add_input_chunks() = *chunk;
        return true;
    }
}

auto TChunkSplitsFetcher::InvokeRequest() -> TFuture<TResponsePtr>
{
    auto req(MoveRV(CurrentRequest));
    return req->Invoke();
}

TError TChunkSplitsFetcher::ProcessResponseItem(
    const TResponsePtr& rsp, 
    int index,
    NTableClient::TRefCountedInputChunkPtr& chunk)
{
    YASSERT(rsp->IsOK());

    const auto& splittedChunks = rsp->splitted_chunks(index);
    if (splittedChunks.has_error()) {
        auto error = FromProto(splittedChunks.error());
        return error;
    } else {
        LOG_TRACE("Received %d chunk splits for chunk number %d",
            splittedChunks.input_chunks_size(),
            index);
        FOREACH (const auto& inputChunk, splittedChunks.input_chunks()) {
            ChunkSplits.push_back(New<TRefCountedInputChunk>(inputChunk, chunk->TableIndex));
        }
        return TError();
    }
}

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
