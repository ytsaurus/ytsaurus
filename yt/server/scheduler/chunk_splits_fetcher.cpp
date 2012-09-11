#include "stdafx.h"
#include "chunk_splits_fetcher.h"
#include "private.h"

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
    TSortOperationSpecPtr spec,
    const TOperationId& operationId,
    const TKeyColumns& keyColumns,
    int maxChunkCount,
    i64 minSplitSize)
    : Config(config)
    , Spec(spec)
    , KeyColumns(keyColumns)
    , MaxChunkCount(maxChunkCount)
    , MinSplitSize(minSplitSize)
    , Logger(OperationLogger)

{
    YCHECK(MinSplitSize > 0);
    YCHECK(MaxChunkCount > 0);
    Logger.AddTag(Sprintf("OperationId: %s", ~operationId.ToString()));
}

NLog::TTaggedLogger& TChunkSplitsFetcher::GetLogger()
{
    return Logger;
}

bool TChunkSplitsFetcher::Prepare(const std::vector<NTableClient::NProto::TInputChunk>& chunks)
{
    if (chunks.size() > MaxChunkCount) {
        // No need to fetch anything from holders.
        ChunkSplits.assign(chunks.begin(), chunks.end());
        return false;
    }

    i64 totalSize = 0;
    FOREACH(const auto& chunk, chunks) {
        auto miscExt = GetProtoExtension<TMiscExt>(chunk.extensions());
        YASSERT(miscExt.sorted() == true);
        totalSize += miscExt.uncompressed_data_size();
    }
    YCHECK(totalSize > 0);

    MinSplitSize = std::max(totalSize / MaxChunkCount, MinSplitSize);
    return true;
}

const std::vector<TInputChunk>& TChunkSplitsFetcher::GetChunkSplits() const
{
    return ChunkSplits;
}

void TChunkSplitsFetcher::CreateNewRequest(const Stroka& address)
{
    YASSERT(!CurrentRequest);

    auto channel = ChannelCache.GetChannel(address);
    TChunkHolderServiceProxy proxy(channel);
    proxy.SetDefaultTimeout(Config->NodeRpcTimeout);

    CurrentRequest = proxy.GetChunkSplits();
    CurrentRequest->set_min_split_size(MinSplitSize);
    ToProto(CurrentRequest->mutable_key_columns(), KeyColumns);
}

bool TChunkSplitsFetcher::AddChunkToRequest(const NTableClient::NProto::TInputChunk& chunk)
{
    *CurrentRequest->add_input_chunks() = chunk;
    return true;
}

auto TChunkSplitsFetcher::InvokeRequest() -> TFuture<TResponsePtr>
{
    auto req(MoveRV(CurrentRequest));
    return req->Invoke();
}

TError TChunkSplitsFetcher::ProcessResponseItem(const TResponsePtr& rsp, int index)
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
            ChunkSplits.push_back(inputChunk);
        }
        return TError();
    }
}

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

