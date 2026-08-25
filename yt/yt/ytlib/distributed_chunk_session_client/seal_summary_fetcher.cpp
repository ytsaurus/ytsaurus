#include "seal_summary_fetcher.h"

#include <yt/yt/ytlib/chunk_client/throttler_manager.h>

#include <yt/yt/ytlib/object_client/object_service_proxy.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/core/concurrency/throughput_throttler.h>

#include <yt/yt/core/misc/protobuf_helpers.h>

#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/ypath_proxy.h>

#include <array>
#include <string_view>

namespace NYT::NDistributedChunkSessionClient {

using namespace NApi;
using namespace NChunkClient;
using namespace NConcurrency;
using namespace NObjectClient;
using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////

static constexpr auto SealSummaryAttributeKeys = std::to_array<std::string_view>({
    "sealed",
    "row_count",
    "compressed_data_size",
});

std::vector<TDistributedChunkSessionSealSummary> ParseSealSummaryResponse(
    THashSet<TChunkId> requestedChunkIds,
    const NLogging::TLogger& Logger,
    const TObjectServiceProxy::TRspExecuteBatchPtr& batchResponse)
{
    auto requestedChunkCount = requestedChunkIds.size();
    std::vector<TDistributedChunkSessionSealSummary> result;
    result.reserve(requestedChunkIds.size());

    for (const auto& [tag, responseOrError] : batchResponse->GetTaggedResponses<TYPathProxy::TRspGet>("get")) {
        auto chunkId = std::any_cast<TChunkId>(tag);
        THROW_ERROR_EXCEPTION_IF(
            requestedChunkIds.erase(chunkId) != 1,
            "Master returned an unexpected or duplicate distributed chunk session seal summary")
            .With("chunk_id", chunkId);

        if (responseOrError.FindMatching(NYTree::EErrorCode::ResolveError)) {
            YT_TLOG_DEBUG("Distributed chunk session chunk is missing")
                .With("ChunkId", chunkId)
                .With(static_cast<const TError&>(responseOrError));
            continue;
        }

        const auto& response = responseOrError.ValueOrThrow();
        auto attributes = ConvertToAttributes(TYsonString(response->value()));
        auto sealed = attributes->Find<bool>("sealed");
        THROW_ERROR_EXCEPTION_IF(
            !sealed,
            "Master returned a distributed chunk session seal summary without seal flag")
            .With("chunk_id", chunkId);

        if (*sealed) {
            auto recordCount = attributes->Find<i64>("row_count");
            auto compressedDataSize = attributes->Find<i64>("compressed_data_size");
            THROW_ERROR_EXCEPTION_IF(
                !recordCount || !compressedDataSize,
                "Master returned an incomplete seal summary for distributed-session chunk %v "
                "(RowCount: %v, CompressedDataSize: %v)",
                chunkId,
                recordCount,
                compressedDataSize);

            result.push_back(TDistributedChunkSessionSealSummary{
                .ChunkId = chunkId,
                .RecordCount = *recordCount,
                .CompressedDataSize = *compressedDataSize,
            });
        }
    }

    THROW_ERROR_EXCEPTION_IF(
        !requestedChunkIds.empty(),
        "Master did not return seal-summary responses for some distributed chunk session chunks")
        .With("missing_chunk_count", requestedChunkIds.size());

    YT_TLOG_DEBUG("Distributed chunk session seal summaries fetched from master")
        .With("RequestedChunkCount", requestedChunkCount)
        .With("SealedChunkCount", result.size());

    return result;
}

TFuture<std::vector<TDistributedChunkSessionSealSummary>> DoFetchDistributedChunkSessionSealSummaries(
    NNative::IClientPtr client,
    IInvokerPtr invoker,
    NLogging::TLogger Logger,
    THashSet<TChunkId> chunkIds)
{
    YT_VERIFY(!chunkIds.empty());

    auto cellTag = CellTagFromId(*chunkIds.begin());
    for (auto chunkId : chunkIds) {
        YT_VERIFY(CellTagFromId(chunkId) == cellTag);
    }

    YT_TLOG_DEBUG("Fetching distributed chunk session seal summaries from master")
        .With("ChunkCount", chunkIds.size());

    auto proxy = CreateObjectServiceReadProxy(
        client,
        EMasterChannelKind::Follower,
        cellTag);
    auto batchRequest = proxy.ExecuteBatchNoBackoffRetries();

    for (auto chunkId : chunkIds) {
        auto request = TYPathProxy::Get(FromObjectId(chunkId) + "/@");
        ToProto(request->mutable_attributes()->mutable_keys(), SealSummaryAttributeKeys);
        request->Tag() = chunkId;
        batchRequest->AddRequest(request, "get");
    }

    return batchRequest->Invoke()
        .Apply(BIND(
            ParseSealSummaryResponse,
            Passed(std::move(chunkIds)),
            Logger)
            .AsyncVia(invoker));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace

////////////////////////////////////////////////////////////////////////////////

TFuture<std::vector<TDistributedChunkSessionSealSummary>> FetchDistributedChunkSessionSealSummaries(
    NNative::IClientPtr client,
    IInvokerPtr invoker,
    TThrottlerManagerPtr throttlerManager,
    std::vector<TChunkId> chunkIds,
    NLogging::TLogger logger)
{
    YT_VERIFY(client);
    YT_VERIFY(invoker);
    YT_VERIFY(throttlerManager);

    if (chunkIds.empty()) {
        return MakeFuture<std::vector<TDistributedChunkSessionSealSummary>>({});
    }

    auto cellTag = CellTagFromId(chunkIds.front());
    THashSet<TChunkId> uniqueChunkIds;
    uniqueChunkIds.reserve(chunkIds.size());
    for (auto chunkId : chunkIds) {
        YT_VERIFY(CellTagFromId(chunkId) == cellTag);
        YT_VERIFY(uniqueChunkIds.insert(chunkId).second);
    }

    auto throttler = throttlerManager->GetThrottler(cellTag);
    return throttler->Throttle(uniqueChunkIds.size())
        .Apply(BIND(
            DoFetchDistributedChunkSessionSealSummaries,
            client,
            invoker,
            logger,
            Passed(std::move(uniqueChunkIds)))
            .AsyncVia(invoker));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDistributedChunkSessionClient
