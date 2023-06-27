#include "pivot_keys_picker.h"
#include "pivot_keys_builder.h"

#include <yt/yt/ytlib/api/native/client_impl.h>
#include <yt/yt/ytlib/api/native/config.h>

#include <yt/yt/ytlib/chunk_client/chunk_meta_extensions.h>
#include <yt/yt/ytlib/chunk_client/chunk_spec_fetcher.h>
#include <yt/yt/ytlib/chunk_client/helpers.h>
#include <yt/yt/ytlib/chunk_client/input_chunk_slice.h>
#include <yt/yt/ytlib/chunk_client/input_chunk.h>
#include <yt/yt/ytlib/chunk_client/legacy_data_slice.h>

#include <yt/yt/ytlib/cypress_client/rpc_helpers.h>

#include <yt/yt/ytlib/table_client/chunk_meta_extensions.h>
#include <yt/yt/ytlib/table_client/chunk_slice_fetcher.h>
#include <yt/yt/ytlib/table_client/chunk_slice_size_fetcher.h>
#include <yt/yt/ytlib/table_client/helpers.h>

#include <yt/yt/client/table_client/row_buffer.h>

#include <yt/yt/client/tablet_client/table_mount_cache.h>

#include <yt/yt/core/concurrency/action_queue.h>

namespace NYT::NTabletClient {

using namespace NApi;
using namespace NChunkClient;
using namespace NConcurrency;
using namespace NLogging;
using namespace NObjectClient;
using namespace NTableClient;
using namespace NYPath;
using namespace NYson;
using namespace NYTree;

using NYT::FromProto;
using NYT::ToProto;

constexpr double DefaultSlicingAccuracy = 0.05;
constexpr i64 ExpectedAverageOverlapping = 10;

////////////////////////////////////////////////////////////////////////////////

std::vector<TLegacyOwningKey> PickPivotKeysWithSlicing(
    const NNative::IClientPtr& client,
    const TYPath& path,
    int tabletCount,
    const TReshardTableOptions& options,
    const TLogger& Logger,
    bool enableVerboseLogging)
{
    const auto& connection = client->GetNativeConnection();
    const auto& tableMountCache = connection->GetTableMountCache();
    auto tableInfo = WaitFor(tableMountCache->GetTableInfo(path))
        .ValueOrThrow();

    tableInfo->ValidateDynamic();
    tableInfo->ValidateSorted();

    TTableId tableId;
    TCellTag externalCellTag;
    auto tableAttributes = ResolveExternalTable(
        client,
        path,
        &tableId,
        &externalCellTag);

    TReadRange range;
    if (options.FirstTabletIndex) {
        auto tabletInfo = tableInfo->GetTabletByIndexOrThrow(*options.FirstTabletIndex);
        auto keyBound = tabletInfo->GetLowerKeyBound();
        range.LowerLimit() = TReadLimit(keyBound);
    }

    auto nextPivot = MaxKey();
    if (options.LastTabletIndex && options.LastTabletIndex < std::ssize(tableInfo->Tablets) - 1) {
        auto tabletInfo = tableInfo->GetTabletByIndexOrThrow(*options.LastTabletIndex + 1);
        auto keyBound = tabletInfo->GetLowerKeyBound();
        range.UpperLimit() = TReadLimit(keyBound).Invert();
        nextPivot = tabletInfo->PivotKey;
    }

    auto prepareFetchRequest = [&] (const TChunkOwnerYPathProxy::TReqFetchPtr& request, int /*tableIndex*/) {
        request->set_fetch_all_meta_extensions(false);
        request->add_extension_tags(TProtoExtensionTag<NChunkClient::NProto::TMiscExt>::Value);
        request->add_extension_tags(TProtoExtensionTag<NTableClient::NProto::TBoundaryKeysExt>::Value);
        AddCellTagToSyncWith(request, tableId);
        NCypressClient::SetTransactionId(request, NullTransactionId);
    };

    auto chunkSpecFetcher = New<TMasterChunkSpecFetcher>(
        client,
        TMasterReadOptions{},
        connection->GetNodeDirectory(),
        connection->GetInvoker(),
        connection->GetConfig()->MaxChunksPerFetch,
        connection->GetConfig()->MaxChunksPerLocateRequest,
        prepareFetchRequest,
        Logger);

    chunkSpecFetcher->Add(
        tableId,
        externalCellTag,
        /*chunkCount*/ -1,
        /*tableIndex*/ 0,
        {range});

    YT_LOG_DEBUG("Fetching chunk specs");

    WaitFor(chunkSpecFetcher->Fetch())
        .ThrowOnError();

    YT_LOG_DEBUG("Chunk specs fetched");

    if (chunkSpecFetcher->ChunkSpecs().empty() && tabletCount > 1) {
        THROW_ERROR_EXCEPTION("Empty table %v cannot be resharded to more than one tablet",
            path)
            << TErrorAttribute("tablet_count", tabletCount);
    }

    i64 chunksDataWeight = 0;
    THashSet<TChunkId> chunkIds;
    for (const auto& chunkSpec : chunkSpecFetcher->ChunkSpecs()) {
        auto chunkId = FromProto<TChunkId>(chunkSpec.chunk_id());
        if (!chunkIds.contains(chunkId)) {
            chunksDataWeight += GetChunkDataWeight(chunkSpec);
            chunkIds.insert(chunkId);
        }
    }

    auto accuracy = options.SlicingAccuracy.value_or(DefaultSlicingAccuracy);

    auto expectedTabletSize = DivCeil<i64>(chunksDataWeight, tabletCount);
    i64 minSliceSize = std::max(expectedTabletSize * accuracy / ExpectedAverageOverlapping, 1.);

    YT_LOG_DEBUG("Initializing pivot keys builder for resharding with slicing"
        " (ChunksDataWeight: %v, ExpectedTabletSize: %v, MinSliceSize: %v, Accuracy: %v, EnableVerboseLogging: %v)",
        chunksDataWeight,
        expectedTabletSize,
        minSliceSize,
        accuracy,
        enableVerboseLogging);

    const auto& comparator = tableInfo->Schemas[ETableSchemaKind::Primary]->ToComparator();
    auto keyColumnCount = tableInfo->Schemas[ETableSchemaKind::Primary]->GetKeyColumnCount();
    TReshardPivotKeysBuilder reshardBuilder(
        comparator,
        keyColumnCount,
        tabletCount,
        accuracy,
        expectedTabletSize,
        nextPivot,
        enableVerboseLogging);

    chunkIds.clear();
    i64 unlimitedChunksDataWeight = 0;
    i64 maxBlockSize = 0;
    std::vector<TInputChunkPtr> splitChunks;

    for (const auto& chunkSpec : chunkSpecFetcher->ChunkSpecs()) {
        auto inputChunk = New<TInputChunk>(chunkSpec);
        auto chunkDataWeight = GetChunkDataWeight(chunkSpec);
        maxBlockSize = std::max(maxBlockSize, inputChunk->GetMaxBlockSize());

        auto chunkId = FromProto<TChunkId>(chunkSpec.chunk_id());
        if (!IsBlobChunkId(chunkId)) {
            continue;
        }

        if ((chunkSpec.has_lower_limit() || chunkSpec.has_upper_limit()) &&
            chunkDataWeight > minSliceSize) {
            splitChunks.push_back(inputChunk);
        } else {
            reshardBuilder.AddChunk(chunkSpec);
            if (!chunkIds.contains(chunkId)) {
                unlimitedChunksDataWeight += chunkDataWeight;
                chunkIds.insert(chunkId);
            }
        }
    }

    if (!splitChunks.empty()) {
        auto config = New<TFetcherConfig>();
        auto sizeFetcher = New<TChunkSliceSizeFetcher>(
            config,
            connection->GetNodeDirectory(),
            connection->GetInvoker(),
            /*chunkScraper*/ nullptr,
            client,
            Logger);

        for (const auto& inputChunk : splitChunks) {
            sizeFetcher->AddChunk(inputChunk);
        }

        YT_LOG_DEBUG("Fetching chunk slice sizes");

        WaitFor(sizeFetcher->Fetch())
            .ThrowOnError();

        YT_LOG_DEBUG("Chunk slice sizes fetched");

        i64 limitedChunksDataWeight = 0;
        for (const auto& chunk : sizeFetcher->WeightedChunks()) {
            limitedChunksDataWeight += chunk->GetDataWeight();
            reshardBuilder.AddChunk(chunk);
        }

        auto expectedTabletSize = DivCeil<i64>(limitedChunksDataWeight + unlimitedChunksDataWeight, tabletCount);
        minSliceSize = std::max(expectedTabletSize * accuracy / ExpectedAverageOverlapping, 1.);
        YT_LOG_DEBUG("Replacing resharding with slicing expected tablet size"
            " (LimitedChunksDataWeight: %v, UnlimitedChunksDataWeight: %v, ExpectedTabletSize: %v, MinSliceSize: %v)",
            limitedChunksDataWeight,
            unlimitedChunksDataWeight,
            expectedTabletSize,
            minSliceSize);

        reshardBuilder.SetExpectedTabletSize(expectedTabletSize);
    }

    YT_LOG_DEBUG("Computing chunks for slicing");

    reshardBuilder.ComputeChunksForSlicing();

    auto firstTabletIndex = options.FirstTabletIndex.value_or(0);
    reshardBuilder.SetFirstPivotKey(tableInfo->GetTabletByIndexOrThrow(firstTabletIndex)->PivotKey);

    if (reshardBuilder.AreAllPivotsFound()) {
        YT_LOG_DEBUG("Picked pivot keys without slicing");
        return reshardBuilder.GetPivotKeys();
    }

    if (reshardBuilder.GetChunksForSlicing().empty()) {
        THROW_ERROR_EXCEPTION("Could not reshard table %v to desired tablet count; consider reducing tablet count or specifying pivot keys manually",
            path)
            << TErrorAttribute("tablet_count", tabletCount)
            << TErrorAttribute("max_block_size", maxBlockSize)
            << TErrorAttribute("split_chunk_count", std::ssize(splitChunks));
    }

    auto rowBuffer = New<TRowBuffer>();
    auto chunkSliceFetcher = CreateChunkSliceFetcher(
        connection->GetConfig()->ChunkSliceFetcher,
        connection->GetNodeDirectory(),
        CreateSerializedInvoker(connection->GetInvoker()),
        /*chunkScraper*/ nullptr,
        client,
        rowBuffer,
        Logger);

    for (const auto& [inputChunk, size] : reshardBuilder.GetChunksForSlicing()) {
        auto chunkSlice = CreateInputChunkSlice(inputChunk);
        InferLimitsFromBoundaryKeys(chunkSlice, rowBuffer);
        auto dataSlice = CreateUnversionedInputDataSlice(chunkSlice);
        dataSlice->TransformToNew(rowBuffer, comparator.GetLength());

        auto sliceCount = DivCeil<i64>(size, minSliceSize);
        auto sliceSize = DivCeil<i64>(size, sliceCount);
        sliceSize = std::max(minSliceSize, sliceSize);

        YT_LOG_DEBUG_IF(enableVerboseLogging,
            "Adding chunk to chunk slice fetcher (SliceSize: %v, Comparator: %v, DataSlice: %v)",
            sliceSize,
            comparator,
            dataSlice);
        chunkSliceFetcher->AddDataSliceForSlicing(dataSlice, comparator, sliceSize, /*sliceByKeys*/ true);
    }

    YT_LOG_DEBUG("Fetching chunk slices");

    WaitFor(chunkSliceFetcher->Fetch())
        .ThrowOnError();

    YT_LOG_DEBUG("Chunk slices fetched");

    for (const auto& slice : chunkSliceFetcher->GetChunkSlices()) {
        reshardBuilder.AddSlice(slice);
    }

    YT_LOG_DEBUG("Computing pivot keys after slicing");

    reshardBuilder.ComputeSlicedChunksPivotKeys();

    if (!reshardBuilder.AreAllPivotsFound()) {
        THROW_ERROR_EXCEPTION("Could not reshard table %v to desired tablet count; consider reducing tablet count or specifying pivot keys manually",
            path)
            << TErrorAttribute("tablet_count", tabletCount)
            << TErrorAttribute("max_block_size", maxBlockSize);
    }

    return reshardBuilder.GetPivotKeys();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletClient
