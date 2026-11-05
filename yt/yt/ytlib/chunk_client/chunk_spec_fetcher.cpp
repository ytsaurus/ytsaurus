#include "chunk_spec_fetcher.h"

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/config.h>
#include <yt/yt/ytlib/api/native/connection.h>
#include <yt/yt/ytlib/api/native/rpc_helpers.h>
#include <yt/yt/ytlib/api/native/tablet_helpers.h>

#include <yt/yt/ytlib/hive/cell_directory.h>

#include <yt/yt/ytlib/chunk_client/helpers.h>

#include <yt/yt/ytlib/object_client/helpers.h>

#include <yt/yt/client/chunk_client/public.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/client/tablet_client/table_mount_cache.h>

#include <yt/yt/client/chunk_client/read_limit.h>

#include <yt/yt/core/misc/protobuf_helpers.h>

#include <library/cpp/iterator/functools.h>

#include <util/generic/cast.h>

namespace NYT::NChunkClient {

using namespace NApi;
using namespace NApi::NNative;
using namespace NChunkClient;
using namespace NConcurrency;
using namespace NLogging;
using namespace NNodeTrackerClient;
using namespace NObjectClient;
using namespace NQueryClient;
using namespace NRpc;
using namespace NTableClient;
using namespace NTabletClient;
using namespace NYPath;

using NYT::FromProto;
using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

TMasterChunkSpecFetcher::TMasterChunkSpecFetcher(
    NApi::NNative::IClientPtr client,
    TNodeDirectoryPtr nodeDirectory,
    IInvokerPtr invoker,
    TMasterChunkSpecFetcherOptions options,
    TLogger logger)
    : Client_(std::move(client))
    , NodeDirectory_(std::move(nodeDirectory))
    , Invoker_(std::move(invoker))
    , Options_(std::move(options))
    , Logger(std::move(logger))
{ }

void TMasterChunkSpecFetcher::Add(
    TObjectId objectId,
    TCellTag externalCellTag,
    i64 chunkCount,
    int tableIndex,
    const std::vector<TReadRange>& ranges)
{
    auto& state = GetCellState(externalCellTag);

    auto oldReqCount = state.ReqCount;

    for (int rangeIndex = 0; rangeIndex < std::ssize(ranges); ++rangeIndex) {
        // XXX(gritukan, babenko): YT-11825
        i64 subrequestCount = chunkCount < 0 ? 1 : (chunkCount + Options_.MaxChunksPerFetch - 1) / Options_.MaxChunksPerFetch;
        for (i64 index = 0; index < subrequestCount; ++index) {
            auto adjustedRange = ranges[rangeIndex];

            // XXX(gritukan, babenko): YT-11825
            if (chunkCount >= 0) {
                auto chunkCountLowerLimit = index * Options_.MaxChunksPerFetch;
                if (auto lowerChunkIndex = adjustedRange.LowerLimit().GetChunkIndex()) {
                    chunkCountLowerLimit = std::max(chunkCountLowerLimit, *lowerChunkIndex);
                }
                adjustedRange.LowerLimit().SetChunkIndex(chunkCountLowerLimit);

                auto chunkCountUpperLimit = (index + 1) * Options_.MaxChunksPerFetch;
                if (auto upperChunkIndex = adjustedRange.UpperLimit().GetChunkIndex()) {
                    chunkCountUpperLimit = std::min(chunkCountUpperLimit, *upperChunkIndex);
                }
                adjustedRange.UpperLimit().SetChunkIndex(chunkCountUpperLimit);
            }

            auto req = TChunkOwnerYPathProxy::Fetch(FromObjectId(objectId));
            AddCellTagToSyncWith(req, objectId);
            if (Options_.FetchRequestInitializer) {
                Options_.FetchRequestInitializer(req.Get(), tableIndex);
            }
            ToProto(req->mutable_ranges(), std::vector<NChunkClient::TReadRange>{adjustedRange});
            req->set_supported_chunk_features(ToUnderlying(GetSupportedChunkFeatures()));
            req->set_chunk_list_content_type(ToProto(Options_.ChunkListContentType));
            SetCachingHeader(req, Client_->GetNativeConnection(), Options_.MasterReadOptions);

            state.BatchReq->AddRequest(req, "fetch");
            ++state.ReqCount;
            state.RangeIndices.push_back(rangeIndex);
            state.TableIndices.push_back(tableIndex);
        }
    }

    ++TableCount_;
    // XXX(gritukan, babenko): YT-11825
    TotalChunkCount_ += chunkCount < 0 ? 1 : chunkCount;

    YT_TLOG_DEBUG("Table added for chunk spec fetching")
        .With("ObjectId", objectId)
        .With("ExternalCellTag", externalCellTag)
        .With("ChunkCount", chunkCount)
        .With("RangeCount", ranges.size())
        .With("TableIndex", tableIndex)
        .With("ReqCount", state.ReqCount - oldReqCount);
}

NNodeTrackerClient::TNodeDirectoryPtr TMasterChunkSpecFetcher::GetNodeDirectory() const
{
    return NodeDirectory_;
}

std::vector<NProto::TChunkSpec> TMasterChunkSpecFetcher::GetChunkSpecsOrderedNaturally() const
{
    std::vector<std::vector<NProto::TChunkSpec>> chunkSpecsPerTable(TableCount_);
    for (const auto& chunkSpec : ChunkSpecs_) {
        auto tableIndex = chunkSpec.table_index();
        YT_VERIFY(tableIndex < std::ssize(chunkSpecsPerTable));
        chunkSpecsPerTable[tableIndex].push_back(chunkSpec);
    }

    std::vector<NProto::TChunkSpec> chunkSpecs;
    chunkSpecs.reserve(TotalChunkCount_);
    for (const auto& table : chunkSpecsPerTable) {
        chunkSpecs.insert(chunkSpecs.end(), table.begin(), table.end());
    }

    return chunkSpecs;
}

TMasterChunkSpecFetcher::TCellState& TMasterChunkSpecFetcher::GetCellState(TCellTag cellTag)
{
    auto it = CellTagToState_.find(cellTag);
    if (it == CellTagToState_.end()) {
        it = CellTagToState_.insert({cellTag, TCellState()}).first;
        auto proxy = CreateObjectServiceReadProxy(
            Client_,
            Options_.MasterReadOptions.ReadFrom,
            cellTag);
        it->second.BatchReq = proxy.ExecuteBatchWithRetries(
            Client_->GetNativeConnection()->GetConfig()->ChunkFetchRetries);
        // TODO(dakovalkov): doesn't work with BatchWithRetries.
        // SetBalancingHeader(it->second.BatchReq, Client_->GetNativeConnection(), Options_.MasterReadOptions);
    }
    return it->second;
}

TFuture<void> TMasterChunkSpecFetcher::Fetch()
{
    return BIND(&TMasterChunkSpecFetcher::DoFetch, MakeWeak(this))
        .AsyncVia(Invoker_)
        .Run();
}

void TMasterChunkSpecFetcher::DoFetch()
{
    YT_TLOG_DEBUG("Fetching chunk specs from masters")
        .With("CellCount", CellTagToState_.size())
        .With("TotalChunkCount", TotalChunkCount_)
        .With("TableCount", TableCount_);

    std::vector<TFuture<void>> asyncResults;
    for (auto& [cellTag, cellState] : CellTagToState_) {
        asyncResults.emplace_back(BIND(&TMasterChunkSpecFetcher::DoFetchFromCell, MakeWeak(this), cellTag)
            .AsyncVia(Invoker_)
            .Run());
    }
    WaitFor(AllSucceeded(asyncResults))
        .ThrowOnError();

    std::vector<NProto::TChunkSpec*> foreignChunkSpecs;
    for (const auto& [cellTag, cellState] : CellTagToState_) {
        const auto& cellForeignChunkSpecs = cellState.ForeignChunkSpecs;
        foreignChunkSpecs.insert(foreignChunkSpecs.end(), cellForeignChunkSpecs.begin(), cellForeignChunkSpecs.end());
    }

    if (!foreignChunkSpecs.empty()) {
        YT_TLOG_DEBUG("Locating foreign chunks")
            .With("ForeignChunkCount", foreignChunkSpecs.size());
        // TODO(dakovalkov): Use MasterReadOptions.
        LocateChunks(Client_, Options_.MaxChunksPerLocateRequest, foreignChunkSpecs, NodeDirectory_, Logger, Options_.SkipUnavailableChunks);
        YT_TLOG_DEBUG("Finished locating foreign chunks");
    }

    for (auto& [cellTag, cellState] : CellTagToState_) {
        for (auto& chunkSpec : cellState.ChunkSpecs) {
            ChunkSpecs_.emplace_back().Swap(&chunkSpec);
        }
    }

    YT_TLOG_DEBUG("Chunk specs fetched from masters")
        .With("ChunkCount", ChunkSpecs_.size());
}

void TMasterChunkSpecFetcher::DoFetchFromCell(TCellTag cellTag)
{
    auto& cellState = CellTagToState_[cellTag];

    YT_TLOG_DEBUG("Fetching chunk specs from master cell")
        .With("CellTag", cellTag)
        .With("FetchRequestCount", cellState.ReqCount);

    auto batchRspOrError = WaitFor(cellState.BatchReq->Invoke());
    THROW_ERROR_EXCEPTION_IF_FAILED(
        GetCumulativeError(batchRspOrError),
        "Error fetching chunk specs from master cell %v",
        cellTag);

    const auto& batchRsp = batchRspOrError.Value();
    auto rspsOrError = batchRsp->GetResponses<TChunkOwnerYPathProxy::TRspFetch>("fetch");

    for (int resultIndex = 0; resultIndex < std::ssize(rspsOrError); ++resultIndex) {
        auto& rsp = rspsOrError[resultIndex].Value();
        for (auto& chunkSpec : *rsp->mutable_chunks()) {
            chunkSpec.set_table_index(cellState.TableIndices[resultIndex]);
            chunkSpec.set_range_index(cellState.RangeIndices[resultIndex]);
            cellState.ChunkSpecs.emplace_back().Swap(&chunkSpec);
        }
        if (NodeDirectory_) {
            NodeDirectory_->MergeFrom(rsp->node_directory());
        }
    }

    for (auto& chunkSpec : cellState.ChunkSpecs) {
        auto chunkId = NYT::FromProto<TChunkId>(chunkSpec.chunk_id());
        auto chunkCellTag = CellTagFromId(chunkId);
        if (chunkCellTag != cellTag) {
            cellState.ForeignChunkSpecs.push_back(&chunkSpec);
        }
    }
    YT_TLOG_DEBUG("Finished processing chunk specs from master cell")
        .With("CellTag", cellTag)
        .With("FetchedChunkCount", cellState.ChunkSpecs.size())
        .With("ForeignChunkCount", cellState.ForeignChunkSpecs.size());
}

////////////////////////////////////////////////////////////////////////////////

TTabletChunkSpecFetcher::TTabletChunkSpecFetcher(
    TOptions options,
    const IInvokerPtr& invoker,
    const TLogger& logger)
    : Options_(std::move(options))
    , Invoker_(invoker)
    , Logger(logger)
{ }

void TTabletChunkSpecFetcher::Add(
    const TYPath& path,
    i64 chunkIndex,
    int tableIndex,
    const std::vector<TReadRange>& ranges)
{
    TotalChunkCount_ += chunkIndex;
    ++TableCount_;

    const auto& tableMountCache = Options_.Client->GetTableMountCache();
    auto mountInfo = WaitFor(tableMountCache->GetTableInfo(path))
        .ValueOrThrow();
    mountInfo->ValidateDynamic();
    // Currently only sorted dynamic tables are supported.
    mountInfo->ValidateSorted();
    mountInfo->ValidateNotPhysicallyLog();

    AddSorted(*mountInfo, tableIndex, ranges);
}

void TTabletChunkSpecFetcher::AddSorted(
    const TTableMountInfo& tableMountInfo,
    int tableIndex,
    const std::vector<TReadRange>& ranges)
{
    const auto& comparator = tableMountInfo.Schemas[ETableSchemaKind::Primary]->ToComparator();
    YT_VERIFY(comparator);

    auto validateReadLimit = [&] (const TReadLimit& readLimit, TStringBuf limitKind) {
        try {
            if (readLimit.GetRowIndex()) {
                THROW_ERROR_EXCEPTION("Row index selectors are not supported for sorted dynamic tables");
            }
            if (readLimit.GetOffset()) {
                THROW_ERROR_EXCEPTION("Offset selectors are not supported for tables");
            }
            if (readLimit.GetTabletIndex()) {
                THROW_ERROR_EXCEPTION("Tablet index selectors are only supported for ordered dynamic tables");
            }
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Invalid %v limit for table %Qv", limitKind, tableMountInfo.Path)
                .With(ex);
        }
    };

    const auto& tabletInfos = tableMountInfo.Tablets;

    // Aggregate subrequests per-tablet. Note that there may be more than one read range,
    // so each subrequest may ask about multiple ranges.
    std::vector<std::optional<TSubrequest>> tabletIndexToSubrequest(tabletInfos.size());

    for (const auto& [rangeIndex, range] : Enumerate(ranges)) {
        validateReadLimit(range.LowerLimit(), "lower");
        validateReadLimit(range.UpperLimit(), "upper");

        size_t tabletIndex = 0;
        if (range.LowerLimit().KeyBound()) {
            tabletIndex = std::upper_bound(
                tabletInfos.begin(),
                tabletInfos.end(),
                range.LowerLimit().KeyBound(),
                [&] (const TKeyBound& lowerBound, const TTabletInfoPtr& tabletInfo) {
                    return comparator.CompareKeyBounds(lowerBound, tabletInfo->GetLowerKeyBound()) < 0;
                }) - tabletInfos.begin();
            if (tabletIndex != 0) {
                --tabletIndex;
            }
        }

        for (; tabletIndex != tabletInfos.size(); ++tabletIndex) {
            const auto& tabletInfo = tabletInfos[tabletIndex];

            auto tabletLowerBound = tabletInfo->GetLowerKeyBound();

            if (range.UpperLimit().KeyBound() &&
                comparator.IsRangeEmpty(tabletLowerBound, range.UpperLimit().KeyBound()))
            {
                break;
            }

            auto tabletUpperBound = tabletIndex + 1 == tabletInfos.size()
                ? TKeyBound::MakeUniversal(/*isUpper*/ true)
                : tabletInfos[tabletIndex + 1]->GetLowerKeyBound().Invert();

            auto subrangeLowerBound = tabletLowerBound;
            if (range.LowerLimit().KeyBound()) {
                comparator.ReplaceIfStrongerKeyBound<TKeyBound>(subrangeLowerBound, range.LowerLimit().KeyBound());
            }
            auto subrangeUpperBound = tabletUpperBound;
            if (range.UpperLimit().KeyBound()) {
                comparator.ReplaceIfStrongerKeyBound<TKeyBound>(subrangeUpperBound, range.UpperLimit().KeyBound());
            }

            TReadRange subrange = range;
            subrange.LowerLimit().KeyBound() = subrangeLowerBound.ToOwning();
            subrange.UpperLimit().KeyBound() = subrangeUpperBound.ToOwning();

            if (comparator.IsRangeEmpty(subrangeLowerBound, subrangeUpperBound)) {
                continue;
            }

            auto& subrequest = tabletIndexToSubrequest[tabletIndex];
            if (!subrequest) {
                subrequest.emplace();
                subrequest->set_table_index(tableIndex);
                subrequest->set_mount_revision(ToProto(tabletInfo->MountRevision));
                ToProto(subrequest->mutable_tablet_id(), tabletInfo->TabletId);
                ToProto(subrequest->mutable_cell_id(), tabletInfo->CellId);
            }

            subrequest->add_range_indices(rangeIndex);
            ToProto(subrequest->add_ranges(), subrange);

            YT_TLOG_TRACE("Adding range for tablet")
                .With("Path", tableMountInfo.Path)
                .With("TabletIndex", tabletIndex)
                .With("TabletLowerBound", tabletLowerBound)
                .With("TabletUpperBound", tabletUpperBound)
                .With("SubrangeLowerBound", subrangeLowerBound)
                .With("SubrangeUpperBound", subrangeUpperBound);
        }
    }

    // Finally assign per-tablet subrequests to corresponding tablet nodes.
    const auto& connection = Options_.Client->GetNativeConnection();
    const auto& cellDirectory = connection->GetCellDirectory();

    for (size_t tabletIndex = 0; tabletIndex < tabletInfos.size(); ++tabletIndex) {
        const auto& tablet = tabletInfos[tabletIndex];
        auto& subrequest = tabletIndexToSubrequest[tabletIndex];
        if (subrequest) {
            YT_TLOG_TRACE("Adding subrequest for tablet")
                .With("Path", tableMountInfo.Path)
                .With("TabletIndex", tabletIndex)
                .With("TabletId", tablet->TabletId)
                .With("CellId", tablet->CellId);
            auto cellId = tablet->CellId;
            auto cellDescriptor = cellDirectory->GetDescriptorByCellIdOrThrow(cellId);
            const auto& primaryPeerDescriptor = NApi::NNative::GetPrimaryTabletPeerDescriptor(
                *cellDescriptor,
                NHydra::EPeerKind::Leader);

            const auto& address = primaryPeerDescriptor.GetAddressOrThrow(connection->GetNetworks());
            auto& state = NodeAddressToState_[address];
            state.Subrequests.emplace_back(std::move(*subrequest));
            state.Tablets.push_back(std::move(tablet));
        }
    }
}

TFuture<void> TTabletChunkSpecFetcher::Fetch()
{
    return BIND(&TTabletChunkSpecFetcher::DoFetch, MakeWeak(this))
        .AsyncVia(Invoker_)
        .Run();
}

void TTabletChunkSpecFetcher::DoFetch()
{
    YT_TLOG_DEBUG("Fetching chunk specs from tablet nodes")
        .With("NodeCount", NodeAddressToState_.size())
        .With("TotalChunkCount", TotalChunkCount_)
        .With("TableCount", TableCount_);

    std::vector<TFuture<void>> asyncResults;
    for (auto& address : GetKeys(NodeAddressToState_)) {
        asyncResults.emplace_back(BIND(&TTabletChunkSpecFetcher::DoFetchFromNode, MakeWeak(this), address)
            .AsyncVia(Invoker_)
            .Run());
    }
    WaitFor(AllSucceeded(asyncResults))
        .ThrowOnError();

    std::vector<TTabletId> missingTabletIds;

    for (auto& state : GetValues(NodeAddressToState_)) {
        for (auto& chunkSpec : state.ChunkSpecs) {
            ChunkSpecs_.emplace_back().Swap(&chunkSpec);
        }
        for (const auto& missingTabletId : state.MissingTabletIds) {
            missingTabletIds.emplace_back(missingTabletId);
        }
    }

    YT_TLOG_DEBUG("Chunk specs fetched from tablet nodes")
        .With("ChunkCount", ChunkSpecs_.size())
        .With("MissingTabletCount", missingTabletIds.size())
        .With("MissingTabletIds", MakeShrunkFormattableView(missingTabletIds, TDefaultFormatter(), MissingTabletIdCountLimit));

    if (!missingTabletIds.empty()) {
        if (missingTabletIds.size() > MissingTabletIdCountLimit) {
            missingTabletIds.resize(MissingTabletIdCountLimit);
        }
        THROW_ERROR_EXCEPTION("Error while fetching chunks due to missing tablets %v",
            missingTabletIds);
    }
}

void TTabletChunkSpecFetcher::DoFetchFromNode(const std::string& address)
{
    auto& state = NodeAddressToState_[address];

    YT_TLOG_DEBUG("Fetching chunk specs from tablet node")
        .With("Address", address)
        .With("TabletCount", state.Subrequests.size());

    const auto& connection = Options_.Client->GetNativeConnection();
    const auto& tableMountCache = connection->GetTableMountCache();
    auto channel = connection->GetChannelFactory()->CreateChannel(address);

    TQueryServiceProxy proxy(std::move(channel));

    auto req = proxy.FetchTabletStores();
    ToProto(req->mutable_subrequests(), state.Subrequests);
    Options_.InitializeFetchRequest(req.Get());
    req->SetResponseCodec(Options_.ResponseCodecId);

    auto rsp = WaitFor(req->Invoke())
        .ValueOrThrow();

    YT_VERIFY(std::ssize(rsp->subresponses()) == std::ssize(state.Subrequests));

    // TODO(max42): introduce proper retrying policy.
    for (const auto& [index, subresponse] : Enumerate(*rsp->mutable_subresponses())) {
        if (subresponse.tablet_missing() || subresponse.has_error()) {
            auto error = FromProto<TError>(subresponse.error());
            YT_TLOG_TRACE("Received error from tablet")
                .With(error);
            if (subresponse.tablet_missing() || error.GetCode() == NTabletClient::EErrorCode::NoSuchTablet) {
                const auto& tablet = state.Tablets[index];
                tableMountCache->InvalidateTablet(tablet->TabletId);
                state.MissingTabletIds.push_back(tablet->TabletId);
            } else {
                THROW_ERROR(error);
            }
        } else {
            for (auto& chunkSpec : *subresponse.mutable_stores()) {
                YT_TLOG_TRACE("Received chunk spec from tablet")
                    .With("ChunkSpec", chunkSpec.ShortDebugString());
                state.ChunkSpecs.push_back(std::move(chunkSpec));
            }
        }
    }

    YT_TLOG_DEBUG("Finished processing chunk specs from tablet node")
        .With("Address", address)
        .With("FetchedChunkCount", state.ChunkSpecs.size())
        .With("MissingTabletCount", state.MissingTabletIds.size())
        .With("MissingTabletIds", MakeShrunkFormattableView(state.MissingTabletIds, TDefaultFormatter(), MissingTabletIdCountLimit));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
