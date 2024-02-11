#include "chunk_slice_fetcher.h"

#include "chunk_meta_extensions.h"
#include "key_set.h"
#include "private.h"

#include <yt/yt/ytlib/chunk_client/config.h>
#include <yt/yt/ytlib/chunk_client/dispatcher.h>
#include <yt/yt/ytlib/chunk_client/input_chunk.h>
#include <yt/yt/ytlib/chunk_client/input_chunk_slice.h>
#include <yt/yt/ytlib/chunk_client/legacy_data_slice.h>

#include <yt/yt/ytlib/tablet_client/helpers.h>

#include <yt/yt/client/chunk_client/chunk_replica.h>

#include <yt/yt/client/rpc/helpers.h>

#include <yt/yt/client/table_client/row_buffer.h>

#include <yt/yt/client/node_tracker_client/node_directory.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/core/concurrency/scheduler.h>

#include <yt/yt/core/misc/protobuf_helpers.h>

#include <yt/yt/core/rpc/channel.h>

namespace NYT::NTableClient {

using namespace NConcurrency;
using namespace NTableClient;
using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NNodeTrackerClient;
using namespace NRpc;
using namespace NObjectClient;
using namespace NTabletClient;

using NYT::FromProto;
using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

//! Fetches slices for a bunch of table chunks by requesting
//! them directly from data nodes.
class TChunkSliceFetcher
    : public IChunkSliceFetcher
    , public NChunkClient::TFetcherBase
{
public:
    TChunkSliceFetcher(
        TChunkSliceFetcherConfigPtr config,
        NNodeTrackerClient::TNodeDirectoryPtr nodeDirectory,
        IInvokerPtr invoker,
        IFetcherChunkScraperPtr chunkScraper,
        NApi::NNative::IClientPtr client,
        TRowBufferPtr rowBuffer,
        const NLogging::TLogger& logger)
        : TFetcherBase(
            config,
            nodeDirectory,
            invoker,
            chunkScraper,
            client,
            logger)
        , Config_(std::move(config))
        , RowBuffer_(std::move(rowBuffer))
    { }

    void AddChunk(TInputChunkPtr /*chunk*/) override
    {
        YT_UNIMPLEMENTED();
    }

    void AddDataSliceForSlicing(
        TLegacyDataSlicePtr dataSlice,
        const TComparator& comparator,
        i64 sliceDataWeight,
        bool sliceByKeys) override
    {
        YT_VERIFY(sliceDataWeight > 0);

        auto chunk = dataSlice->GetSingleUnversionedChunk();
        TFetcherBase::AddChunk(chunk);

        YT_VERIFY(dataSlice->ChunkSlices.size() == 1);

        // Note that we do not patch chunk slices limits anywhere in chunk pool as it is
        // a part of data slice physical representation. In future it is going to become
        // hidden in physical data reigstry.
        //
        // As a consequence, by this moment limit in chunk slice may be longer than needed,
        // so we copy chunk slice for internal chunk slice fetcher needs and replace
        // chunk slice limits with data slice limits which are already proper (i.e. have length
        // of #keyColumnCount).
        //
        // This logic fixes test_scheduler_reduce.py::TestSchedulerReduceCommands::test_column_filter.

        auto chunkSliceCopy = CreateInputChunkSlice(*dataSlice->ChunkSlices[0]);
        chunkSliceCopy->LegacyLowerLimit() = dataSlice->LegacyLowerLimit();
        chunkSliceCopy->LegacyUpperLimit() = dataSlice->LegacyUpperLimit();
        chunkSliceCopy->LowerLimit() = dataSlice->LowerLimit();
        chunkSliceCopy->UpperLimit() = dataSlice->UpperLimit();

        auto dataSliceCopy = CreateUnversionedInputDataSlice(chunkSliceCopy);

        TChunkSliceRequest chunkSliceRequest {
            .Comparator = comparator,
            .ChunkSliceDataWeight = sliceDataWeight,
            .SliceByKeys = sliceByKeys,
            .DataSlice = dataSliceCopy,
        };
        YT_VERIFY(ChunkToChunkSliceRequest_.emplace(chunk, chunkSliceRequest).second);
    }

    TFuture<void> Fetch() override
    {
        YT_LOG_DEBUG("Started fetching chunk slices (ChunkCount: %v)",
            Chunks_.size());
        return TFetcherBase::Fetch();
    }

    std::vector<NChunkClient::TInputChunkSlicePtr> GetChunkSlices() override
    {
        std::vector<NChunkClient::TInputChunkSlicePtr> chunkSlices;
        chunkSlices.reserve(SliceCount_);
        for (const auto& slices : SlicesByChunkIndex_) {
            chunkSlices.insert(chunkSlices.end(), slices.begin(), slices.end());
        }
        return chunkSlices;
    }

private:
    const TChunkSliceFetcherConfigPtr Config_;
    const NTableClient::TRowBufferPtr RowBuffer_;

    //! All slices fetched so far.
    std::vector<std::vector<NChunkClient::TInputChunkSlicePtr>> SlicesByChunkIndex_;

    //! Number of slices in SlicesByChunkIndex_.
    i64 SliceCount_ = 0;

    struct TChunkSliceRequest
    {
        TComparator Comparator;
        i64 ChunkSliceDataWeight;
        bool SliceByKeys;
        TLegacyDataSlicePtr DataSlice;
    };
    THashMap<TInputChunkPtr, TChunkSliceRequest> ChunkToChunkSliceRequest_;

    void OnFetchingStarted() override
    {
        SlicesByChunkIndex_.resize(Chunks_.size());
    }

    void ProcessDynamicStore(int chunkIndex) override
    {
        AddTrivialSlice(chunkIndex);
    }

    TFuture<void> FetchFromNode(
        NNodeTrackerClient::TNodeId nodeId,
        std::vector<int> chunkIndexes) override
    {
        return BIND(&TChunkSliceFetcher::DoFetchFromNode, MakeStrong(this), nodeId, Passed(std::move(chunkIndexes)))
            .AsyncVia(Invoker_)
            .Run();
    }

    TFuture<void> DoFetchFromNode(
        NNodeTrackerClient::TNodeId nodeId,
        const std::vector<int>& chunkIndexes)
    {
        TDataNodeServiceProxy proxy(GetNodeChannel(nodeId));
        proxy.SetDefaultTimeout(Config_->NodeRpcTimeout);

        std::vector<TFuture<void>> futures;

        TDataNodeServiceProxy::TReqGetChunkSlicesPtr req;
        std::vector<int> requestedChunkIndexes;

        auto createRequest = [&] {
            req = proxy.GetChunkSlices();
            // TODO(babenko): make configurable
            SetRequestWorkloadDescriptor(req, TWorkloadDescriptor(EWorkloadCategory::UserBatch));
            req->SetRequestHeavy(true);
            req->SetResponseHeavy(true);
            req->SetMultiplexingBand(EMultiplexingBand::Heavy);
        };

        auto flushBatch = [&] {
            if (req->slice_requests_size() > 0) {
                futures.push_back(
                    req->Invoke().Apply(
                        BIND(&TChunkSliceFetcher::OnResponse, MakeStrong(this), nodeId, Passed(std::move(requestedChunkIndexes)))
                            .AsyncVia(Invoker_)));

                requestedChunkIndexes.clear();
                createRequest();
            }
        };

        createRequest();

        for (auto chunkIndex : chunkIndexes) {
            const auto& chunk = Chunks_[chunkIndex];
            const auto& sliceRequest = ChunkToChunkSliceRequest_[chunk];

            auto chunkDataSize = chunk->GetDataWeight();

            if (!chunk->BoundaryKeys()) {
                THROW_ERROR_EXCEPTION("Missing boundary keys in chunk %v", chunk->GetChunkId());
            }

            const auto& comparator = sliceRequest.Comparator;
            auto minKey = chunk->BoundaryKeys()->MinKey;
            auto maxKey = chunk->BoundaryKeys()->MaxKey;
            auto chunkSliceDataWeight = sliceRequest.ChunkSliceDataWeight;
            auto sliceByKeys = sliceRequest.SliceByKeys;

            // TODO(gritukan): Comparing rows using == here is ok, but quite ugly.
            if (chunkDataSize < chunkSliceDataWeight || (sliceByKeys && minKey == maxKey)) {
                AddTrivialSlice(chunkIndex);
            } else {
                requestedChunkIndexes.push_back(chunkIndex);
                auto chunkId = EncodeChunkId(chunk, nodeId);

                auto* protoSliceRequest = req->add_slice_requests();
                ToProto(protoSliceRequest->mutable_chunk_id(), chunkId);

                if (sliceRequest.DataSlice->IsLegacy) {
                    ToProto(protoSliceRequest->mutable_lower_limit(), sliceRequest.DataSlice->LegacyLowerLimit());
                    ToProto(protoSliceRequest->mutable_upper_limit(), sliceRequest.DataSlice->LegacyUpperLimit());
                } else {
                    ToProto(protoSliceRequest->mutable_lower_limit(), sliceRequest.DataSlice->LowerLimit());
                    ToProto(protoSliceRequest->mutable_upper_limit(), sliceRequest.DataSlice->UpperLimit());
                }

                // TODO(max42, gritukan): this field seems useless. Consider dropping it here and in proto message.
                protoSliceRequest->set_erasure_codec(static_cast<int>(chunk->GetErasureCodec()));
                protoSliceRequest->set_slice_data_weight(chunkSliceDataWeight);
                protoSliceRequest->set_slice_by_keys(sliceByKeys);
                protoSliceRequest->set_key_column_count(comparator.GetLength());
            }

            if (req->slice_requests_size() >= Config_->MaxSlicesPerFetch) {
                flushBatch();
            }
        }

        flushBatch();

        return AllSucceeded(futures);
    }

    void OnResponse(
        NNodeTrackerClient::TNodeId nodeId,
        const std::vector<int>& requestedChunkIndexes,
        const NChunkClient::TDataNodeServiceProxy::TErrorOrRspGetChunkSlicesPtr& rspOrError)
    {
        if (!rspOrError.IsOK()) {
            YT_LOG_INFO("Failed to get chunk slices from node (Address: %v, NodeId: %v)",
                NodeDirectory_->GetDescriptor(nodeId).GetDefaultAddress(),
                nodeId);

            OnNodeFailed(nodeId, requestedChunkIndexes);

            if (rspOrError.FindMatching(EErrorCode::IncomparableTypes)) {
                // Any exception thrown here interrupts fetching.
                rspOrError.ThrowOnError();
            }
            return;
        }

        const auto& rsp = rspOrError.Value();

        TKeySetReader keysReader(rsp->Attachments()[0]);
        std::unique_ptr<TKeySetReader> keyBoundsReader;
        // COMPAT(gritukan)
        if (rsp->Attachments().size() == 2) {
            keyBoundsReader.reset(new TKeySetReader(rsp->Attachments()[1]));
        }
        auto keys = keysReader.GetKeys();
        TRange<TLegacyKey> keyBoundPrefixes;
        if (keyBoundsReader) {
            keyBoundPrefixes = keyBoundsReader->GetKeys();
        }

        for (int i = 0; i < std::ssize(requestedChunkIndexes); ++i) {
            int index = requestedChunkIndexes[i];
            const auto& chunk = Chunks_[index];
            const auto& sliceRequest = ChunkToChunkSliceRequest_[chunk];
            const auto& sliceResponse = rsp->slice_responses(i);

            if (sliceResponse.has_error()) {
                auto error = FromProto<TError>(sliceResponse.error());

                if (error.FindMatching(EErrorCode::IncompatibleKeyColumns)) {
                    // Any exception thrown here interrupts fetching.
                    error.ThrowOnError();
                }

                OnChunkFailed(nodeId, index, error);
                continue;
            }

            YT_LOG_TRACE("Received %v chunk slices for chunk #%v",
                sliceResponse.chunk_slices_size(),
                index);

            YT_VERIFY(index < std::ssize(SlicesByChunkIndex_));

            const auto& originalChunkSlice = sliceRequest.DataSlice->ChunkSlices[0];

            int chunkSliceIndex = 0;

            for (const auto& protoChunkSlice : sliceResponse.chunk_slices()) {
                TInputChunkSlicePtr chunkSlice;
                if (sliceRequest.DataSlice->IsLegacy) {
                    chunkSlice = New<TInputChunkSlice>(*originalChunkSlice, RowBuffer_, protoChunkSlice, keys);
                } else {
                    chunkSlice = New<TInputChunkSlice>(*originalChunkSlice, sliceRequest.Comparator, RowBuffer_, protoChunkSlice, keys, keyBoundPrefixes);
                    InferLimitsFromBoundaryKeys(chunkSlice);
                }
                chunkSlice->SetSliceIndex(chunkSliceIndex++);
                SlicesByChunkIndex_[index].push_back(chunkSlice);
                SliceCount_++;
            }
        }
    }

    void InferLimitsFromBoundaryKeys(const TInputChunkSlicePtr chunkSlice) const
    {
        // New data slices infer their limits from chunk slice limits, so it is
        // more convenient (though it is not necessary) to have chunk slices that
        // always impose some non-trivial lower or upper key bound limit.
        if (!chunkSlice->LowerLimit().KeyBound || chunkSlice->LowerLimit().KeyBound.IsUniversal()) {
            chunkSlice->LowerLimit().KeyBound = TKeyBound::FromRowUnchecked(
                chunkSlice->GetInputChunk()->BoundaryKeys()->MinKey,
                /*isInclusive*/ true,
                /*isUpper*/ false);
        }
        if (!chunkSlice->UpperLimit().KeyBound || chunkSlice->UpperLimit().KeyBound.IsUniversal()) {
            chunkSlice->UpperLimit().KeyBound = TKeyBound::FromRowUnchecked(
                chunkSlice->GetInputChunk()->BoundaryKeys()->MaxKey,
                /*isInclusive*/ true,
                /*isUpper*/ true);
        }
    }

    void AddTrivialSlice(int chunkIndex)
    {
        YT_VERIFY(chunkIndex < std::ssize(SlicesByChunkIndex_));
        const auto& chunk = Chunks_[chunkIndex];
        const auto& sliceRequest = GetOrCrash(ChunkToChunkSliceRequest_, chunk);
        auto chunkSlice = sliceRequest.DataSlice->ChunkSlices[0];

        SlicesByChunkIndex_[chunkIndex].push_back(chunkSlice);
        // NB: we cannot infer limits from boundary keys here since chunk may actually be a dynamic store.
        SliceCount_++;
    }
};

////////////////////////////////////////////////////////////////////////////////

IChunkSliceFetcherPtr CreateChunkSliceFetcher(
    TChunkSliceFetcherConfigPtr config,
    TNodeDirectoryPtr nodeDirectory,
    IInvokerPtr invoker,
    IFetcherChunkScraperPtr chunkScraper,
    NApi::NNative::IClientPtr client,
    TRowBufferPtr rowBuffer,
    const NLogging::TLogger& logger)
{
    return New<TChunkSliceFetcher>(
        config,
        nodeDirectory,
        invoker,
        chunkScraper,
        client,
        rowBuffer,
        logger);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
