#include "erasure_parts_reader.h"
#include "helpers.h"
#include "config.h"

#include <yt/yt/ytlib/chunk_client/replication_reader.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader_options.h>
#include <yt/yt/ytlib/chunk_client/chunk_meta_extensions.h>
#include <yt/yt/ytlib/chunk_client/config.h>

#include <yt/yt/client/chunk_client/chunk_replica.h>
#include <yt/yt_proto/yt/client/chunk_client/proto/chunk_meta.pb.h>

#include <yt/yt/client/node_tracker_client/node_directory.h>

#include <yt/yt/library/erasure/impl/codec.h>

#include <yt/yt/core/misc/finally.h>

#include <yt/yt/core/rpc/dispatcher.h>

#include <library/cpp/yt/compact_containers/compact_set.h>

#include <util/generic/algorithm.h>

namespace NYT::NJournalClient {

using namespace NChunkClient;
using namespace NConcurrency;
using namespace NNodeTrackerClient;
using namespace NApi::NNative;
using namespace NYT::NErasure;
using namespace ::NErasure;

using TPartIndexSet = TCompactSet<int, ::NErasure::MaxTotalPartCount>;

////////////////////////////////////////////////////////////////////////////////

class TErasurePartsReader::TReadRowsSession
    : public TRefCounted
{
public:
    TReadRowsSession(
        TErasurePartsReaderPtr reader,
        const TClientChunkReadOptions& options,
        int firstRowIndex,
        int readRowCount)
        : Reader_(std::move(reader))
        , Options_(options)
        , FirstRowIndex_(firstRowIndex)
        , ReadRowCount_(readRowCount)
        , Logger(Reader_->Logger().WithTag("ErasurePartsReaderSessionId: %v", TGuid::Create()))
    { }

    TFuture<std::vector<std::vector<TSharedRef>>> Run()
    {
        if (ReadRowCount_ <= 0) {
            return MakeFuture(std::vector<std::vector<TSharedRef>>(Reader_->PartIndices_.size()));
        }

        YT_LOG_DEBUG("Erasure rows read session started (FirstRowIndex: %v, RowCount: %v, PartIndices: %v)",
            FirstRowIndex_,
            ReadRowCount_,
            Reader_->PartIndices_);

        const auto& chunkReaders = Reader_->ChunkReaders_;
        for (const auto& reader : chunkReaders) {
            int partIndex = DecodeChunkId(reader->GetChunkId()).ReplicaIndex;

            auto& partSession = PartSessions_[partIndex];
            partSession.PartIndex = partIndex;
            partSession.ReplicaSessions.push_back(TPartReadingSession::TReplicaSession{
                .Reader = reader,
            });
        }

        for (auto& [partIndex, partSession] : PartSessions_) {
            YT_LOG_DEBUG("Requesting part meta (PartIndex: %v, ReplicaCount: %v)",
                partIndex,
                partSession.ReplicaSessions.size());

            for (auto& replicaSession : partSession.ReplicaSessions) {
                replicaSession.MetaFuture = replicaSession.Reader->GetMeta(IChunkReader::TGetMetaOptions{
                    .ClientOptions = Options_,
                });
            }
        }

        // NB: Only subscribe to futures once all meta requests are initialized.
        for (auto& [partIndex, partSession] : PartSessions_) {
            for (int replicaIndex = 0; replicaIndex < std::ssize(partSession.ReplicaSessions); ++replicaIndex) {
                auto& replicaSession = partSession.ReplicaSessions[replicaIndex];
                replicaSession.MetaFuture.Subscribe(
                    BIND(&TReadRowsSession::OnGotMetaFromReplica, MakeStrong(this), partIndex, replicaIndex)
                        .Via(NRpc::TDispatcher::Get()->GetHeavyInvoker()));
            }
        }

        Promise_.OnCanceled(
            BIND(&TReadRowsSession::OnSessionCanceled, MakeWeak(this)));

        return Promise_.ToFuture();
    }

private:
    const TErasurePartsReaderPtr Reader_;
    const TClientChunkReadOptions Options_;
    const int FirstRowIndex_;
    const int ReadRowCount_;

    const NLogging::TLogger Logger;

    const TPromise<std::vector<std::vector<TSharedRef>>> Promise_ = NewPromise<std::vector<std::vector<TSharedRef>>>();

    bool Finished_ = false;

    struct TPartReadingSession
    {
        struct TReplicaSession
        {
            const IChunkReaderPtr Reader;

            TFuture<TRefCountedChunkMetaPtr> MetaFuture;
            bool GotMetaResponse = false;

            int RowCount = 0;
            bool Sealed = false;
            i64 DataSize = 0;
            int ReplicaReadRowCount = 0;

            TFuture<std::vector<TBlock>> DataFuture;


            bool IsEligibleForRequestingData() const
            {
                if (DataFuture) {
                    return false;
                }

                if (!GotMetaResponse) {
                    return false;
                }

                if (ReplicaReadRowCount <= 0) {
                    return false;
                }

                return true;
            }
        };

        int PartIndex = -1;

        bool Finished = false;

        // NB: We distinguish replica readers of each part because replicas are not necessarily identical.
        // It may happen e.g. due to repair job being stuck or failed, leaving an (unsealed) unfinished replica.
        std::vector<TReplicaSession> ReplicaSessions;

        bool HasPendingDataRequest = false;
        std::optional<std::vector<TBlock>> Data;


        std::optional<int> GetReplicaToReadDataFrom() const
        {
            // NB: No need to do more reads - even partial data is enough.
            if (Data) {
                return std::nullopt;
            }

            // NB: Do not run simultaneous data requests to part replicas.
            if (HasPendingDataRequest) {
                return std::nullopt;
            }

            for (int replicaIndex = 0; replicaIndex < std::ssize(ReplicaSessions); ++replicaIndex) {
                const auto& replicaSession = ReplicaSessions[replicaIndex];
                if (replicaSession.IsEligibleForRequestingData()) {
                    return replicaIndex;
                }
            }

            return std::nullopt;
        }

        bool IsFinished() const
        {
            if (Data) {
                return true;
            }

            if (HasPendingDataRequest) {
                return false;
            }

            for (int replicaIndex = 0; replicaIndex < std::ssize(ReplicaSessions); ++replicaIndex) {
                const auto& replicaSession = ReplicaSessions[replicaIndex];

                YT_VERIFY(replicaSession.MetaFuture);
                if (!replicaSession.MetaFuture.IsSet()) {
                    return false;
                }

                if (replicaSession.IsEligibleForRequestingData()) {
                    return false;
                }
            }

            return true;
        }
    };

    std::unordered_map<int, TPartReadingSession> PartSessions_;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, Lock_);


    void TryScheduleNextRead(TGuard<NThreading::TSpinLock>&& guard, int partIndex)
    {
        const auto& partSession = GetOrCrash(PartSessions_, partIndex);
        if (partSession.IsFinished()) {
            OnPartSessionFinished(partIndex, std::move(guard));
        } else if (auto replicaIndex = partSession.GetReplicaToReadDataFrom()) {
            ReadDataFromReplica(partIndex, *replicaIndex);
        }

        // NB: In case none of the branches above is true we will just keep waiting
        // for some other inflight (meta or data) requests to finish.
    }

    void OnGotMetaFromReplica(int partIndex, int replicaIndex, const TErrorOr<TRefCountedChunkMetaPtr>& metaOrError)
    {
        auto guard = Guard(Lock_);

        if (Finished_) {
            return;
        }

        ProcceesReplicaMetaResult(
            partIndex,
            replicaIndex,
            metaOrError);

        TryScheduleNextRead(std::move(guard), partIndex);
    }

    void ProcceesReplicaMetaResult(
        int partIndex,
        int replicaIndex,
        const TErrorOr<TRefCountedChunkMetaPtr>& metaOrError)
    {
        YT_ASSERT_SPINLOCK_AFFINITY(Lock_);

        auto& partSession = GetOrCrash(PartSessions_, partIndex);
        auto& replicaSession = partSession.ReplicaSessions[replicaIndex];

        replicaSession.GotMetaResponse = true;

        if (!metaOrError.IsOK()) {
            YT_LOG_WARNING(metaOrError, "Error requesting replica meta (PartIndex: %v, ReplicaIndex: %v)",
                partIndex,
                replicaIndex);
            return;
        }

        const auto& meta = metaOrError.Value();
        auto miscExt = GetProtoExtension<NChunkClient::NProto::TMiscExt>(meta->extensions());

        replicaSession.RowCount = miscExt.row_count();
        replicaSession.Sealed = miscExt.sealed();
        replicaSession.DataSize = miscExt.uncompressed_data_size();
        replicaSession.ReplicaReadRowCount = std::min(ReadRowCount_, replicaSession.RowCount - FirstRowIndex_);

        YT_LOG_DEBUG("Got replica meta "
            "(PartIndex: %v, ReplicaIndex: %v, RowCount: %v, DataSize: %v, ReplicaReadRowCount: %v, Sealed: %v)",
            partIndex,
            replicaIndex,
            replicaSession.RowCount,
            replicaSession.DataSize,
            replicaSession.ReplicaReadRowCount,
            replicaSession.Sealed);

        if (replicaSession.ReplicaReadRowCount <= 0) {
            YT_LOG_DEBUG("Replica has no relevant rows (PartIndex: %v, ReplicaIndex: %v)",
                partIndex,
                replicaIndex);
            return;
        }

        if (!replicaSession.Sealed) {
            YT_LOG_DEBUG("Replica has not been sealed yet (PartIndex: %v, ReplicaIndex: %v)",
                partIndex,
                replicaIndex);
            return;
        }
    }

    void ReadDataFromReplica(
        int partIndex,
        int replicaIndex)
    {
        YT_ASSERT_SPINLOCK_AFFINITY(Lock_);

        auto& partSession = GetOrCrash(PartSessions_, partIndex);
        auto& replicaSession = partSession.ReplicaSessions[replicaIndex];

        i64 estimatedReplicaReadSize = static_cast<i64>(
            replicaSession.ReplicaReadRowCount * replicaSession.DataSize / replicaSession.RowCount + 1);

        YT_LOG_DEBUG("Requesting data from replica "
            "(PartIndex: %v, ReplicaIndex: %v, FirstRowIndex: %v, ReadRowCount: %v, EstimatedReadSize: %v)",
            partIndex,
            replicaIndex,
            FirstRowIndex_,
            replicaSession.ReplicaReadRowCount,
            estimatedReplicaReadSize);

        YT_VERIFY(!std::exchange(partSession.HasPendingDataRequest, true));

        replicaSession.DataFuture = replicaSession.Reader->ReadBlocks(
            IChunkReader::TReadBlocksOptions{
                .ClientOptions = Options_,
                .EstimatedSize = estimatedReplicaReadSize,
            },
            FirstRowIndex_,
            replicaSession.ReplicaReadRowCount);

        replicaSession.DataFuture.Subscribe(
            BIND(&TReadRowsSession::OnGotDataFromReplica, MakeStrong(this), partIndex, replicaIndex)
                .Via(NRpc::TDispatcher::Get()->GetHeavyInvoker()));
    }

    void OnGotDataFromReplica(int partIndex, int replicaIndex, const TErrorOr<std::vector<TBlock>>& dataOrError)
    {
        auto guard = Guard(Lock_);

        if (Finished_) {
            return;
        }

        ProcceesReplicaDataResult(
            partIndex,
            replicaIndex,
            dataOrError);

        if (auto completedRowCount = ComputeCompletedRowCount()) {
            Complete(std::move(guard), *completedRowCount);
            return;
        }

        TryScheduleNextRead(std::move(guard), partIndex);
    }

    void ProcceesReplicaDataResult(int partIndex, int replicaIndex, const TErrorOr<std::vector<TBlock>>& dataOrError)
    {
        YT_ASSERT_SPINLOCK_AFFINITY(Lock_);

        auto& partSession = GetOrCrash(PartSessions_, partIndex);

        YT_VERIFY(std::exchange(partSession.HasPendingDataRequest, false));

        if (!dataOrError.IsOK()) {
            YT_LOG_WARNING(dataOrError, "Error requesting replica data (PartIndex: %v, ReplicaIndex: %v)",
                partIndex,
                replicaIndex);
            return;
        }

        auto data = dataOrError.Value();
        int readRowCount = data.size();

        YT_LOG_DEBUG("Got replica data (PartIndex: %v, ReplicaIndex: %v, ReadRowCount: %v)",
            partIndex,
            replicaIndex,
            readRowCount);

        YT_VERIFY(!partSession.Data);
        YT_VERIFY(readRowCount > 0);

        partSession.Data = std::move(data);
    }

    void OnPartSessionFinished(int partIndex, TGuard<NThreading::TSpinLock>&& guard)
    {
        YT_ASSERT_SPINLOCK_AFFINITY(Lock_);

        auto& partSession = GetOrCrash(PartSessions_, partIndex);
        partSession.Finished = true;

        if (auto completedRowCount = ComputeCompletedRowCount()) {
            Complete(std::move(guard), *completedRowCount);
            return;
        }

        auto finishedPartCount = std::ranges::count_if(
            PartSessions_,
            [] (const auto& it) { return it.second.Finished; });
        if (finishedPartCount == std::ssize(PartSessions_)) {
            Fail(std::move(guard));
        }
    }

    std::optional<int> ComputeCompletedRowCount() const
    {
        YT_ASSERT_SPINLOCK_AFFINITY(Lock_);

        if (CanComplete(ReadRowCount_)) {
            return ReadRowCount_;
        }

        // Try candidates in decreasing order.
        for (auto rowCount : GetCandidateReadRowCounts()) {
            if (CanComplete(rowCount)) {
                return rowCount;
            }
        }

        return std::nullopt;
    }

    std::vector<i64> GetCandidateReadRowCounts() const
    {
        YT_ASSERT_SPINLOCK_AFFINITY(Lock_);

        std::vector<i64> result;
        for (const auto& [_, partSession] : PartSessions_) {
            if (partSession.Data && !partSession.Data->empty()) {
                result.push_back(std::ssize(*partSession.Data));
            }
        }
        SortUnique(result, std::greater<>());
        return result;
    }

    std::vector<TSharedRef> GetDataFromReplica(int partIndex, i64 rowCount)
    {
        // NB: No need to hold the lock since the session is already finished.
        YT_VERIFY(Finished_);

        const auto& partSession = GetOrCrash(PartSessions_, partIndex);
        YT_VERIFY(partSession.Data && std::ssize(*partSession.Data) >= rowCount);

        std::vector<TSharedRef> result;
        result.reserve(rowCount);
        for (i64 rowIndex = 0; rowIndex < rowCount; ++rowIndex) {
            result.push_back((*partSession.Data)[rowIndex].Data);
        }
        return result;
    }

    void Complete(TGuard<NThreading::TSpinLock>&& guard, i64 rowCount)
    {
        YT_ASSERT_SPINLOCK_AFFINITY(Lock_);

        YT_VERIFY(!std::exchange(Finished_, true));

        YT_LOG_DEBUG("Erasure rows read session will complete (ReadRowCount: %v, RequestedRowCount: %v)",
            rowCount,
            ReadRowCount_);

        std::vector<std::vector<TSharedRef>> requestedRowLists;
        if (CanCompleteWithFastPath(rowCount)) {
            DoCancelFutures(std::move(guard));

            for (int partIndex : Reader_->PartIndices_) {
                requestedRowLists.push_back(GetDataFromReplica(partIndex, rowCount));
            }
        } else {
            auto availableIndices = GetAvailableIndices(rowCount);
            auto erasedIndices = GetErasedIndices(availableIndices);
            auto repairIndices = GetRepairIndices(erasedIndices);

            DoCancelFutures(std::move(guard));

            YT_LOG_DEBUG("Started repairing rows "
                "(AvailableIndices: %v, ErasedIndices: %v, RepairIndices: %v)",
                availableIndices,
                erasedIndices,
                repairIndices);

            std::vector<std::vector<TSharedRef>> repairRowLists(repairIndices.size());
            for (int index = 0; index < std::ssize(repairIndices); ++index) {
                repairRowLists[index] = GetDataFromReplica(repairIndices[index], rowCount);
            }

            auto erasedRowLists = RepairErasureJournalRows(
                Reader_->Codec_,
                erasedIndices,
                repairRowLists);

            for (int partIndex : Reader_->PartIndices_) {
                if (auto it = Find(erasedIndices, partIndex); it != erasedIndices.end()) {
                    requestedRowLists.push_back(std::move(erasedRowLists[std::distance(erasedIndices.begin(), it)]));
                } else {
                    requestedRowLists.push_back(GetDataFromReplica(partIndex, rowCount));
                }
            }
        }

        // Some final validation.
        for (const auto& rowList : requestedRowLists) {
            YT_VERIFY(std::ssize(rowList) == rowCount);
        }

        Promise_.Set(std::move(requestedRowLists));

        YT_LOG_DEBUG("Erasure rows read session completed");
    }

    void Fail(TGuard<NThreading::TSpinLock>&& guard)
    {
        YT_ASSERT_SPINLOCK_AFFINITY(Lock_);

        YT_VERIFY(!std::exchange(Finished_, true));

        auto availableIndices = GetAvailableIndices(/*desiredRowCount*/ 0);
        auto erasedIndices = GetErasedIndices(availableIndices);

        auto error = TError("Erasure journal chunk cannot be read")
            << TErrorAttribute("chunk_id", Reader_->ChunkId_)
            << TErrorAttribute("required_indices", Reader_->PartIndices_)
            << TErrorAttribute("erased_indices", erasedIndices)
            << TErrorAttribute("available_indices", availableIndices);

        DoCancelFutures(std::move(guard));

        YT_LOG_WARNING(error);
        Promise_.Set(error);
    }

    TPartIndexList GetAvailableIndices(i64 desiredRowCount) const
    {
        YT_ASSERT_SPINLOCK_AFFINITY(Lock_);

        NErasure::TPartIndexList result;
        for (const auto& [partIndex, partSession] : PartSessions_) {
            if (partSession.Data && std::ssize(*partSession.Data) >= desiredRowCount) {
                result.push_back(partIndex);
            }
        }
        SortUnique(result);
        return result;
    }

    TPartIndexList GetErasedIndices(const TPartIndexList& availableIndices) const
    {
        TPartIndexSet set;
        for (int index = 0; index < Reader_->Codec_->GetTotalPartCount(); ++index) {
            set.insert(index);
        }
        for (int index : availableIndices) {
            set.erase(index);
        }
        TPartIndexList list(set.begin(), set.end());
        Sort(list);
        return list;
    }

    TPartIndexList GetRepairIndices(const TPartIndexList& erasedIndices)
    {
        auto repairIndices = Reader_->Codec_->GetRepairIndices(erasedIndices);
        YT_VERIFY(repairIndices);
        return *repairIndices;
    }

    bool CanCompleteWithFastPath(i64 desiredRowCount) const
    {
        YT_ASSERT_SPINLOCK_AFFINITY(Lock_);

        for (int partIndex : Reader_->PartIndices_) {
            auto it = PartSessions_.find(partIndex);
            if (it == PartSessions_.end()) {
                return false;
            }
            const auto& partSession = it->second;
            if (!partSession.Data || std::ssize(*partSession.Data) < desiredRowCount) {
                return false;
            }
        }
        return true;
    }

    bool CanCompleteWithSlowPath(i64 desiredRowCount) const
    {
        auto availableIndices = GetAvailableIndices(desiredRowCount);
        auto erasedIndices = GetErasedIndices(availableIndices);
        return Reader_->Codec_->CanRepair(erasedIndices);
    }

    bool CanComplete(i64 desiredRowCount) const
    {
        YT_ASSERT_SPINLOCK_AFFINITY(Lock_);

        return CanCompleteWithFastPath(desiredRowCount) || CanCompleteWithSlowPath(desiredRowCount);
    }

    void OnSessionCanceled(const TError& error)
    {
        YT_LOG_DEBUG(error, "Erasure rows read session canceled");

        auto guard = Guard(Lock_);

        if (std::exchange(Finished_, true)) {
            return;
        }

        DoCancelFutures(std::move(guard));
    }

    void DoCancelFutures(TGuard<NThreading::TSpinLock>&& guard)
    {
        std::vector<TFuture<void>> futuresToCancel;
        for (const auto& [_, partSession] : PartSessions_) {
            for (const auto& replicaSession : partSession.ReplicaSessions) {
                if (replicaSession.MetaFuture) {
                    futuresToCancel.push_back(replicaSession.MetaFuture.AsVoid());
                }
                if (replicaSession.DataFuture) {
                    futuresToCancel.push_back(replicaSession.DataFuture.AsVoid());
                }
            }
        }

        guard.Release();

        auto error = TError("Erasure rows read session finished");
        for (const auto& future : futuresToCancel) {
            future.Cancel(error);
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

TErasurePartsReader::TErasurePartsReader(
    TChunkReaderConfigPtr config,
    NErasure::ICodec* codec,
    std::vector<IChunkReaderPtr> readers,
    const TPartIndexList& partIndices,
    NLogging::TLogger logger)
    : Config_(std::move(config))
    , Codec_(codec)
    , ChunkReaders_(std::move(readers))
    , PartIndices_(partIndices)
    , Logger(std::move(logger))
    , ChunkId_(ChunkReaders_.empty() ? TChunkId() : DecodeChunkId(ChunkReaders_[0]->GetChunkId()).Id)
{ }

TFuture<std::vector<std::vector<TSharedRef>>> TErasurePartsReader::ReadRows(
    const TClientChunkReadOptions& options,
    int firstRowIndex,
    int rowCount)
{
    return New<TReadRowsSession>(
        this,
        options,
        firstRowIndex,
        rowCount)
        ->Run();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJournalClient
