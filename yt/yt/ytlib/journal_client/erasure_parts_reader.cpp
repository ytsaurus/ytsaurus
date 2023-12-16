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

#include <library/cpp/yt/small_containers/compact_set.h>

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
        , Logger(Reader_->Logger.WithTag("SessionId: %v", TGuid::Create()))
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
        Replicas_.reserve(chunkReaders.size());
        for (const auto& reader : chunkReaders) {
            int replicaIndex = DecodeChunkId(reader->GetChunkId()).ReplicaIndex;
            EmplaceOrCrash(
                Replicas_,
                replicaIndex,
                TReplica{.Reader = reader,});
        }

        for (auto& [partIndex, replica] : Replicas_) {
            YT_LOG_DEBUG("Requesting replica meta (PartIndex: %v)",
                partIndex);
            ++OutstandingReplicaCount_;
            replica.MetaFuture = replica.Reader->GetMeta(Options_);
        }

        // NB: Only subscribe to futures once all data members are ready.
        for (const auto& [partIndex, replica] : Replicas_) {
            replica.MetaFuture.Subscribe(
                BIND(&TReadRowsSession::OnGotReplicaMeta, MakeStrong(this), partIndex)
                    .Via(NRpc::TDispatcher::Get()->GetHeavyInvoker()));
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

    int OutstandingReplicaCount_ = 0;
    bool Finished_ = false;

    struct TReplica
    {
        IChunkReaderPtr Reader;
        TFuture<TRefCountedChunkMetaPtr> MetaFuture;
        TFuture<std::vector<TBlock>> DataFuture;
        std::optional<std::vector<TBlock>> Data;
        int RowCount = 0;
        i64 DataSize = 0;
    };

    std::unordered_map<int, TReplica> Replicas_;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, Lock_);

    void OnGotReplicaMeta(int partIndex, const TErrorOr<TRefCountedChunkMetaPtr>& metaOrError)
    {
        auto guard = Guard(Lock_);

        if (Finished_) {
            return;
        }

        if (!metaOrError.IsOK()) {
            YT_LOG_WARNING(metaOrError, "Error requesting replica meta (PartIndex: %v)",
                partIndex);
            OnReplicaFinished(std::move(guard));
            return;
        }

        const auto& meta = metaOrError.Value();
        auto miscExt = GetProtoExtension<NChunkClient::NProto::TMiscExt>(meta->extensions());

        auto& replica = GetOrCrash(Replicas_, partIndex);
        replica.RowCount = miscExt.row_count();
        replica.DataSize = miscExt.uncompressed_data_size();

        YT_LOG_DEBUG("Got replica meta (PartIndex: %v, RowCount: %v, DataSize: %v)",
            partIndex,
            replica.RowCount,
            replica.DataSize);

        auto replicaReadRowCount = std::min(ReadRowCount_, replica.RowCount - FirstRowIndex_);
        if (replicaReadRowCount <= 0) {
            YT_LOG_DEBUG("Replica has no relevant rows (PartIndex: %v)",
                partIndex);
            OnReplicaFinished(std::move(guard));
            return;
        }

        i64 estimatedReplicaReadSize = static_cast<i64>(replicaReadRowCount * replica.DataSize / replica.RowCount + 1);
        YT_LOG_DEBUG("Requesting data from replica (PartIndex: %v, FirstRowIndex: %v, ReadRowCount: %v, EstimatedReadSize: %v)",
            partIndex,
            FirstRowIndex_,
            replicaReadRowCount,
            estimatedReplicaReadSize);
        replica.DataFuture = replica.Reader->ReadBlocks(
            IChunkReader::TReadBlocksOptions{
                .ClientOptions = Options_,
                .EstimatedSize = estimatedReplicaReadSize,
            },
            FirstRowIndex_,
            replicaReadRowCount);
        replica.DataFuture.SubscribeUnique(
            BIND(&TReadRowsSession::OnGotReplicaData, MakeStrong(this), partIndex)
                .Via(NRpc::TDispatcher::Get()->GetHeavyInvoker()));
    }

    void OnGotReplicaData(int partIndex, TErrorOr<std::vector<TBlock>>&& dataOrError)
    {
        auto guard = Guard(Lock_);

        if (Finished_) {
            return;
        }

        if (!dataOrError.IsOK()) {
            YT_LOG_WARNING(dataOrError, "Error requesting replica data (PartIndex: %v)",
                partIndex);
            OnReplicaFinished(std::move(guard));
            return;
        }

        const auto& data = dataOrError.Value();
        auto& replica = GetOrCrash(Replicas_, partIndex);

        YT_LOG_DEBUG("Got replica data (PartIndex: %v, ReadRowCount: %v)",
            partIndex,
            data.size());

        replica.Data = std::move(data);

        if (CanComplete(ReadRowCount_)) {
            YT_LOG_DEBUG("Erasure rows read session will complete with full read");
            Complete(std::move(guard), ReadRowCount_);
            return;
        }

        OnReplicaFinished(std::move(guard));
    }


    std::vector<i64> GetCandidateReadRowCounts()
    {
        VERIFY_SPINLOCK_AFFINITY(Lock_);

        std::vector<i64> result;
        for (const auto& [_, replica] : Replicas_) {
            if (replica.Data) {
                result.push_back(std::ssize(*replica.Data));
            }
        }
        SortUnique(result, std::greater<>());
        return result;
    }

    void OnReplicaFinished(TGuard<NThreading::TSpinLock>&& guard)
    {
        VERIFY_SPINLOCK_AFFINITY(Lock_);

        if (--OutstandingReplicaCount_ > 0) {
            return;
        }

        // Try candidates in decreasing order.
        for (auto rowCount : GetCandidateReadRowCounts()) {
            if (CanComplete(rowCount)) {
                YT_LOG_DEBUG("Erasure rows read session will complete with partial read (RowCount: %v)",
                    rowCount);
                Complete(std::move(guard), rowCount);
                return;
            }
        }

        Fail(std::move(guard));
    }


    std::vector<TSharedRef> GetDataFromReplica(int partIndex, i64 rowCount)
    {
        // NB: No need to hold the lock since the session is already finished.
        YT_VERIFY(Finished_);

        const auto& replica = GetOrCrash(Replicas_, partIndex);
        YT_VERIFY(replica.Data && std::ssize(*replica.Data) >= rowCount);

        std::vector<TSharedRef> result;
        result.reserve(rowCount);
        for (i64 rowIndex = 0; rowIndex < rowCount; ++rowIndex) {
            result.push_back((*replica.Data)[rowIndex].Data);
        }
        return result;
    }


    void Complete(TGuard<NThreading::TSpinLock>&& guard, i64 rowCount)
    {
        VERIFY_SPINLOCK_AFFINITY(Lock_);

        YT_VERIFY(!std::exchange(Finished_, true));

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
            auto fetchIndices = GetFetchIndices(erasedIndices, repairIndices);

            DoCancelFutures(std::move(guard));

            YT_LOG_DEBUG("Started repairing rows");

            std::vector<std::vector<TSharedRef>> repairRowLists(repairIndices.size());
            for (int index = 0; index < std::ssize(repairIndices); ++index) {
                YT_VERIFY(repairIndices[index] == fetchIndices[index]);
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
        VERIFY_SPINLOCK_AFFINITY(Lock_);

        YT_VERIFY(!std::exchange(Finished_, true));

        auto availableIndicies = GetAvailableIndices(/*desiredRowCount*/ 0);
        auto erasedIndices = GetErasedIndices(availableIndicies);

        auto error = TError("Erasure journal chunk cannot be read")
            << TErrorAttribute("chunk_id", Reader_->ChunkId_)
            << TErrorAttribute("required_indices", Reader_->PartIndices_)
            << TErrorAttribute("erased_indices", erasedIndices)
            << TErrorAttribute("available_indices", GetAvailableIndices(0));

        DoCancelFutures(std::move(guard));

        YT_LOG_WARNING(error);
        Promise_.Set(error);
    }


    TPartIndexList GetAvailableIndices(i64 desiredRowCount)
    {
        VERIFY_SPINLOCK_AFFINITY(Lock_);

        NErasure::TPartIndexList result;
        for (const auto& [partIndex, replica] : Replicas_) {
            if (replica.Data && std::ssize(*replica.Data) >= desiredRowCount) {
                result.push_back(partIndex);
            }
        }
        SortUnique(result);
        return result;
    }

    TPartIndexList GetErasedIndices(const TPartIndexList& availableIndices)
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

    TPartIndexList GetFetchIndices(const TPartIndexList& erasedIndices, const TPartIndexList& repairIndices)
    {
        TPartIndexList list;
        // Repair indices must come first.
        for (int index : repairIndices) {
            list.push_back(index);
        }
        // The rest is parts requested by the client but not needed for repair.
        for (int index : Reader_->PartIndices_) {
            if (Find(list, index) == list.end() && Find(erasedIndices, index) == erasedIndices.end()) {
                list.push_back(index);
            }
        }
        return list;
    }

    bool CanCompleteWithFastPath(i64 desiredRowCount)
    {
        VERIFY_SPINLOCK_AFFINITY(Lock_);

        for (int partIndex : Reader_->PartIndices_) {
            auto it = Replicas_.find(partIndex);
            if (it == Replicas_.end()) {
                return false;
            }
            const auto& replica = it->second;
            if (!replica.Data || std::ssize(*replica.Data) < desiredRowCount) {
                return false;
            }
        }
        return true;
    }

    bool CanCompleteWithSlowPath(i64 desiredRowCount)
    {
        auto availableIndices = GetAvailableIndices(desiredRowCount);
        auto erasedIndices = GetErasedIndices(availableIndices);
        return Reader_->Codec_->CanRepair(erasedIndices);
    }

    bool CanComplete(i64 desiredRowCount)
    {
        VERIFY_SPINLOCK_AFFINITY(Lock_);

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
        for (const auto& [_, replica] : Replicas_) {
            if (replica.MetaFuture) {
                futuresToCancel.push_back(replica.MetaFuture.AsVoid());
            }
            if (replica.DataFuture) {
                futuresToCancel.push_back(replica.DataFuture.AsVoid());
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

