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
#include <library/cpp/yt/small_containers/compact_vector.h>

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
        int readRowCount,
        bool enableFastPath)
        : Reader_(std::move(reader))
        , Options_(options)
        , FirstRowIndex_(firstRowIndex)
        , ReadRowCount_(readRowCount)
        , EnableFastPath_(enableFastPath)
        , Logger(Reader_->Logger)
    { }

    TFuture<std::vector<std::vector<TSharedRef>>> Run()
    {
        if (ReadRowCount_ <= 0) {
            std::vector<std::vector<TSharedRef>> result(Reader_->PartIndices_.size());
            return MakeFuture(result);
        }

        YT_LOG_DEBUG("Erasure rows read session started (FirstRowIndex: %v, RowCount: %v, PartIndices: %v)",
            FirstRowIndex_,
            ReadRowCount_,
            Reader_->PartIndices_);

        const auto& chunkReaders = Reader_->ChunkReaders_;

        MetaFutures_.reserve(chunkReaders.size());
        Replicas_.reserve(chunkReaders.size());
        for (int index = 0; index < std::ssize(chunkReaders); ++index) {
            const auto& reader = chunkReaders[index];

            auto& replica = Replicas_.emplace_back();
            replica.ChunkReader = reader;
            replica.PartIndex = DecodeChunkId(reader->GetChunkId()).ReplicaIndex;
            YT_VERIFY(IndexToReplica_.emplace(replica.PartIndex, &replica).second);

            MetaFutures_.push_back(reader->GetMeta(Options_));
        }

        Promise_.OnCanceled(BIND(&TErasurePartsReader::TReadRowsSession::CancelMetaFutures, MakeWeak(this)));

        for (int index = 0; index < std::ssize(chunkReaders); ++index) {
            // NB: Future subscription might trigger immediate subscriber execution and OnGotReplicaMeta
            // requires all meta futures to be present, so we firstly create all meta futures and then subscribe to them.
            const auto& metaFuture = MetaFutures_[index];
            metaFuture
                .Subscribe(BIND(&TErasurePartsReader::TReadRowsSession::OnGotReplicaMeta, MakeStrong(this), index));
        }

        return Promise_.ToFuture();
    }

private:
    const TErasurePartsReaderPtr Reader_;
    const TClientChunkReadOptions Options_;
    const int FirstRowIndex_;
    const int ReadRowCount_;
    const bool EnableFastPath_;

    const NLogging::TLogger& Logger;

    const TPromise<std::vector<std::vector<TSharedRef>>> Promise_ = NewPromise<std::vector<std::vector<TSharedRef>>>();

    struct TReplica
    {
        int PartIndex;
        IChunkReaderPtr ChunkReader;
        int RowCount;
        i64 DataSize;
        bool Available = false;
    };
    std::vector<TReplica> Replicas_;
    std::unordered_map<int, TReplica*> IndexToReplica_;
    int MinAvailableRowCount_ = Max<int>();
    int AdjustedReadRowCount_ = -1;

    std::vector<TFuture<TRefCountedChunkMetaPtr>> MetaFutures_;
    int MetaResponseCount_ = 0;

    bool ReadStarted_ = false;
    bool SlowPathScheduled_ = false;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, ReplicasLock_);

    void OnGotReplicaMeta(int index, const TErrorOr<TRefCountedChunkMetaPtr>& metaOrError)
    {
        // NB: Meta future cancelation might happen with #ReplicasLock_ acquired, so this check is performed
        // before lock acquisition to avoid deadlock.
        if (metaOrError.FindMatching(NYT::EErrorCode::Canceled)) {
            return;
        }

        auto guard = Guard(ReplicasLock_);

        ++MetaResponseCount_;

        if (ReadStarted_) {
            YT_LOG_DEBUG("Replica dropped: read already started (Index: %v)",
                index);
            return;
        }

        YT_VERIFY(index >= 0);
        YT_VERIFY(index < std::ssize(Replicas_));
        auto& replica = Replicas_[index];

        if (metaOrError.IsOK()) {
            const auto& meta = metaOrError.Value();
            RegisterReplicaMeta(index, meta);
        } else {
            YT_LOG_DEBUG(metaOrError, "Replica dropped: chunk meta cannot be obtained (Index: %v)",
                replica.PartIndex);
        }

        // NB(gritukan): If only one part is required, it's possible
        // that fast path can be done since there is another replica with
        // the same part, but chunk repair is not possible.
        if (MetaResponseCount_ == std::ssize(MetaFutures_) && !CanRunFastPath() && !CanRunSlowPath()) {
            auto availableIndices = GetAvailableIndices();
            auto erasedIndices = GetErasedIndices(availableIndices);

            auto error = TError("Erasure journal chunk %v cannot be read: codec is unable to perform repair from given replicas",
                Reader_->ChunkId_)
                << TErrorAttribute("needed_row_count", FirstRowIndex_ + ReadRowCount_)
                << TErrorAttribute("min_available_row_count", MinAvailableRowCount_)
                << TErrorAttribute("required_indices", Reader_->PartIndices_)
                << TErrorAttribute("erased_indices", erasedIndices)
                << TErrorAttribute("available_indices", availableIndices);
            Promise_.Set(error);
        }
    }

    void RegisterReplicaMeta(int replicaIndex, const TRefCountedChunkMetaPtr& meta)
    {
        VERIFY_SPINLOCK_AFFINITY(ReplicasLock_);

        auto& replica = Replicas_[replicaIndex];

        auto miscExt = GetProtoExtension<NChunkClient::NProto::TMiscExt>(meta->extensions());
        replica.RowCount = miscExt.row_count();
        replica.DataSize = miscExt.uncompressed_data_size();

        YT_LOG_DEBUG("Got replica meta (Index: %v, RowCount: %v, DataSize: %v)",
            replicaIndex,
            replica.RowCount,
            replica.DataSize);

        i64 relevantRowCount = replica.RowCount - FirstRowIndex_;
        if (relevantRowCount <= 0) {
            YT_LOG_DEBUG("Replica dropped: no relevant rows present (Index: %v, ReplicaRowCount: %v, SessionFirstRowIndex: %v)",
                replica.PartIndex,
                replica.RowCount,
                FirstRowIndex_);
            return;
        }

        i64 relevantDataSize = static_cast<i64>(replica.DataSize * relevantRowCount / replica.RowCount);
        if (replica.RowCount < FirstRowIndex_ + ReadRowCount_ && relevantDataSize < Reader_->Config_->ReplicaDataSizeReadThreshold) {
            YT_LOG_DEBUG("Replica dropped: too few relevant data (Index: %v, RelevantDataSize: %v)",
                replica.PartIndex,
                relevantDataSize);
            return;
        }

        YT_LOG_DEBUG("Replica is available (Index: %v)", replicaIndex);

        replica.Available = true;

        MinAvailableRowCount_ = std::min(MinAvailableRowCount_, replica.RowCount);
        AdjustedReadRowCount_ = std::min(ReadRowCount_, MinAvailableRowCount_ - FirstRowIndex_);

        if (CanRunFastPath()) {
            MaybeStartRead();
        } else if (CanRunSlowPath()) {
            if (MetaResponseCount_ == std::ssize(MetaFutures_)) {
                // No hope to run fast path, so start slow path immediately.
                MaybeStartRead();
            } else if (!SlowPathScheduled_) {
                auto slowPathDelay = Reader_->Config_->SlowPathDelay;

                YT_LOG_DEBUG("Scheduling slow path execution (SlowPathDelay: %v)",
                    Reader_->Config_->SlowPathDelay);

                TDelayedExecutor::Submit(
                    BIND(&TErasurePartsReader::TReadRowsSession::OnSlowPathDelayExpired, MakeStrong(this)),
                    slowPathDelay);
                SlowPathScheduled_ = true;
            }
        }
    }

    void OnSlowPathDelayExpired()
    {
        YT_LOG_DEBUG("Slow path delay expired");

        auto guard = Guard(ReplicasLock_);
        MaybeStartRead();
    }

    void MaybeStartRead()
    {
        VERIFY_SPINLOCK_AFFINITY(ReplicasLock_);

        if (ReadStarted_) {
            return;
        }

        ReadStarted_ = true;

        for (const auto& metaFuture : MetaFutures_) {
            metaFuture.Cancel(TError(NYT::EErrorCode::Canceled, "Read started"));
        }

        YT_LOG_DEBUG("Available replicas determined (MinAvailableRowCount: %v, AdjustedReadRowCount: %v)",
            MinAvailableRowCount_,
            AdjustedReadRowCount_);

        if (CanRunFastPath()) {
            Promise_.SetFrom(DoRunFastPath());
        } else {
            YT_VERIFY(CanRunSlowPath());
            Promise_.SetFrom(DoRunSlowPath());
        }
    }

    bool CanRunFastPath()
    {
        VERIFY_SPINLOCK_AFFINITY(ReplicasLock_);

        if (!EnableFastPath_) {
            return false;
        }

        TPartIndexSet set;
        for (int index : Reader_->PartIndices_) {
            set.insert(index);
        }
        for (const auto& replica : Replicas_) {
            if (replica.Available) {
                set.erase(replica.PartIndex);
            }
        }
        return set.empty();
    }

    bool CanRunSlowPath()
    {
        VERIFY_SPINLOCK_AFFINITY(ReplicasLock_);

        auto availableIndices = GetAvailableIndices();
        auto erasedIndices = GetErasedIndices(availableIndices);
        return Reader_->Codec_->CanRepair(erasedIndices);
    }

    TFuture<std::vector<std::vector<TSharedRef>>> DoRunFastPath()
    {
        VERIFY_SPINLOCK_AFFINITY(ReplicasLock_);
        YT_VERIFY(CanRunFastPath());

        YT_LOG_DEBUG("Session will run fast path");

        std::vector<TFuture<std::vector<TSharedRef>>> futures;
        for (int partIndex : Reader_->PartIndices_) {
            const auto* replica = GetOrCrash(IndexToReplica_, partIndex);
            YT_VERIFY(replica->Available);
            futures.push_back(RequestRowsFromReplica(*replica));
        }

        return AllSucceeded(futures)
            .ApplyUnique(BIND([=, this, this_ = MakeStrong(this)] (std::vector<std::vector<TSharedRef>>&& requestedRowLists) {
                auto rowCount = Max<i64>();
                for (const auto& rowList : requestedRowLists) {
                    rowCount = std::min<i64>(rowCount, std::ssize(rowList));
                }
                for (auto& rowList : requestedRowLists) {
                    rowList.resize(rowCount);
                }

                YT_LOG_DEBUG("All fast path data received (RowCount: %v)",
                    rowCount);

                return requestedRowLists;
            }));
    }

    TPartIndexList GetAvailableIndices()
    {
        VERIFY_SPINLOCK_AFFINITY(ReplicasLock_);

        NErasure::TPartIndexList result;
        for (const auto& replica : Replicas_) {
            if (replica.Available) {
                result.push_back(replica.PartIndex);
            }
        }
        SortUnique(result);
        return result;
    }

    TPartIndexList GetErasedIndices(const TPartIndexList& availableIndicies)
    {
        TPartIndexSet set;
        for (int index = 0; index < Reader_->Codec_->GetTotalPartCount(); ++index) {
            set.insert(index);
        }
        for (int index : availableIndicies) {
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
        // Repair indicies must come first.
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

    TFuture<std::vector<std::vector<TSharedRef>>> DoRunSlowPath()
    {
        VERIFY_SPINLOCK_AFFINITY(ReplicasLock_);
        YT_VERIFY(CanRunSlowPath());

        auto availableIndicies = GetAvailableIndices();
        auto erasedIndices = GetErasedIndices(availableIndicies);
        auto repairIndices = GetRepairIndices(erasedIndices);
        auto fetchIndices = GetFetchIndices(erasedIndices, repairIndices);

        YT_LOG_DEBUG("Session will run slow path (AvailableIndices: %v, ErasedIndices: %v, RepairIndices: %v, FetchIndices: %v)",
            availableIndicies,
            erasedIndices,
            repairIndices,
            fetchIndices);

        std::vector<TFuture<std::vector<TSharedRef>>> futures;
        for (int partIndex : fetchIndices) {
            const auto* replica = GetOrCrash(IndexToReplica_, partIndex);
            futures.push_back(RequestRowsFromReplica(*replica));
        }

        return AllSucceeded(futures)
            .Apply(BIND([=, this, this_ = MakeStrong(this)] (std::vector<std::vector<TSharedRef>> fetchedRowLists) {
                i64 rowCount = Max<i64>();
                for (const auto& fetchedRowList : fetchedRowLists) {
                    rowCount = std::min<i64>(rowCount, fetchedRowList.size());
                }
                for (auto& fetchedRowList : fetchedRowLists) {
                    fetchedRowList.resize(rowCount);
                }

                std::vector<std::vector<TSharedRef>> repairRowLists;
                repairRowLists.reserve(repairIndices.size());
                for (int index = 0; index < std::ssize(repairIndices); ++index) {
                    YT_VERIFY(repairIndices[index] == fetchIndices[index]);
                    repairRowLists.push_back(fetchedRowLists[index]);
                }

                auto erasedRowLists = RepairErasureJournalRows(
                    Reader_->Codec_,
                    erasedIndices,
                    repairRowLists);

                std::vector<std::vector<TSharedRef>> requestedRowLists;
                for (int partIndex : Reader_->PartIndices_) {
                    auto tryFill = [&] (const auto& indices, const auto& rows) {
                        auto it = std::find(indices.begin(), indices.end(), partIndex);
                        if (it == indices.end()) {
                            return false;
                        }

                        const auto& rowList = rows[std::distance(indices.begin(), it)];
                        YT_VERIFY(std::ssize(rowList) == rowCount);
                        requestedRowLists.push_back(rowList);

                        return true;
                    };
                    YT_VERIFY(tryFill(fetchIndices, fetchedRowLists) || tryFill(erasedIndices, erasedRowLists));
                }

                YT_LOG_DEBUG("All slow path data repaired");

                return requestedRowLists;
            }).AsyncVia(NRpc::TDispatcher::Get()->GetHeavyInvoker()));
    }

    TFuture<std::vector<TSharedRef>> RequestRowsFromReplica(const TReplica& replica)
    {
        VERIFY_SPINLOCK_AFFINITY(ReplicasLock_);

        int partIndex = replica.PartIndex;
        i64 estimatedSize = static_cast<i64>(AdjustedReadRowCount_ * replica.DataSize / replica.RowCount);
        YT_LOG_DEBUG("Requesting rows from replica (PartIndex: %v, EstimatedSize: %v)",
            partIndex,
            estimatedSize);
        return replica.ChunkReader->ReadBlocks(
            IChunkReader::TReadBlocksOptions{
                .ClientOptions = Options_,
                .EstimatedSize = estimatedSize,
            },
            FirstRowIndex_,
            AdjustedReadRowCount_)
            .Apply(BIND([=, this, this_ = MakeStrong(this)] (const TErrorOr<std::vector<TBlock>>& blocksOrError) {
                if (!blocksOrError.IsOK()) {
                    YT_LOG_DEBUG(blocksOrError, "Error requesting rows from replica (PartIndex: %v)",
                        partIndex);
                    THROW_ERROR(blocksOrError);
                }

                const auto& blocks = blocksOrError.Value();
                std::vector<TSharedRef> rows;
                rows.reserve(blocks.size());
                for (const auto& block : blocks) {
                    rows.push_back(block.Data);
                }

                YT_LOG_DEBUG("Received rows from replica (PartIndex: %v, RowCount: %v)",
                    partIndex,
                    rows.size());

                return rows;
            }).AsyncVia(NRpc::TDispatcher::Get()->GetHeavyInvoker()));
    }

    void CancelMetaFutures(const TError& error)
    {
        for (const auto& metaFuture : MetaFutures_) {
            metaFuture.Cancel(error);
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
    int rowCount,
    bool enableFastPath)
{
    return New<TReadRowsSession>(
        this,
        options,
        firstRowIndex,
        rowCount,
        enableFastPath)
        ->Run();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJournalClient

