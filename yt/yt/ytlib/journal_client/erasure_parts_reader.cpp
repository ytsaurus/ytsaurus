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

#include <yt/yt/core/misc/small_vector.h>
#include <yt/yt/core/misc/small_set.h>

#include <yt/yt/core/rpc/dispatcher.h>

#include <util/generic/algorithm.h>

namespace NYT::NJournalClient {

using namespace NChunkClient;
using namespace NNodeTrackerClient;
using namespace NApi::NNative;
using namespace NYT::NErasure;
using namespace ::NErasure;

using TPartIndexSet = SmallSet<int, ::NErasure::MaxTotalPartCount>;

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
        , Logger(Reader_->Logger)
    { }

    TFuture<std::vector<std::vector<TSharedRef>>> Run()
    {
        if (ReadRowCount_ <= 0) {
            std::vector<std::vector<TSharedRef>> result(Reader_->PartIndices_.size());
            return MakeFuture(result);
        }

        YT_LOG_DEBUG("Erasure rows read session started (FirstRowIndex: %v, RowCount: %v)",
            FirstRowIndex_,
            ReadRowCount_);

        std::vector<TFuture<TRefCountedChunkMetaPtr>> metaFutures;
        for (const auto& reader : Reader_->ChunkReaders_) {
            metaFutures.push_back(reader->GetMeta(Options_));
        }

        return AllSet(std::move(metaFutures))
            .Apply(BIND(&TReadRowsSession::OnGotReplicaMetas, MakeStrong(this)));
    }

private:
    const TErasurePartsReaderPtr Reader_;
    const TClientChunkReadOptions Options_;
    const int FirstRowIndex_;
    const int ReadRowCount_;

    const NLogging::TLogger& Logger;

    struct TReplica
    {
        int SequenceIndex;
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

    TFuture<std::vector<std::vector<TSharedRef>>> OnGotReplicaMetas(const std::vector<TErrorOr<TRefCountedChunkMetaPtr>>& metaOrErrors)
    {
        YT_LOG_DEBUG("Chunk metas received (Metas: {%v})",
            MakeFormattableView(metaOrErrors, [&] (auto* builder, const auto& metaOrError) {
                const auto& reader = Reader_->ChunkReaders_[std::distance(metaOrErrors.data(), &metaOrError)];
                builder->AppendFormat("%v => ",
                    DecodeChunkId(reader->GetChunkId()).ReplicaIndex);
                if (metaOrError.IsOK()) {
                    const auto& meta = metaOrError.Value();
                    auto miscExt = GetProtoExtension<NChunkClient::NProto::TMiscExt>(meta->extensions());
                    builder->AppendFormat("{RowCount: %v}",
                        miscExt.row_count());
                } else {
                    builder->AppendFormat("<error>");
                }
            }));

        YT_VERIFY(Reader_->ChunkReaders_.size() == metaOrErrors.size());
        Replicas_.reserve(metaOrErrors.size());
        for (int index = 0; index < static_cast<int>(metaOrErrors.size()); ++index) {
            const auto& metaOrError = metaOrErrors[index];
            auto& replica = Replicas_.emplace_back();
            replica.SequenceIndex = index;
            replica.ChunkReader = Reader_->ChunkReaders_[index];
            replica.PartIndex = DecodeChunkId(replica.ChunkReader->GetChunkId()).ReplicaIndex;
            IndexToReplica_[replica.PartIndex] = &replica;
            if (metaOrError.IsOK()) {
                const auto& meta = metaOrError.Value();
                auto miscExt = GetProtoExtension<NChunkClient::NProto::TMiscExt>(meta->extensions());
                replica.RowCount = miscExt.row_count();
                replica.DataSize = miscExt.uncompressed_data_size();
                replica.Available = true;
            } else {
                YT_LOG_DEBUG(metaOrError, "Replica dropped: chunk meta cannot be obtained (Index: %v)",
                    replica.PartIndex);
            }
        }

        for (auto& replica : Replicas_) {
            i64 relevantRowCount = replica.RowCount - FirstRowIndex_;
            if (relevantRowCount <= 0) {
                YT_LOG_DEBUG("Replica dropped: no relevant rows present (Index: %v, ReplicaRowCount: %v, SessionFirstRowIndex: %v)",
                    replica.PartIndex,
                    replica.RowCount,
                    FirstRowIndex_);
                replica.Available = false;
                continue;
            }

            i64 relevantDataSize = static_cast<i64>(replica.DataSize * relevantRowCount / replica.RowCount);
            if (replica.RowCount < FirstRowIndex_ + ReadRowCount_ && relevantDataSize < Reader_->Config_->ReplicaDataSizeReadThreshold) {
                YT_LOG_DEBUG("Replica dropped: too few relevant data (Index: %v, RelevantDataSize: %v)",
                    replica.PartIndex,
                    relevantDataSize);
                replica.Available = false;
                continue;
            }

            MinAvailableRowCount_ = std::min(MinAvailableRowCount_, replica.RowCount);
        }

        AdjustedReadRowCount_ = std::min(ReadRowCount_, MinAvailableRowCount_ - FirstRowIndex_);

        YT_LOG_DEBUG("Available replicas determined (MinAvailableRowCount: %v, AdjustedReadRowCount: %v)",
            MinAvailableRowCount_,
            AdjustedReadRowCount_);

        return IsFastPath() ? DoRunFastPath() : DoRunSlowPath();
    }

    bool IsFastPath()
    {
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

    TFuture<std::vector<std::vector<TSharedRef>>> DoRunFastPath()
    {
        YT_LOG_DEBUG("Session will run fast path");

        std::vector<TFuture<std::vector<TSharedRef>>> futures;
        for (int partIndex : Reader_->PartIndices_) {
            const auto* replica = GetOrCrash(IndexToReplica_, partIndex);
            YT_VERIFY(replica->Available);
            futures.push_back(RequestRowsFromReplica(*replica));
        }

        return AllSucceeded(futures)
            .Apply(BIND([=, this_ = MakeStrong(this)] (const std::vector<std::vector<TSharedRef>>& requestedRowLists) {
                YT_LOG_DEBUG("All fast path data received");
                return requestedRowLists;
            }));
    }

    TPartIndexList GetAvailableIndices()
    {
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

    TPartIndexList GetRepairIndices(const TPartIndexList& availableIndices, const TPartIndexList& erasedIndices)
    {
        auto throwError = [&] {
            THROW_ERROR_EXCEPTION("Erasure journal chunk %v cannot be read: codec is unable to perform repair from given replicas",
                Reader_->ChunkId_)
                << TErrorAttribute("needed_row_count", FirstRowIndex_ + ReadRowCount_)
                << TErrorAttribute("min_available_row_count", MinAvailableRowCount_)
                << TErrorAttribute("erased_indices", erasedIndices)
                << TErrorAttribute("available_indices", availableIndices);
        };

        auto optionalRepairIndices = Reader_->Codec_->GetRepairIndices(erasedIndices);
        if (!optionalRepairIndices) {
            throwError();
        }
        for (int index : *optionalRepairIndices) {
            if (Find(availableIndices, index) == availableIndices.end()) {
                throwError();
            }
        }
        return *optionalRepairIndices;
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
        auto availableIndicies = GetAvailableIndices();
        auto erasedIndices = GetErasedIndices(availableIndicies);
        auto repairIndices = GetRepairIndices(availableIndicies, erasedIndices);
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
            .Apply(BIND([=, this_ = MakeStrong(this)] (std::vector<std::vector<TSharedRef>> fetchedRowLists) {
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
        int partIndex = replica.PartIndex;
        i64 estimatedSize = static_cast<i64>(AdjustedReadRowCount_ * replica.DataSize / replica.RowCount);
        YT_LOG_DEBUG("Requesting rows from replica (PartIndex: %v, EstimatedSize: %v)",
            partIndex,
            estimatedSize);
        return replica.ChunkReader->ReadBlocks(Options_, FirstRowIndex_, AdjustedReadRowCount_, estimatedSize)
            .Apply(BIND([=, this_ = MakeStrong(this)] (const TErrorOr<std::vector<TBlock>>& blocksOrError) {
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
    return New<TReadRowsSession>(this, options, firstRowIndex, rowCount)
        ->Run();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJournalClient

