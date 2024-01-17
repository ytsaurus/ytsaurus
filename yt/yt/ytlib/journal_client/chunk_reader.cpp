#include "chunk_reader.h"
#include "erasure_parts_reader.h"
#include "helpers.h"
#include "config.h"

#include <yt/yt/ytlib/chunk_client/chunk_reader.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader_host.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader_options.h>
#include <yt/yt/ytlib/chunk_client/chunk_replica_cache.h>
#include <yt/yt/ytlib/chunk_client/config.h>
#include <yt/yt/ytlib/chunk_client/helpers.h>
#include <yt/yt/ytlib/chunk_client/replication_reader.h>

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/ytlib/chunk_client/erasure_helpers.h>
#include <yt/yt/ytlib/chunk_client/dispatcher.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/library/erasure/impl/codec.h>

#include <yt/yt/core/rpc/dispatcher.h>

#include <yt/yt/core/concurrency/delayed_executor.h>

#include <yt/yt/core/misc/atomic_object.h>

#include <util/generic/algorithm.h>

namespace NYT::NJournalClient {

using namespace NChunkClient;
using namespace NObjectClient;
using namespace NConcurrency;
using namespace NNodeTrackerClient;
using namespace NApi::NNative;

////////////////////////////////////////////////////////////////////////////////

namespace {

std::vector<IChunkReaderPtr> CreatePartReaders(
    const TChunkReaderConfigPtr& config,
    const TChunkReaderHostPtr& chunkReaderHost,
    TChunkId chunkId,
    TChunkReplicaWithMediumList replicas)
{
    if (replicas.empty()) {
        return {};
    }

    SortBy(replicas, [] (auto replica) { return replica.GetReplicaIndex(); });

    auto options = New<TRemoteReaderOptions>();
    options->AllowFetchingSeedsFromMaster = false;

    std::vector<IChunkReaderPtr> partReaders;
    for (int replicaIndex = 0; replicaIndex < ChunkReplicaIndexBound; ++replicaIndex) {
        auto partChunkId = EncodeChunkId(TChunkIdWithIndex(chunkId, replicaIndex));
        TChunkReplicaWithMediumList partReplicas;
        for (auto replica : replicas) {
            if (replica.GetReplicaIndex() == replicaIndex) {
                partReplicas.push_back(replica);
            }
        }
        if (partReplicas.empty()) {
            continue;
        }
        partReaders.push_back(CreateReplicationReader(
            config,
            options,
            chunkReaderHost,
            partChunkId,
            partReplicas));
    }
    return partReaders;
}

NErasure::TPartIndexList GetPartIndexesToRead(TChunkId chunkId, NErasure::ICodec* codec)
{
    if (TypeFromId(chunkId) == EObjectType::ErasureJournalChunk) {
        int dataPartCount = codec->GetDataPartCount();
        NErasure::TPartIndexList result;
        result.resize(dataPartCount);
        std::iota(result.begin(), result.end(), 0);
        return result;
    } else {
        YT_VERIFY(IsErasureChunkPartId(chunkId));
        return {DecodeChunkId(chunkId).ReplicaIndex};
    }
}

std::vector<TBlock> RowsToBlocks(const std::vector<TSharedRef>& rows)
{
    std::vector<TBlock> blocks;
    blocks.reserve(rows.size());
    for (auto& row : rows) {
        blocks.emplace_back(std::move(row));
    }
    return blocks;
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TErasureChunkReader)

class TErasureChunkReader
    : public IChunkReader
{
public:
    TErasureChunkReader(
        TChunkReaderConfigPtr config,
        TChunkReaderHostPtr chunkReaderHost,
        TChunkId chunkId,
        NErasure::ICodec* codec,
        const TChunkReplicaWithMediumList& replicas)
        : Config_(std::move(config))
        , ChunkReaderHost_(std::move(chunkReaderHost))
        , Client_(ChunkReaderHost_->Client)
        , ChunkId_(chunkId)
        , Codec_(codec)
        , Logger(JournalClientLogger.WithTag("ChunkId: %v", ChunkId_))
        , InitialReplicas_(replicas)
    {
        const auto& nodeDirectory = Client_->GetNativeConnection()->GetNodeDirectory();
        YT_LOG_DEBUG("Erasure chunk reader created (ChunkId: %v, Codec: %v, InitialReplicas: %v)",
            ChunkId_,
            Codec_->GetId(),
            MakeFormattableView(replicas, TChunkReplicaAddressFormatter(nodeDirectory)));
    }

    TFuture<std::vector<TBlock>> ReadBlocks(
        const TReadBlocksOptions& /*options*/,
        const std::vector<int>& /*blockIndexes*/) override
    {
        YT_ABORT();
    }

    class TReadBlocksSession
        : public TRefCounted
    {
    public:
        TReadBlocksSession(
            TErasureChunkReaderPtr reader,
            const TClientChunkReadOptions& options,
            int firstBlockIndex,
            int blockCount)
            : Reader_(std::move(reader))
            , Options_(options)
            , FirstBlockIndex_(firstBlockIndex)
            , BlockCount_(blockCount)
            , InitialReplicas_(Reader_->InitialReplicas_.Load())
            , Logger(Reader_->Logger.WithTag("ReadSessionId: %v, ReadBlocksSessionId: %v", Options_.ReadSessionId, TGuid::Create()))
        {
            DoRetry();
        }

        TFuture<std::vector<TBlock>> Run()
        {
            return Promise_;
        }

    private:
        const TErasureChunkReaderPtr Reader_;
        const TClientChunkReadOptions Options_;
        const int FirstBlockIndex_;
        const int BlockCount_;
        const TChunkReplicaWithMediumList InitialReplicas_;

        const NLogging::TLogger Logger;
        const TPromise<std::vector<TBlock>> Promise_ = NewPromise<std::vector<TBlock>>();

        int RetryIndex_ = 0;
        std::vector<TError> InnerErrors_;
        TFuture<TAllyReplicasInfo> ReplicasFuture_;


        void DoRetry()
        {
            ++RetryIndex_;
            YT_LOG_DEBUG("Retry started (RetryIndex: %v/%v)",
                RetryIndex_,
                Reader_->Config_->RetryCount);

            if (!InitialReplicas_.empty()) {
                ReplicasFuture_ = MakeFuture(TAllyReplicasInfo::FromChunkReplicas(InitialReplicas_));
            } else {
                const auto& chunkReplicaCache = Reader_->Client_->GetNativeConnection()->GetChunkReplicaCache();
                auto futures = chunkReplicaCache->GetReplicas({DecodeChunkId(Reader_->ChunkId_).Id});
                YT_VERIFY(futures.size() == 1);
                ReplicasFuture_ = std::move(futures[0]);
            }

            ReplicasFuture_.Subscribe(BIND(&TReadBlocksSession::OnGotReplicas, MakeStrong(this))
                .Via(NChunkClient::TDispatcher::Get()->GetReaderInvoker()));
        }

        void OnGotReplicas(const TErrorOr<TAllyReplicasInfo>& replicasOrError)
        {
            if (!replicasOrError.IsOK()) {
                OnSessionFailed(TError("Error fetching blocks for chunk %v: cannot obtain chunk replicas",
                    Reader_->ChunkId_));
                return;
            }

            // NB: These replicas may correspond to replicas of whole erasure chunk even if Reader_->ChunkId_
            // is an erasure chunk part. That is fine because CreatePartReaders performs proper filtering.
            const auto& replicas = replicasOrError.Value();

            auto partsReader = New<TErasurePartsReader>(
                Reader_->Config_,
                Reader_->Codec_,
                CreatePartReaders(
                    Reader_->Config_,
                    Reader_->ChunkReaderHost_,
                    DecodeChunkId(Reader_->ChunkId_).Id,
                    replicas.Replicas),
                GetPartIndexesToRead(Reader_->ChunkId_, Reader_->Codec_),
                Logger);

            partsReader->ReadRows(
                Options_,
                FirstBlockIndex_,
                BlockCount_)
                .Subscribe(BIND(&TReadBlocksSession::OnRowsRead, MakeStrong(this))
                    .Via(NChunkClient::TDispatcher::Get()->GetReaderInvoker()));
        }

        void OnRowsRead(const TErrorOr<std::vector<std::vector<TSharedRef>>>& rowListsOrError)
        {
            if (!rowListsOrError.IsOK()) {
                OnRetryFailed(rowListsOrError);
                return;
            }

            const auto& rowLists = rowListsOrError.Value();
            if (TypeFromId(Reader_->ChunkId_) == EObjectType::ErasureJournalChunk) {
                auto rows = DecodeErasureJournalRows(Reader_->Codec_, rowLists);
                Promise_.Set(RowsToBlocks(rows));
            } else {
                YT_VERIFY(rowLists.size() == 1);
                const auto& rows = rowLists[0];
                Promise_.Set(RowsToBlocks(rows));
            }
        }

        void OnRetryFailed(const TError& error)
        {
            YT_LOG_DEBUG(error, "Retry failed (RetryIndex: %v/%v)",
                RetryIndex_,
                Reader_->Config_->RetryCount);

            InnerErrors_.push_back(error);

            if (RetryIndex_ >= Reader_->Config_->RetryCount) {
                OnSessionFailed(TError("Error fetching blocks for chunk %v: out of retries",
                    Reader_->ChunkId_));
                return;
            }

            if (error.FindMatching(NChunkClient::EErrorCode::NoSuchChunk)) {
                Reader_->InitialReplicas_.Store(TChunkReplicaWithMediumList{});
                Reader_->Client_->GetNativeConnection()->GetChunkReplicaCache()->DiscardReplicas(
                    DecodeChunkId(Reader_->ChunkId_).Id,
                    ReplicasFuture_);
            }

            DoRetry();
        }

        void OnSessionFailed(const TError& error)
        {
            Promise_.Set(error << InnerErrors_);
        }
    };

    TFuture<std::vector<TBlock>> ReadBlocks(
        const TReadBlocksOptions& options,
        int firstBlockIndex,
        int blockCount) override
    {
        return New<TReadBlocksSession>(this, options.ClientOptions, firstBlockIndex, blockCount)
            ->Run();
    }

    TFuture<TRefCountedChunkMetaPtr> GetMeta(
        const TClientChunkReadOptions& /*options*/,
        std::optional<int> /*partitionTag*/,
        const std::optional<std::vector<int>>& /*extensionTags*/) override
    {
        YT_ABORT();
    }

    TChunkId GetChunkId() const override
    {
        return ChunkId_;
    }

    TInstant GetLastFailureTime() const override
    {
        return TInstant();
    }

private:
    const TChunkReaderConfigPtr Config_;
    const TChunkReaderHostPtr ChunkReaderHost_;
    const IClientPtr Client_;
    const TChunkId ChunkId_;
    NErasure::ICodec* const Codec_;

    const NLogging::TLogger Logger;

    TAtomicObject<TChunkReplicaWithMediumList> InitialReplicas_;
};

DEFINE_REFCOUNTED_TYPE(TErasureChunkReader)

////////////////////////////////////////////////////////////////////////////////

IChunkReaderPtr CreateChunkReader(
    TChunkReaderConfigPtr config,
    TChunkReaderHostPtr chunkReaderHost,
    TChunkId chunkId,
    NErasure::ECodec codecId,
    const TChunkReplicaWithMediumList& replicas)
{
    if (codecId == NErasure::ECodec::None) {
        return CreateReplicationReader(
            config,
            New<TRemoteReaderOptions>(),
            std::move(chunkReaderHost),
            chunkId,
            replicas);
    } else {
        return New<TErasureChunkReader>(
            config,
            std::move(chunkReaderHost),
            chunkId,
            NErasure::GetCodec(codecId),
            replicas);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJournalClient

