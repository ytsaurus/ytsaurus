#include "chunk_reader.h"
#include "erasure_parts_reader.h"
#include "helpers.h"
#include "config.h"

#include <yt/yt/ytlib/chunk_client/chunk_reader.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader_options.h>
#include <yt/yt/ytlib/chunk_client/chunk_replica_locator.h>
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
    const IClientPtr& client,
    const TNodeDirectoryPtr& nodeDirectory,
    TChunkId chunkId,
    TChunkReplicaWithMediumList replicas,
    IBlockCachePtr blockCache,
    IClientChunkMetaCachePtr chunkMetaCache,
    TTrafficMeterPtr trafficMeter,
    NConcurrency::IThroughputThrottlerPtr bandwidthThrottler,
    NConcurrency::IThroughputThrottlerPtr rpsThrottler)
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
        TChunkReplicaList partReplicas;
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
            client,
            nodeDirectory,
            /*localDescriptor*/ {},
            /*localNodeId*/ {},
            partChunkId,
            partReplicas,
            blockCache,
            chunkMetaCache,
            trafficMeter,
            /*nodeStatusDirectory*/ nullptr,
            bandwidthThrottler,
            rpsThrottler));
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

DECLARE_REFCOUNTED_CLASS(TErasureChunkReader)

class TErasureChunkReader
    : public IChunkReader
{
public:
    TErasureChunkReader(
        TChunkReaderConfigPtr config,
        IClientPtr client,
        TNodeDirectoryPtr nodeDirectory,
        TChunkId chunkId,
        NErasure::ICodec* codec,
        const TChunkReplicaList& replicas,
        IBlockCachePtr blockCache,
        IClientChunkMetaCachePtr chunkMetaCache,
        TTrafficMeterPtr trafficMeter,
        NConcurrency::IThroughputThrottlerPtr bandwidthThrottler,
        NConcurrency::IThroughputThrottlerPtr rpsThrottler)
        : Config_(std::move(config))
        , Client_(std::move(client))
        , NodeDirectory_(std::move(nodeDirectory))
        , ChunkId_(chunkId)
        , Codec_(codec)
        , BlockCache_(std::move(blockCache))
        , ChunkMetaCache_(std::move(chunkMetaCache))
        , TrafficMeter_(std::move(trafficMeter))
        , BandwidthThrottler_(std::move(bandwidthThrottler))
        , RpsThrottler_(std::move(rpsThrottler))
        , Logger(JournalClientLogger.WithTag("ChunkId: %v", ChunkId_))
        , ChunkReplicaLocator_(New<TChunkReplicaLocator>(
            Client_,
            NodeDirectory_,
            ChunkId_,
            Config_->SeedsTimeout,
            replicas,
            Logger))
    {
        YT_LOG_DEBUG("Erasure chunk reader created (ChunkId: %v, Codec: %v, InitialReplicas: %v)",
            ChunkId_,
            Codec_->GetId(),
            MakeFormattableView(replicas, TChunkReplicaAddressFormatter(NodeDirectory_)));
    }

    TFuture<std::vector<TBlock>> ReadBlocks(
        const TClientChunkReadOptions& /*options*/,
        const std::vector<int>& /*blockIndexes*/,
        std::optional<i64> /*estimatedSize*/) override
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
            , Logger(Reader_->Logger.WithTag("SessionId: %v", TGuid::Create()))
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

            ReplicasFuture_ = Reader_->ChunkReplicaLocator_->GetReplicasFuture();
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

            const auto& replicas = replicasOrError.Value();
            auto partsReader = New<TErasurePartsReader>(
                Reader_->Config_,
                Reader_->Codec_,
                CreatePartReaders(
                    Reader_->Config_,
                    Reader_->Client_,
                    Reader_->NodeDirectory_,
                    DecodeChunkId(Reader_->ChunkId_).Id,
                    replicas.Replicas,
                    Reader_->BlockCache_,
                    Reader_->ChunkMetaCache_,
                    Reader_->TrafficMeter_,
                    Reader_->BandwidthThrottler_,
                    Reader_->RpsThrottler_),
                GetPartIndexesToRead(Reader_->ChunkId_, Reader_->Codec_),
                Logger);

            partsReader->ReadRows(Options_, FirstBlockIndex_, BlockCount_)
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
                Reader_->ChunkReplicaLocator_->DiscardReplicas(ReplicasFuture_);
            }

            DoRetry();
        }

        void OnSessionFailed(const TError& error)
        {
            Promise_.Set(error << InnerErrors_);
        }
    };

    TFuture<std::vector<TBlock>> ReadBlocks(
        const TClientChunkReadOptions& options,
        int firstBlockIndex,
        int blockCount,
        std::optional<i64> /*estimatedSize*/) override
    {
        return New<TReadBlocksSession>(this, options, firstBlockIndex, blockCount)
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
    const IClientPtr Client_;
    const TNodeDirectoryPtr NodeDirectory_;
    const TChunkId ChunkId_;
    NErasure::ICodec* const Codec_;
    const IBlockCachePtr BlockCache_;
    const IClientChunkMetaCachePtr ChunkMetaCache_;
    const TTrafficMeterPtr TrafficMeter_;
    const NConcurrency::IThroughputThrottlerPtr BandwidthThrottler_;
    const NConcurrency::IThroughputThrottlerPtr RpsThrottler_;

    const NLogging::TLogger Logger;
    const TChunkReplicaLocatorPtr ChunkReplicaLocator_;
};

DEFINE_REFCOUNTED_TYPE(TErasureChunkReader)

////////////////////////////////////////////////////////////////////////////////

IChunkReaderPtr CreateChunkReader(
    TChunkReaderConfigPtr config,
    IClientPtr client,
    TNodeDirectoryPtr nodeDirectory,
    TChunkId chunkId,
    NErasure::ECodec codecId,
    const TChunkReplicaList& replicas,
    IBlockCachePtr blockCache,
    IClientChunkMetaCachePtr chunkMetaCache,
    TTrafficMeterPtr trafficMeter,
    NConcurrency::IThroughputThrottlerPtr bandwidthThrottler,
    NConcurrency::IThroughputThrottlerPtr rpsThrottler)
{
    if (codecId == NErasure::ECodec::None) {
        return CreateReplicationReader(
            config,
            New<TRemoteReaderOptions>(),
            std::move(client),
            std::move(nodeDirectory),
            /*localDescriptor*/ {},
            /*partitionTag*/ {},
            chunkId,
            replicas,
            std::move(blockCache),
            std::move(chunkMetaCache),
            std::move(trafficMeter),
            /*nodeStatusDirectory*/ nullptr,
            std::move(bandwidthThrottler),
            std::move(rpsThrottler));
    } else {
        return New<TErasureChunkReader>(
            config,
            std::move(client),
            std::move(nodeDirectory),
            chunkId,
            NErasure::GetCodec(codecId),
            replicas,
            std::move(blockCache),
            std::move(chunkMetaCache),
            std::move(trafficMeter),
            std::move(bandwidthThrottler),
            std::move(rpsThrottler));
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJournalClient

