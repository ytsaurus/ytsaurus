#include "chunk_reader.h"
#include "erasure_parts_reader.h"
#include "helpers.h"
#include "config.h"

#include <yt/ytlib/chunk_client/chunk_reader.h>
#include <yt/ytlib/chunk_client/replication_reader.h>
#include <yt/ytlib/chunk_client/chunk_replica_locator.h>
#include <yt/ytlib/chunk_client/config.h>

#include <yt/ytlib/api/native/client.h>
#include <yt/ytlib/api/native/connection.h>

#include <yt/ytlib/chunk_client/erasure_helpers.h>
#include <yt/ytlib/chunk_client/dispatcher.h>

#include <yt/client/object_client/helpers.h>

#include <yt/library/erasure/codec.h>

#include <yt/core/rpc/dispatcher.h>

#include <yt/core/concurrency/delayed_executor.h>

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
    TChunkReplicaList replicas,
    IBlockCachePtr blockCache,
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
    for (int replicaIndex = 0; replicaIndex < replicas.back().GetReplicaIndex(); ++replicaIndex) {
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
            /* localDescriptor */ {},
            /* localNodeId */ {},
            partChunkId,
            partReplicas,
            blockCache,
            trafficMeter,
            bandwidthThrottler,
            rpsThrottler));
    }
    return partReaders;
}

NErasure::TPartIndexList GetPartIndexesToRead(TChunkId chunkId, NErasure::ECodec codecId)
{
    if (TypeFromId(chunkId) == EObjectType::ErasureJournalChunk) {
        auto* codec = NErasure::GetCodec(codecId);
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
        NErasure::ECodec codecId,
        const TChunkReplicaList& replicas,
        IBlockCachePtr blockCache,
        TTrafficMeterPtr trafficMeter,
        NConcurrency::IThroughputThrottlerPtr bandwidthThrottler,
        NConcurrency::IThroughputThrottlerPtr rpsThrottler)
        : Config_(std::move(config))
        , Client_(std::move(client))
        , NodeDirectory_(std::move(nodeDirectory))
        , ChunkId_(chunkId)
        , CodecId_(codecId)
        , BlockCache_(std::move(blockCache))
        , TrafficMeter_(std::move(trafficMeter))
        , BandwidthThrottler_(std::move(bandwidthThrottler))
        , RpsThrottler_(std::move(rpsThrottler))
        , Logger(NLogging::TLogger(JournalClientLogger)
            .AddTag("ChunkId: %v", ChunkId_))
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
            CodecId_,
            MakeFormattableView(replicas, TChunkReplicaAddressFormatter(NodeDirectory_)));
    }

    virtual TFuture<std::vector<TBlock>> ReadBlocks(
        const TClientBlockReadOptions& /*options*/,
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
            const TClientBlockReadOptions& options,
            int firstBlockIndex,
            int blockCount)
            : Reader_(std::move(reader))
            , Options_(options)
            , FirstBlockIndex_(firstBlockIndex)
            , BlockCount_(blockCount)
            , Logger(NLogging::TLogger(Reader_->Logger)
                .AddTag("SessionId: %v", TGuid::Create()))
        {
            DoRetry();
        }

        TFuture<std::vector<TBlock>> Run()
        {
            return Promise_;
        }

    private:
        const TErasureChunkReaderPtr Reader_;
        const TClientBlockReadOptions Options_;
        const int FirstBlockIndex_;
        const int BlockCount_;

        const NLogging::TLogger Logger;
        const TPromise<std::vector<TBlock>> Promise_ = NewPromise<std::vector<TBlock>>();

        int RetryIndex_ = 0;
        std::vector<TError> InnerErrors_;
        TFuture<TChunkReplicaList> ChunkReplicasFuture_;

        void DoRetry()
        {
            ++RetryIndex_;
            YT_LOG_DEBUG("Retry started (RetryIndex: %v/%v)",
                RetryIndex_,
                Reader_->Config_->RetryCount);

            ChunkReplicasFuture_ = Reader_->ChunkReplicaLocator_->GetReplicas();
            ChunkReplicasFuture_.Subscribe(BIND(&TReadBlocksSession::OnGotReplicas, MakeStrong(this))
                .Via(NChunkClient::TDispatcher::Get()->GetReaderInvoker()));
        }

        void OnGotReplicas(const TErrorOr<TChunkReplicaList>& replicasOrError)
        {
            if (!replicasOrError.IsOK()) {
                OnSessionFailed(TError("Error fetching blocks for chunk %v: cannot obtain chunk replicas",
                    Reader_->ChunkId_));
                return;
            }

            const auto& replicas = replicasOrError.Value();
            auto partsReader = New<TErasurePartsReader>(
                Reader_->Config_,
                Reader_->CodecId_,
                CreatePartReaders(
                    Reader_->Config_,
                    Reader_->Client_,
                    Reader_->NodeDirectory_,
                    DecodeChunkId(Reader_->ChunkId_).Id,
                    replicas,
                    Reader_->BlockCache_,
                    Reader_->TrafficMeter_,
                    Reader_->BandwidthThrottler_,
                    Reader_->RpsThrottler_),
                GetPartIndexesToRead(Reader_->ChunkId_, Reader_->CodecId_),
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
                auto rows = DecodeErasureJournalRows(Reader_->CodecId_, rowLists);
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
                Reader_->ChunkReplicaLocator_->DiscardReplicas(ChunkReplicasFuture_);
            }

            DoRetry();
        }

        void OnSessionFailed(const TError& error)
        {
            Promise_.Set(error << InnerErrors_);
        }
    };

    virtual TFuture<std::vector<TBlock>> ReadBlocks(
        const TClientBlockReadOptions& options,
        int firstBlockIndex,
        int blockCount,
        std::optional<i64> /*estimatedSize*/) override
    {
        return New<TReadBlocksSession>(this, options, firstBlockIndex, blockCount)
            ->Run();
    }

    virtual TFuture<TRefCountedChunkMetaPtr> GetMeta(
        const TClientBlockReadOptions& /*options*/,
        std::optional<int> /*partitionTag*/,
        const std::optional<std::vector<int>>& /*extensionTags*/) override
    {
        YT_ABORT();        
    }

    virtual TFuture<TSharedRef> LookupRows(
        const TClientBlockReadOptions& /*options*/,
        const TSharedRange<NTableClient::TKey>& /*lookupKeys*/,
        NCypressClient::TObjectId /*tableId*/,
        NHydra::TRevision /*revision*/,
        const NTableClient::TTableSchema& /*tableSchema*/,
        std::optional<i64> /*estimatedSize*/,
        std::atomic<i64>* /*uncompressedDataSize*/,
        const NTableClient::TColumnFilter& /*columnFilter*/,
        NTableClient::TTimestamp /*timestamp*/,
        NCompression::ECodec /*codecId*/,
        bool /*produceAllVersions*/) override
    {
        YT_ABORT();
    }

    virtual bool IsLookupSupported() const override
    {
        return false;
    }

    virtual TChunkId GetChunkId() const override
    {
        return ChunkId_;
    }

    virtual bool IsValid() const override
    {
        return true;
    }

private:
    const TChunkReaderConfigPtr Config_;
    const IClientPtr Client_;
    const TNodeDirectoryPtr NodeDirectory_;
    const TChunkId ChunkId_;
    const NErasure::ECodec CodecId_;
    const IBlockCachePtr BlockCache_;
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
            /* localDescriptor */ {},
            /* partitionTag */ {},
            chunkId,
            replicas,
            std::move(blockCache),
            std::move(trafficMeter),
            std::move(bandwidthThrottler),
            std::move(rpsThrottler));
    } else {
        return New<TErasureChunkReader>(
            config,
            std::move(client),
            std::move(nodeDirectory),
            chunkId,
            codecId,
            replicas,
            std::move(blockCache),
            std::move(trafficMeter),
            std::move(bandwidthThrottler),
            std::move(rpsThrottler));
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJournalClient

