#include "private.h"
#include "config.h"
#include "erasure_reader.h"
#include "erasure_repair.h"
#include "erasure_helpers.h"
#include "erasure_adaptive_repair.h"
#include "dispatcher.h"
#include "chunk_reader_options.h"
#include "chunk_reader_statistics.h"
#include "replication_reader.h"

#include <yt/yt/client/misc/workload.h>

#include <yt/yt/core/concurrency/periodic_executor.h>
#include <yt/yt/core/concurrency/action_queue.h>

#include <library/cpp/yt/threading/rw_spin_lock.h>

#include <util/random/shuffle.h>

#include <algorithm>
#include <vector>

namespace NYT::NChunkClient {

using namespace NErasure;
using namespace NLogging;
using namespace NConcurrency;
using namespace NProfiling;
using namespace NChunkClient::NProto;
using namespace NErasureHelpers;

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TAdaptiveRepairingErasureReader)

class TAdaptiveRepairingErasureReader
    : public TErasureChunkReaderBase
{
public:
    TAdaptiveRepairingErasureReader(
        TChunkId chunkId,
        ICodec* codec,
        TErasureReaderConfigPtr config,
        const std::vector<IChunkReaderAllowingRepairPtr>& readers,
        const TLogger& logger);

    TFuture<TRefCountedChunkMetaPtr> GetMeta(
        const TClientChunkReadOptions& options,
        std::optional<int> partitionTag,
        const std::optional<std::vector<int>>& extensionTags) override;

    TFuture<std::vector<TBlock>> ReadBlocks(
        const TReadBlocksOptions& options,
        const std::vector<int>& blockIndexes) override;

    TFuture<std::vector<TBlock>> ReadBlocks(
        const TReadBlocksOptions& options,
        int firstBlockIndex,
        int blockCount) override;

    TInstant GetLastFailureTime() const override;

    void UpdateBannedPartIndices();
    TPartIndexSet GetBannedPartIndices();

    TError CheckPartReaderIsSlow(int partIndex, i64 bytesReceived, TDuration timePassed);

private:
    friend class TErasureReaderWithOverridenThrottlers;

    TRefCountedChunkMetaPtr DoGetMeta(
        const TClientChunkReadOptions& options,
        std::optional<int> partitionTag,
        const std::optional<std::vector<int>>& extensionTags);

    // ReadBlocks implementation with customizable readers list.
    // TODO(akozhikhov): Get rid of this.
    TFuture<std::vector<TBlock>> ReadBlocksImpl(
        const TReadBlocksOptions& options,
        const std::vector<int>& blockIndexes,
        const std::vector<IChunkReaderAllowingRepairPtr>& readers);

private:
    const TErasureReaderConfigPtr Config_;
    const TLogger Logger;

    const IInvokerPtr ReaderInvoker_ = CreateSerializedInvoker(TDispatcher::Get()->GetReaderInvoker());

    const TRepairingReadersObserverPtr ReadersObserver_;
};

DEFINE_REFCOUNTED_TYPE(TAdaptiveRepairingErasureReader)

////////////////////////////////////////////////////////////////////////////////

TAdaptiveRepairingErasureReader::TAdaptiveRepairingErasureReader(
    TChunkId chunkId,
    ICodec* codec,
    TErasureReaderConfigPtr config,
    const std::vector<IChunkReaderAllowingRepairPtr>& partReaders,
    const TLogger& logger)
    : TErasureChunkReaderBase(chunkId, codec, partReaders)
    , Config_(config)
    , Logger(logger)
    , ReadersObserver_(New<TRepairingReadersObserver>(Codec_, Config_, ReaderInvoker_, partReaders))
{
    YT_VERIFY(Config_->EnableAutoRepair);
}

TFuture<TRefCountedChunkMetaPtr> TAdaptiveRepairingErasureReader::GetMeta(
    const TClientChunkReadOptions& options,
    std::optional<int> partitionTag,
    const std::optional<std::vector<int>>& extensionTags)
{
    YT_VERIFY(!partitionTag);
    if (extensionTags) {
        for (const auto& forbiddenTag : {TProtoExtensionTag<TBlocksExt>::Value}) {
            auto it = std::find(extensionTags->begin(), extensionTags->end(), forbiddenTag);
            YT_VERIFY(it == extensionTags->end());
        }
    }

    return BIND(
        &TAdaptiveRepairingErasureReader::DoGetMeta,
        MakeStrong(this),
        options,
        partitionTag,
        extensionTags)
        .AsyncVia(ReaderInvoker_)
        .Run();
}

NErasure::TPartIndexList GetDataPartIndices(const NErasure::ICodec* codec)
{
    NErasure::TPartIndexList parts(codec->GetDataPartCount());
    std::iota(parts.begin(), parts.end(), 0);
    return parts;
}


TFuture<std::vector<TBlock>> TAdaptiveRepairingErasureReader::ReadBlocks(
    const TReadBlocksOptions& options,
    const std::vector<int>& blockIndexes)
{
    return ReadBlocksImpl(options, blockIndexes, Readers_);
}

TFuture<std::vector<TBlock>> TAdaptiveRepairingErasureReader::ReadBlocks(
    const TReadBlocksOptions& options,
    int firstBlockIndex,
    int blockCount)
{
    std::vector<int> blockIndexes(blockCount);
    std::iota(blockIndexes.begin(), blockIndexes.end(), firstBlockIndex);
    return ReadBlocks(options, blockIndexes);
}

TFuture<std::vector<TBlock>> TAdaptiveRepairingErasureReader::ReadBlocksImpl(
    const TReadBlocksOptions& options,
    const std::vector<int>& blockIndexes,
    const std::vector<IChunkReaderAllowingRepairPtr>& allReaders)
{
    auto target = TAdaptiveErasureRepairingSession::TTarget {
        .Existing = GetDataPartIndices(Codec_)
    };

    auto session = New<TAdaptiveErasureRepairingSession>(
        GetChunkId(),
        Codec_,
        ReadersObserver_,
        allReaders,
        ReaderInvoker_,
        target,
        Logger);

    return session->Run<std::vector<TBlock>>(
        [=, this, this_ = MakeStrong(this)] (
            const NErasure::TPartIndexList& erasedParts,
            const std::vector<IChunkReaderAllowingRepairPtr>& availableReaders)
        {
            if (!erasedParts.empty()) {
                YT_LOG_DEBUG("Reading blocks with repair (BlockIndexes: %v, BannedPartIndices: %v)",
                    blockIndexes,
                    erasedParts);
            }

            return CreateRepairingErasureReader(
                GetChunkId(),
                Codec_,
                erasedParts,
                availableReaders,
                Logger)
                ->ReadBlocks(options, blockIndexes);
        });
}

TInstant TAdaptiveRepairingErasureReader::GetLastFailureTime() const
{
    return TInstant();
}

TRefCountedChunkMetaPtr TAdaptiveRepairingErasureReader::DoGetMeta(
    const TClientChunkReadOptions& options,
    std::optional<int> partitionTag,
    const std::optional<std::vector<int>>& extensionTags)
{
    std::vector<TError> errors;

    std::vector<int> indices(Readers_.size());
    std::iota(indices.begin(), indices.end(), 0);
    Shuffle(indices.begin(), indices.end());

    auto now = GetInstant();
    for (auto index : indices) {
        if (ReadersObserver_->IsReaderRecentlyFailed(now, index)) {
            continue;
        }
        auto result = WaitFor(Readers_[index]->GetMeta(options, partitionTag, extensionTags));
        if (result.IsOK()) {
            return result.Value();
        }
        errors.push_back(result);
    }

    THROW_ERROR_EXCEPTION(
        NChunkClient::EErrorCode::ChunkMetaFetchFailed,
        "Failed to get chunk meta of chunk %v from any of valid part readers",
        GetChunkId())
        << errors;
}

////////////////////////////////////////////////////////////////////////////////

IChunkReaderPtr CreateAdaptiveRepairingErasureReader(
    TChunkId chunkId,
    ICodec* codec,
    TErasureReaderConfigPtr config,
    const std::vector<IChunkReaderAllowingRepairPtr>& partReaders,
    const TLogger& logger)
{
    if (!config->EnableAutoRepair) {
        return CreateRepairingErasureReader(
            chunkId,
            codec,
            /*erasedIndices*/ TPartIndexList(),
            partReaders,
            logger);
    }

    return New<TAdaptiveRepairingErasureReader>(
        chunkId,
        codec,
        config,
        partReaders,
        logger);
}

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TErasureReaderWithOverridenThrottlers)

class TErasureReaderWithOverridenThrottlers
    : public IChunkReader
{
public:
    TErasureReaderWithOverridenThrottlers(
        TAdaptiveRepairingErasureReaderPtr underlyingReader,
        const IThroughputThrottlerPtr& bandwidthThrottler,
        const IThroughputThrottlerPtr& rpsThrottler)
        : UnderlyingReader_(std::move(underlyingReader))
    {
        ReaderAdapters_.reserve(UnderlyingReader_->Readers_.size());
        for (int i = 0; i < std::ssize(UnderlyingReader_->Readers_); ++i) {
            ReaderAdapters_.push_back(CreateReplicationReaderThrottlingAdapter(
                UnderlyingReader_->Readers_[i],
                bandwidthThrottler,
                rpsThrottler));
        }
    }

    TFuture<std::vector<TBlock>> ReadBlocks(
        const TReadBlocksOptions& options,
        const std::vector<int>& blockIndexes) override
    {
        return UnderlyingReader_->ReadBlocksImpl(
            options,
            blockIndexes,
            ReaderAdapters_);
    }

    TFuture<std::vector<TBlock>> ReadBlocks(
        const TReadBlocksOptions& options,
        int firstBlockIndex,
        int blockCount) override
    {
        std::vector<int> blockIndexes(blockCount);
        std::iota(blockIndexes.begin(), blockIndexes.end(), firstBlockIndex);
        return ReadBlocks(options, blockIndexes);
    }

    TFuture<TRefCountedChunkMetaPtr> GetMeta(
        const TClientChunkReadOptions& options,
        std::optional<int> partitionTag,
        const std::optional<std::vector<int>>& extensionTags) override
    {
        return UnderlyingReader_->GetMeta(options, partitionTag, extensionTags);
    }

    TChunkId GetChunkId() const override
    {
        return UnderlyingReader_->GetChunkId();
    }

    TInstant GetLastFailureTime() const override
    {
        return UnderlyingReader_->GetLastFailureTime();
    }

private:
    const TAdaptiveRepairingErasureReaderPtr UnderlyingReader_;

    std::vector<IChunkReaderAllowingRepairPtr> ReaderAdapters_;
};

DEFINE_REFCOUNTED_TYPE(TErasureReaderWithOverridenThrottlers)

////////////////////////////////////////////////////////////////////////////////

IChunkReaderPtr CreateAdaptiveRepairingErasureReaderThrottlingAdapter(
    const IChunkReaderPtr& underlyingReader,
    IThroughputThrottlerPtr bandwidthThrottler,
    IThroughputThrottlerPtr rpsThrottler)
{
    auto* underylingErasureReader = dynamic_cast<TAdaptiveRepairingErasureReader*>(underlyingReader.Get());
    YT_VERIFY(underylingErasureReader);

    return New<TErasureReaderWithOverridenThrottlers>(
        underylingErasureReader,
        std::move(bandwidthThrottler),
        std::move(rpsThrottler));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
