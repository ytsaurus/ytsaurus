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

DECLARE_REFCOUNTED_CLASS(TRepairingErasureReaderBase)

class TRepairingErasureReaderBase
    : public IChunkReader
{
protected:
    const TChunkId ChunkId_;
    NErasure::ICodec* const Codec_;
    const std::vector<IChunkReaderAllowingRepairPtr> Readers_;
    const IInvokerPtr ReaderInvoker_;

    const TLogger Logger;


    TRepairingErasureReaderBase(
        TChunkId chunkId,
        ICodec* codec,
        std::vector<IChunkReaderAllowingRepairPtr> readers,
        const TLogger& logger)
        : ChunkId_(chunkId)
        , Codec_(codec)
        , Readers_(std::move(readers))
        , ReaderInvoker_(TDispatcher::Get()->GetReaderInvoker())
        , Logger(logger)
    { }

    TChunkId GetChunkId() const override
    {
        return ChunkId_;
    }

    TFuture<TRefCountedChunkMetaPtr> GetMeta(
        const TClientChunkReadOptions& options,
        std::optional<int> partitionTag,
        const std::optional<std::vector<int>>& extensionTags) override
    {
        YT_VERIFY(!partitionTag);

        if (extensionTags) {
            for (const auto& forbiddenTag : {TProtoExtensionTag<TBlocksExt>::Value}) {
                auto it = std::find(extensionTags->begin(), extensionTags->end(), forbiddenTag);
                YT_VERIFY(it == extensionTags->end());
            }
        }

        return DoGetMeta(options, extensionTags);
    }

    TFuture<std::vector<TBlock>> ReadBlocks(
        const TReadBlocksOptions& options,
        const std::vector<int>& blockIndexes) override
    {
        return DoReadBlocks(options, blockIndexes, Readers_);
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

    TInstant GetLastFailureTime() const override
    {
        YT_UNIMPLEMENTED();
    }

    virtual TFuture<std::vector<TBlock>> DoReadBlocks(
        const TReadBlocksOptions& options,
        const std::vector<int>& blockIndexes,
        const std::vector<IChunkReaderAllowingRepairPtr>& readers) = 0;

    virtual TFuture<TRefCountedChunkMetaPtr> DoGetMeta(
        const TClientChunkReadOptions& options,
        const std::optional<std::vector<int>>& extensionTags) = 0;
};

DEFINE_REFCOUNTED_TYPE(TRepairingErasureReaderBase)

////////////////////////////////////////////////////////////////////////////////

class TRepairingErasureReader
    : public TRepairingErasureReaderBase
{
public:
    TRepairingErasureReader(
        TChunkId chunkId,
        ICodec* codec,
        std::vector<IChunkReaderAllowingRepairPtr> readers,
        std::optional<TRepairingErasureReaderTestingOptions> testingOptions,
        const TLogger& logger)
        : TRepairingErasureReaderBase(chunkId, codec, std::move(readers), logger)
        , TestingOptions_(std::move(testingOptions))
    { }

private:
    const std::optional<TRepairingErasureReaderTestingOptions> TestingOptions_;

    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, PlacementExtLock_);
    TFuture<TErasurePlacementExt> PlacementExtFuture_;


    TFuture<TRefCountedChunkMetaPtr> DoGetMeta(
        const TClientChunkReadOptions& options,
        const std::optional<std::vector<int>>& extensionTags) override
    {
        const auto& reader = Readers_[RandomNumber(Readers_.size())];
        return reader->GetMeta(options, /*partitionTag*/ std::nullopt, extensionTags);
    }

    TFuture<std::vector<TBlock>> DoReadBlocks(
        const TReadBlocksOptions& options,
        const std::vector<int>& blockIndexes,
        const std::vector<IChunkReaderAllowingRepairPtr>& readers) override
    {
        return PreparePlacementMeta(options.ClientOptions, readers)
            .Apply(BIND([=, this, this_ = MakeStrong(this)] (TErasurePlacementExt placementExt) {
                auto erasedParts = TestingOptions_
                    ? TestingOptions_->ErasedIndices
                    : TPartIndexList{};
                return ExecuteErasureRepairingSession(
                    ChunkId_,
                    Codec_,
                    erasedParts,
                    readers,
                    blockIndexes,
                    options,
                    ReaderInvoker_,
                    Logger,
                    std::move(placementExt));
            }));
    }

    TFuture<TErasurePlacementExt> PreparePlacementMeta(
        const TClientChunkReadOptions& options,
        const std::vector<IChunkReaderAllowingRepairPtr>& readers)
    {
        {
            auto guard = ReaderGuard(PlacementExtLock_);

            if (PlacementExtFuture_) {
                return PlacementExtFuture_;
            }
        }

        {
            auto guard = WriterGuard(PlacementExtLock_);

            if (!PlacementExtFuture_) {
                const auto& reader = readers[RandomNumber(readers.size())];
                PlacementExtFuture_ = GetPlacementMeta(reader, options)
                    .Apply(BIND([] (const TRefCountedChunkMetaPtr& meta) {
                        return GetProtoExtension<NProto::TErasurePlacementExt>(meta->extensions());
                    }));
            }

            return PlacementExtFuture_;
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

class TAdaptiveRepairingErasureReader
    : public TRepairingErasureReaderBase
{
public:
    TAdaptiveRepairingErasureReader(
        TChunkId chunkId,
        ICodec* codec,
        TErasureReaderConfigPtr config,
        std::vector<IChunkReaderAllowingRepairPtr> readers,
        const TLogger& logger)
        : TRepairingErasureReaderBase(chunkId, codec, std::move(readers), logger)
        , ReadersObserver_(New<TRepairingReadersObserver>(
            Codec_,
            std::move(config),
            ReaderInvoker_,
            Readers_))
    { }

private:
    friend class TErasureReaderWithOverriddenThrottlers;

    const TRepairingReadersObserverPtr ReadersObserver_;


    TFuture<TRefCountedChunkMetaPtr> DoGetMeta(
        const TClientChunkReadOptions& options,
        const std::optional<std::vector<int>>& extensionTags) override
    {
        return BIND(
            &TAdaptiveRepairingErasureReader::DoGetMetaImpl,
            MakeStrong(this),
            options,
            extensionTags)
            .AsyncVia(ReaderInvoker_)
            .Run();
    }

    TRefCountedChunkMetaPtr DoGetMetaImpl(
        const TClientChunkReadOptions& options,
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
            auto result = WaitFor(Readers_[index]->GetMeta(options, /*partitionTag*/ std::nullopt, extensionTags));
            if (result.IsOK()) {
                return result.Value();
            }
            errors.push_back(result);
        }

        // NB: This is solely for testing purposes. We want to eliminate unlikely test flaps.
        if (ReadersObserver_->GetConfig()->ChunkMetaCacheFailureProbability) {
            return DoGetMetaImpl(options, extensionTags);
        }

        THROW_ERROR_EXCEPTION(
            NChunkClient::EErrorCode::ChunkMetaFetchFailed,
            "Failed to get chunk meta of chunk %v from any of valid part readers",
            GetChunkId())
            << errors;
    }

    // ReadBlocks implementation with customizable readers list.
    TFuture<std::vector<TBlock>> DoReadBlocks(
        const TReadBlocksOptions& options,
        const std::vector<int>& blockIndexes,
        const std::vector<IChunkReaderAllowingRepairPtr>& readers) override
    {
        NErasure::TPartIndexList existingPartIndices(Codec_->GetDataPartCount());
        std::iota(existingPartIndices.begin(), existingPartIndices.end(), 0);

        auto session = New<TAdaptiveErasureRepairingSession>(
            GetChunkId(),
            Codec_,
            ReadersObserver_,
            readers,
            ReaderInvoker_,
            TAdaptiveErasureRepairingSession::TTarget{
                .Existing = existingPartIndices,
            },
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

                const auto& reader = availableReaders[RandomNumber(availableReaders.size())];
                return GetPlacementMeta(reader, options.ClientOptions)
                    .Apply(BIND([=, this, this_ = MakeStrong(this)] (const TRefCountedChunkMetaPtr& meta) {
                        auto placementExt = GetProtoExtension<NProto::TErasurePlacementExt>(meta->extensions());
                        return ExecuteErasureRepairingSession(
                            ChunkId_,
                            Codec_,
                            erasedParts,
                            availableReaders,
                            blockIndexes,
                            options,
                            ReaderInvoker_,
                            Logger,
                            std::move(placementExt));
                    }));
            });
    }
};

////////////////////////////////////////////////////////////////////////////////

IChunkReaderPtr CreateAdaptiveRepairingErasureReader(
    TChunkId chunkId,
    ICodec* codec,
    TErasureReaderConfigPtr config,
    std::vector<IChunkReaderAllowingRepairPtr> partReaders,
    std::optional<TRepairingErasureReaderTestingOptions> testingOptions,
    const TLogger& logger)
{
    if (config->EnableAutoRepair) {
        return New<TAdaptiveRepairingErasureReader>(
            chunkId,
            codec,
            std::move(config),
            std::move(partReaders),
            logger);
    } else {
        return New<TRepairingErasureReader>(
            chunkId,
            codec,
            std::move(partReaders),
            testingOptions,
            logger);
    }
}

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TErasureReaderWithOverriddenThrottlers)

class TErasureReaderWithOverriddenThrottlers
    : public IChunkReader
{
public:
    TErasureReaderWithOverriddenThrottlers(
        TIntrusivePtr<TAdaptiveRepairingErasureReader> underlyingReader,
        const IThroughputThrottlerPtr& bandwidthThrottler,
        const IThroughputThrottlerPtr& rpsThrottler,
        const IThroughputThrottlerPtr& mediumThrottler)
        : UnderlyingReader_(std::move(underlyingReader))
    {
        ReaderAdapters_.reserve(UnderlyingReader_->Readers_.size());
        for (int i = 0; i < std::ssize(UnderlyingReader_->Readers_); ++i) {
            ReaderAdapters_.push_back(CreateReplicationReaderThrottlingAdapter(
                UnderlyingReader_->Readers_[i],
                bandwidthThrottler,
                rpsThrottler,
                mediumThrottler));
        }
    }

    TFuture<std::vector<TBlock>> ReadBlocks(
        const TReadBlocksOptions& options,
        const std::vector<int>& blockIndexes) override
    {
        return UnderlyingReader_->DoReadBlocks(
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
    const TIntrusivePtr<TAdaptiveRepairingErasureReader> UnderlyingReader_;

    std::vector<IChunkReaderAllowingRepairPtr> ReaderAdapters_;
};

DEFINE_REFCOUNTED_TYPE(TErasureReaderWithOverriddenThrottlers)

////////////////////////////////////////////////////////////////////////////////

IChunkReaderPtr CreateAdaptiveRepairingErasureReaderThrottlingAdapter(
    const IChunkReaderPtr& underlyingReader,
    IThroughputThrottlerPtr bandwidthThrottler,
    IThroughputThrottlerPtr rpsThrottler,
    IThroughputThrottlerPtr mediumThrottler)
{
    auto* underlyingErasureReader = dynamic_cast<TAdaptiveRepairingErasureReader*>(underlyingReader.Get());
    YT_VERIFY(underlyingErasureReader);

    return New<TErasureReaderWithOverriddenThrottlers>(
        underlyingErasureReader,
        std::move(bandwidthThrottler),
        std::move(rpsThrottler),
        std::move(mediumThrottler));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
