#include "private.h"
#include "config.h"
#include "erasure_reader.h"
#include "erasure_repair.h"
#include "erasure_helpers.h"
#include "dispatcher.h"
#include "chunk_reader_statistics.h"

#include <yt/yt/client/misc/workload.h>

#include <yt/yt/core/concurrency/spinlock.h>
#include <yt/yt/core/concurrency/periodic_executor.h>
#include <yt/yt/core/concurrency/action_queue.h>

#include <util/random/shuffle.h>

#include <algorithm>
#include <vector>

static constexpr double SpeedComparisonPrecision = 1e-9;

namespace NYT::NChunkClient {

using namespace NErasure;
using namespace NLogging;
using namespace NConcurrency;
using namespace NProfiling;
using namespace NChunkClient::NProto;
using namespace NErasureHelpers;

////////////////////////////////////////////////////////////////////////////////

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
        const TClientBlockReadOptions& options,
        std::optional<int> partitionTag,
        const std::optional<std::vector<int>>& extensionTags) override;

    virtual TFuture<std::vector<TBlock>> ReadBlocks(
        const TClientBlockReadOptions& options,
        const std::vector<int>& blockIndexes,
        std::optional<i64> estimatedSize) override;

    virtual TFuture<std::vector<TBlock>> ReadBlocks(
        const TClientBlockReadOptions& options,
        int firstBlockIndex,
        int blockCount,
        std::optional<i64> estimatedSize) override;

    virtual TInstant GetLastFailureTime() const override;

    void UpdateBannedPartIndices();
    TPartIndexSet GetBannedPartIndices();

    TError CheckPartReaderIsSlow(int partIndex, i64 bytesReceived, TDuration timePassed);

private:
    void MaybeUnbanReaders();

    TRefCountedChunkMetaPtr DoGetMeta(
        const TClientBlockReadOptions& options,
        std::optional<int> partitionTag,
        const std::optional<std::vector<int>>& extensionTags);

    bool CheckReaderRecentlyFailed(TInstant now, int index) const;

    const TErasureReaderConfigPtr Config_;
    const TLogger Logger;

    const IInvokerPtr ReaderInvoker_ = CreateSerializedInvoker(TDispatcher::Get()->GetReaderInvoker());

    TPartIndexSet BannedPartIndices_;
    YT_DECLARE_SPINLOCK(TReaderWriterSpinLock, IndicesLock_);
    std::vector<TInstant> SlowReaderBanTimes_;
    TPeriodicExecutorPtr ExpirationTimesExecutor_;
};

////////////////////////////////////////////////////////////////////////////////

class TAdaptiveRepairingErasureReaderSession
    : public TRefCounted
{
public:
    TAdaptiveRepairingErasureReaderSession(
        ICodec* codec,
        const TErasureReaderConfigPtr config,
        const TLogger& logger,
        const std::vector<IChunkReaderAllowingRepairPtr>& readers,
        const TClientBlockReadOptions& options,
        const TErasurePlacementExt& placementExt,
        const std::vector<int>& blockIndexes,
        std::optional<i64> estimatedSize,
        const TWeakPtr<TAdaptiveRepairingErasureReader>& reader)
        : Codec_(codec)
        , Config_(config)
        , Reader_(reader)
        , Logger(logger ? logger : ChunkClientLogger)
        , Readers_(readers)
        , BlockReadOptions_(options)
        , PlacementExt_(placementExt)
        , BlockIndexes_(blockIndexes)
        , EstimatedSize_(estimatedSize)
        , DataBlocksPlacementInParts_(BuildDataBlocksPlacementInParts(BlockIndexes_, PlacementExt_))
    {
        if (Config_->EnableAutoRepair) {
            YT_VERIFY(Readers_.size() == Codec_->GetTotalPartCount());
        } else {
            YT_VERIFY(Readers_.size() == Codec_->GetDataPartCount());
        }
    }

    TFuture<std::vector<TBlock>> Run()
    {
        return BIND(&TAdaptiveRepairingErasureReaderSession::DoRun, MakeStrong(this))
            .AsyncVia(ReaderInvoker_)
            .Run();
    }

private:
    ICodec* const Codec_;
    const TErasureReaderConfigPtr Config_;
    const TWeakPtr<TAdaptiveRepairingErasureReader> Reader_;
    const TLogger Logger;
    const std::vector<IChunkReaderAllowingRepairPtr> Readers_;
    const TClientBlockReadOptions BlockReadOptions_;
    const TErasurePlacementExt PlacementExt_;
    const std::vector<int> BlockIndexes_;
    const std::optional<i64> EstimatedSize_;
    const TDataBlocksPlacementInParts DataBlocksPlacementInParts_;

    const IInvokerPtr ReaderInvoker_ = CreateSerializedInvoker(TDispatcher::Get()->GetReaderInvoker());

    std::vector<TBlock> DoRun()
    {
        auto reader = Reader_.Lock();
        if (!reader) {
            return std::vector<TBlock>();
        }

        if (!Config_->EnableAutoRepair) {
            auto repairingReader = CreateRepairingErasureReader(
                reader->GetChunkId(),
                Codec_,
                /* erasedIndices */ TPartIndexList(),
                Readers_,
                Logger);
            return WaitFor(repairingReader->ReadBlocks(BlockReadOptions_, BlockIndexes_))
                .ValueOrThrow();
        }

        std::optional<TPartIndexSet> erasedIndicesOnPreviousIteration;
        std::vector<TError> innerErrors;
        while (true) {
            reader->UpdateBannedPartIndices();

            auto bannedPartIndices = reader->GetBannedPartIndices();

            TPartIndexList bannedPartIndicesList;
            for (size_t i = 0; i < Readers_.size(); ++i) {
                if (bannedPartIndices.test(i)) {
                    bannedPartIndicesList.push_back(i);
                }
            }

            if (erasedIndicesOnPreviousIteration && bannedPartIndices == *erasedIndicesOnPreviousIteration) {
                THROW_ERROR_EXCEPTION("Error reading chunk %v with repair; cannot proceed since the list of valid underlying part readers did not change",
                    reader->GetChunkId())
                    << TErrorAttribute("banned_part_indexes", bannedPartIndicesList)
                    << innerErrors;
            }

            erasedIndicesOnPreviousIteration = bannedPartIndices;

            auto optionalRepairIndices = Codec_->GetRepairIndices(bannedPartIndicesList);
            if (!optionalRepairIndices) {
                THROW_ERROR_EXCEPTION("Not enough parts to read chunk %v with repair",
                    reader->GetChunkId())
                    << TErrorAttribute("banned_part_indexes", bannedPartIndicesList)
                    << innerErrors;
            }
            auto repairIndices = *optionalRepairIndices;

            std::vector<IChunkReaderAllowingRepairPtr> readers;
            for (int index = 0; index < Codec_->GetDataPartCount(); ++index) {
                if (!std::binary_search(bannedPartIndicesList.begin(), bannedPartIndicesList.end(), index)) {
                    readers.push_back(Readers_[index]);
                }
            }
            for (int index = Codec_->GetDataPartCount(); index < Codec_->GetTotalPartCount(); ++index) {
                if (std::binary_search(repairIndices.begin(), repairIndices.end(), index)) {
                    readers.push_back(Readers_[index]);
                }
            }

            if (!bannedPartIndicesList.empty()) {
                YT_LOG_DEBUG("Reading blocks with repair (BlockIndexes: %v, BannedPartIndices: %v)",
                    BlockIndexes_,
                    bannedPartIndicesList);
            }

            auto repairingReader = CreateRepairingErasureReader(
                reader->GetChunkId(),
                Codec_,
                bannedPartIndicesList,
                readers,
                Logger);
            auto result = WaitFor(repairingReader->ReadBlocks(BlockReadOptions_, BlockIndexes_, EstimatedSize_));

            if (result.IsOK()) {
                return result.Value();
            } else {
                innerErrors.push_back(result);
            }
        }
    }
};

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
    , SlowReaderBanTimes_(codec->GetTotalPartCount(), TInstant())
{
    if (Config_->EnableAutoRepair) {
        for (int partIndex = 0; partIndex < Codec_->GetTotalPartCount(); ++partIndex) {
            auto callback = BIND([partIndex, weakThis = MakeWeak(this)] (i64 bytesReceived, TDuration timePassed) {
                auto this_ = weakThis.Lock();
                if (!this_) {
                    return TError();
                }
                return this_->CheckPartReaderIsSlow(partIndex, bytesReceived, timePassed);
            });
            Readers_[partIndex]->SetSlownessChecker(callback);
        }

        ExpirationTimesExecutor_ = New<TPeriodicExecutor>(
            ReaderInvoker_,
            BIND(&TAdaptiveRepairingErasureReader::MaybeUnbanReaders, MakeWeak(this)),
            std::min(Config_->SlowReaderExpirationTimeout, Config_->ReplicationReaderFailureTimeout)
        );
        ExpirationTimesExecutor_->Start();
    }
}

TFuture<TRefCountedChunkMetaPtr> TAdaptiveRepairingErasureReader::GetMeta(
    const TClientBlockReadOptions& options,
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

TFuture<std::vector<TBlock>> TAdaptiveRepairingErasureReader::ReadBlocks(
    const TClientBlockReadOptions& options,
    const std::vector<int>& blockIndexes,
    std::optional<i64> estimatedSize)
{
    // We don't use estimatedSize here, because during repair actual use of bandwidth is much higher that the size of blocks;
    return PreparePlacementMeta(options).Apply(
        BIND([=, this_ = MakeStrong(this)] () {
            auto session = New<TAdaptiveRepairingErasureReaderSession>(
                Codec_,
                Config_,
                Logger,
                Readers_,
                options,
                PlacementExt_,
                blockIndexes,
                estimatedSize,
                MakeStrong(this));
            return session->Run();
        }));
}

TFuture<std::vector<TBlock>> TAdaptiveRepairingErasureReader::ReadBlocks(
    const TClientBlockReadOptions& /*options*/,
    int /*firstBlockIndex*/,
    int /*blockCount*/,
    std::optional<i64> /*estimatedSize*/)
{
    YT_ABORT();
}

TInstant TAdaptiveRepairingErasureReader::GetLastFailureTime() const
{
    return TInstant();
}

void TAdaptiveRepairingErasureReader::UpdateBannedPartIndices()
{
    TPartIndexList failedReaderIndices;
    {
        auto now = NProfiling::GetInstant();
        auto guard = ReaderGuard(IndicesLock_);
        for (size_t index = 0; index < Readers_.size(); ++index) {
            if (CheckReaderRecentlyFailed(now, index) && !BannedPartIndices_.test(index)) {
                failedReaderIndices.push_back(index);
            }
        }
    }

    if (failedReaderIndices.empty()) {
        return;
    }

    {
        auto guard = WriterGuard(IndicesLock_);
        for (auto index : failedReaderIndices) {
            BannedPartIndices_.set(index);
        }
    }
}

TPartIndexSet TAdaptiveRepairingErasureReader::GetBannedPartIndices()
{
    auto guard = ReaderGuard(IndicesLock_);
    return BannedPartIndices_;
}

void TAdaptiveRepairingErasureReader::MaybeUnbanReaders()
{
    auto guard = WriterGuard(IndicesLock_);

    auto now = NProfiling::GetInstant();
    for (size_t index = 0; index < SlowReaderBanTimes_.size(); ++index) {
        if (!BannedPartIndices_.test(index)) {
            continue;
        }

        auto slownessBanExpiration = SlowReaderBanTimes_[index]
            ? SlowReaderBanTimes_[index] + Config_->SlowReaderExpirationTimeout
            : TInstant();

        if (now >= slownessBanExpiration) {
            SlowReaderBanTimes_[index] = TInstant();
            if (!CheckReaderRecentlyFailed(now, index)) {
                BannedPartIndices_.set(index, false);
            }
        }
    }
}

TError TAdaptiveRepairingErasureReader::CheckPartReaderIsSlow(int partIndex, i64 bytesReceived, TDuration timePassed)
{
    double secondsPassed = timePassed.SecondsFloat();
    double speed = secondsPassed < SpeedComparisonPrecision ? 0.0 : static_cast<double>(bytesReceived) / secondsPassed;
    if (speed > Config_->ReplicationReaderSpeedLimitPerSec || timePassed < Config_->ReplicationReaderTimeout) {
        return TError();
    }

    {
        auto guard = WriterGuard(IndicesLock_);
        if (BannedPartIndices_.test(partIndex)) {
            return TError("Reader of part %v is already banned", partIndex);
        }
        BannedPartIndices_.set(partIndex);
        if (Codec_->CanRepair(BannedPartIndices_)) {
            SlowReaderBanTimes_[partIndex] = GetInstant();
            return TError("Reader of part %v is marked as slow since speed is less than %v and passed more than %v seconds from start",
                partIndex,
                speed,
                timePassed.Seconds());
        } else {
            BannedPartIndices_.flip(partIndex);
            return TError();
        }
    }
}

TRefCountedChunkMetaPtr TAdaptiveRepairingErasureReader::DoGetMeta(
    const TClientBlockReadOptions& options,
    std::optional<int> partitionTag,
    const std::optional<std::vector<int>>& extensionTags)
{
    std::vector<TError> errors;

    std::vector<int> indices(Readers_.size());
    std::iota(indices.begin(), indices.end(), 0);
    Shuffle(indices.begin(), indices.end());

    auto now = NProfiling::GetInstant();
    for (auto index : indices) {
        if (CheckReaderRecentlyFailed(now, index)) {
            continue;
        }
        auto result = WaitFor(Readers_[index]->GetMeta(options, partitionTag, extensionTags));
        if (result.IsOK()) {
            return result.Value();
        }
        errors.push_back(result);
    }

    THROW_ERROR_EXCEPTION("Failed to get chunk meta of chunk %v from any of valid part readers",
        GetChunkId())
        << errors;
}

bool TAdaptiveRepairingErasureReader::CheckReaderRecentlyFailed(TInstant now, int index) const
{
    if (auto lastFailureTime = Readers_[index]->GetLastFailureTime()) {
        return now < lastFailureTime + Config_->ReplicationReaderFailureTimeout;
    }
    return false;
}

////////////////////////////////////////////////////////////////////////////////

IChunkReaderPtr CreateAdaptiveRepairingErasureReader(
    TChunkId chunkId,
    ICodec* codec,
    TErasureReaderConfigPtr config,
    const std::vector<IChunkReaderAllowingRepairPtr>& partReaders,
    const TLogger& logger)
{
    return New<TAdaptiveRepairingErasureReader>(
        chunkId,
        codec,
        config,
        partReaders,
        logger);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
