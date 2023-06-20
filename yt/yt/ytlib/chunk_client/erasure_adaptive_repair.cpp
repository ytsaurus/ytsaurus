#include "erasure_adaptive_repair.h"

#include <yt/yt/client/chunk_client/config.h>

#include <yt/yt/ytlib/chunk_client/chunk_reader_allowing_repair.h>
#include <yt/yt/ytlib/chunk_client/chunk_meta_extensions.h>

#include <yt/yt/core/concurrency/action_queue.h>

#include <yt/yt/library/erasure/impl/codec.h>

namespace NYT::NChunkClient {

////////////////////////////////////////////////////////////////////////////////

static constexpr double SpeedComparisonPrecision = 1e-9;

////////////////////////////////////////////////////////////////////////////////

TRepairingReadersObserver::TRepairingReadersObserver(
    NErasure::ICodec* codec,
    TErasureReaderConfigPtr config,
    IInvokerPtr invoker,
    std::vector<IChunkReaderAllowingRepairPtr> readers)
    : Codec_(codec)
    , Config_(std::move(config))
    , Readers_(std::move(readers))
    , SlowReaderBanTimes_(codec->GetTotalPartCount(), TInstant())
{
    YT_VERIFY(Config_->EnableAutoRepair);
    YT_VERIFY(std::ssize(Readers_) == Codec_->GetTotalPartCount());

    for (int partIndex = 0; partIndex < Codec_->GetTotalPartCount(); ++partIndex) {
        auto callback = BIND_NO_PROPAGATE([partIndex, weakThis = MakeWeak(this)] (i64 bytesReceived, TDuration timePassed) {
            auto this_ = weakThis.Lock();
            if (!this_) {
                return TError();
            }
            return this_->CheckPartReaderIsSlow(partIndex, bytesReceived, timePassed);
        });
        Readers_[partIndex]->SetSlownessChecker(callback);
    }

    CheckReadersForUnbanExecutor_ = New<NConcurrency::TPeriodicExecutor>(
        std::move(invoker),
        BIND(&TRepairingReadersObserver::CheckReadersForUnban, MakeWeak(this)),
        std::min(Config_->SlowReaderExpirationTimeout, Config_->ReplicationReaderFailureTimeout));
    CheckReadersForUnbanExecutor_->Start();
}

TError TRepairingReadersObserver::CheckPartReaderIsSlow(int partIndex, i64 bytesReceived, TDuration timePassed)
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

void TRepairingReadersObserver::CheckReadersForUnban()
{
    auto guard = WriterGuard(IndicesLock_);

    auto now = GetInstant();
    for (size_t index = 0; index < SlowReaderBanTimes_.size(); ++index) {
        if (!BannedPartIndices_.test(index)) {
            continue;
        }

        auto slownessBanExpiration = SlowReaderBanTimes_[index]
            ? SlowReaderBanTimes_[index] + Config_->SlowReaderExpirationTimeout
            : TInstant();

        if (now >= slownessBanExpiration) {
            SlowReaderBanTimes_[index] = TInstant();
            if (!IsReaderRecentlyFailed(now, index)) {
                BannedPartIndices_.set(index, false);
            }
        }
    }
}

bool TRepairingReadersObserver::IsReaderRecentlyFailed(TInstant now, int index) const
{
    if (auto lastFailureTime = Readers_[index]->GetLastFailureTime()) {
        return now < lastFailureTime + Config_->ReplicationReaderFailureTimeout;
    }
    return false;
}

const TErasureReaderConfigPtr& TRepairingReadersObserver::GetConfig() const
{
    return Config_;
}

void TRepairingReadersObserver::UpdateBannedPartIndices()
{
    NErasure::TPartIndexList failedReaderIndices;
    {
        auto now = GetInstant();
        auto guard = ReaderGuard(IndicesLock_);
        for (size_t index = 0; index < Readers_.size(); ++index) {
            if (IsReaderRecentlyFailed(now, index) && !BannedPartIndices_.test(index)) {
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

NErasure::TPartIndexSet TRepairingReadersObserver::GetBannedPartIndices()
{
    auto guard = ReaderGuard(IndicesLock_);
    return BannedPartIndices_;
}

DEFINE_REFCOUNTED_TYPE(TRepairingReadersObserver)

////////////////////////////////////////////////////////////////////////////////

TAdaptiveErasureRepairingSession::TAdaptiveErasureRepairingSession(
    TChunkId chunkId,
    const NErasure::ICodec* codec,
    TRepairingReadersObserverPtr observer,
    std::vector<IChunkReaderAllowingRepairPtr> readers,
    IInvokerPtr invoker,
    TTarget target,
    NLogging::TLogger logger,
    NProfiling::TCounter adaptivelyRepairedCounter)
    : ChunkId_(chunkId)
    , Codec_(codec)
    , Observer_(std::move(observer))
    , Readers_(std::move(readers))
    , Invoker_(std::move(invoker))
    , Target_(std::move(target))
    , Logger(std::move(logger))
    , AdaptivelyRepairedCounter_(std::move(adaptivelyRepairedCounter))
{
    YT_VERIFY(std::ssize(Readers_) == Codec_->GetTotalPartCount());
}

NErasure::TPartIndexSet TAdaptiveErasureRepairingSession::CalculateBannedParts()
{
    Observer_->UpdateBannedPartIndices();
    auto bannedPartIndices = Observer_->GetBannedPartIndices();

    // Never directly read erased parts (only repair from other existing parts).
    for (auto erasedPart : Target_.Erased) {
        bannedPartIndices.set(erasedPart);
    }
    return bannedPartIndices;
}

NErasure::TPartIndexList TAdaptiveErasureRepairingSession::ToReadersIndexList(NErasure::TPartIndexSet indexSet)
{
    NErasure::TPartIndexList result;
    for (int index = 0; index < std::ssize(Readers_); ++index) {
        if (indexSet.test(index)) {
            result.push_back(index);
        }
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

class TUnavailablePartReader
    : public IChunkReaderAllowingRepair
{
public:
    TUnavailablePartReader(TChunkId chunkId)
        : ChunkId_(chunkId)
    { }

    TFuture<TRefCountedChunkMetaPtr> GetMeta(
        const TClientChunkReadOptions& /*options*/,
        std::optional<int> /*partitionTag*/,
        const std::optional<std::vector<int>>& /*extensionTag*/) override
    {
        return MakeFuture<TRefCountedChunkMetaPtr>(MakeError());
    }

    TChunkId GetChunkId() const override
    {
        return ChunkId_;
    }

    TFuture<std::vector<TBlock>> ReadBlocks(
        const TReadBlocksOptions& /*options*/,
        const std::vector<int>& /*blockIndices*/) override
    {
        return MakeFuture<std::vector<TBlock>>(MakeError());
    }

    TFuture<std::vector<TBlock>> ReadBlocks(
        const TReadBlocksOptions& /*options*/,
        int /*firstblockIndex*/,
        int /*blockCount*/) override
    {
        return MakeFuture<std::vector<TBlock>>(MakeError());
    }

    TInstant GetLastFailureTime() const override
    {
        return TInstant::Now();
    }

    void SetSlownessChecker(TCallback<TError(i64, TDuration)> /*slownessChecker*/) override
    { }

private:
    const TChunkId ChunkId_;

    TError MakeError() const
    {
        return TError("Part %v is unavailable",
            ChunkId_);
    }
};

IChunkReaderAllowingRepairPtr CreateUnavailablePartReader(TChunkId chunkId)
{
    return New<TUnavailablePartReader>(chunkId);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
