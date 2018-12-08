#include "private.h"
#include "config.h"
#include "repairing_reader.h"
#include "erasure_repair.h"
#include "erasure_helpers.h"
#include "dispatcher.h"
#include "chunk_reader_statistics.h"

#include <yt/client/misc/workload.h>

#include <yt/core/concurrency/rw_spinlock.h>
#include <yt/core/concurrency/periodic_executor.h>

#include <util/random/shuffle.h>

#include <algorithm>
#include <vector>

static constexpr double SpeedComparisonPrecision = 1e-9;

namespace NYT {
namespace NChunkClient {

using namespace NErasure;
using namespace NLogging;
using namespace NConcurrency;
using namespace NProfiling;
using namespace NChunkClient::NProto;
using namespace NErasureHelpers;

////////////////////////////////////////////////////////////////////////////////

class TRepairingReader
    : public TErasureChunkReaderBase
{
public:
    TRepairingReader(
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
        const std::optional<i64>& estimatedSize) override;

    virtual TFuture<std::vector<TBlock>> ReadBlocks(
        const TClientBlockReadOptions& options,
        int firstBlockIndex,
        int blockCount,
        const std::optional<i64>& estimatedSize) override;

    virtual bool IsValid() const override;

    void UpdateBannedPartIndices();

    TPartIndexSet GetBannedIndices();

    TError CheckPartReaderIsSlow(int partIndex, i64 bytesReceived, TDuration timePassed);

private:
    void CheckSlowReaders();

    TRefCountedChunkMetaPtr GetMetaAsync(
        const TClientBlockReadOptions& options,
        std::optional<int> partitionTag,
        const std::optional<std::vector<int>>& extensionTags);

    const TErasureReaderConfigPtr Config_;
    TLogger Logger;
    TPartIndexSet BannedPartIndices_;
    TReaderWriterSpinLock IndicesLock_;
    std::vector<TCpuInstant> SlowReaderBanTimes_;
    TPeriodicExecutorPtr ExpirationTimesExecutor_;
};

////////////////////////////////////////////////////////////////////////////////

class TRepairingReaderSession
    : public TRefCounted
{
public:
    TRepairingReaderSession(
        ICodec* codec,
        const TErasureReaderConfigPtr config,
        const TLogger& logger,
        const std::vector<IChunkReaderAllowingRepairPtr>& readers,
        const TClientBlockReadOptions& options,
        const TErasurePlacementExt& placementExt,
        const std::vector<int>& blockIndexes,
        const std::optional<i64>& estimatedSize,
        const TWeakPtr<TRepairingReader>& reader)
        : Codec_(codec)
        , Config_(config)
        , Logger(logger ? logger : ChunkClientLogger)
        , Readers_(readers)
        , BlockReadOptions_(options)
        , PlacementExt_(placementExt)
        , BlockIndexes_(blockIndexes)
        , EstimatedSize_(estimatedSize)
        , DataBlocksPlacementInParts_(BuildDataBlocksPlacementInParts(BlockIndexes_, PlacementExt_))
        , Reader_(reader)
    {
        if (Config_->EnableAutoRepair) {
            YCHECK(Readers_.size() == Codec_->GetTotalPartCount());
        } else {
            YCHECK(Readers_.size() == Codec_->GetDataPartCount());
        }
    }

    TFuture<std::vector<TBlock>> Run()
    {
        return BIND(&TRepairingReaderSession::DoRun, MakeStrong(this))
            .AsyncVia(TDispatcher::Get()->GetReaderInvoker())
            .Run();
    }

private:
    ICodec* const Codec_;
    const TErasureReaderConfigPtr Config_;
    TLogger Logger;
    std::vector<IChunkReaderAllowingRepairPtr> Readers_;
    const TClientBlockReadOptions BlockReadOptions_;
    const TErasurePlacementExt PlacementExt_;
    const std::vector<int> BlockIndexes_;
    const std::optional<i64> EstimatedSize_;
    TDataBlocksPlacementInParts DataBlocksPlacementInParts_;
    TWeakPtr<TRepairingReader> Reader_;

    std::vector<TBlock> DoRun()
    {
        if (!Config_->EnableAutoRepair) {
            auto reader = CreateRepairingErasureReader(Codec_, TPartIndexList(), Readers_);
            return WaitFor(reader->ReadBlocks(BlockReadOptions_, BlockIndexes_))
                .ValueOrThrow();
        }

        std::optional<TPartIndexSet> erasedIndicesOnPreviousIteration;
        std::vector<TError> innerErrors;
        while (true) {
            auto reader = Reader_.Lock();
            if (!reader) {
                return std::vector<TBlock>();
            }

            reader->UpdateBannedPartIndices();

            TPartIndexList bannedPartIndicesList;
            auto bannedPartIndices = reader->GetBannedIndices();

            if (erasedIndicesOnPreviousIteration && bannedPartIndices == *erasedIndicesOnPreviousIteration) {
                THROW_ERROR_EXCEPTION("Read with repair failed, but list of valid underlying part readers did not change")
                    << innerErrors;
            }

            for (size_t i = 0; i < Readers_.size(); ++i) {
                if (bannedPartIndices.test(i)) {
                    bannedPartIndicesList.push_back(i);
                }
            }
            erasedIndicesOnPreviousIteration = bannedPartIndices;

            auto optionalRepairIndices = Codec_->GetRepairIndices(bannedPartIndicesList);
            if (!optionalRepairIndices) {
                THROW_ERROR_EXCEPTION("Not enough parts to read with repair")
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
                LOG_DEBUG("Reading blocks with repair (BlockIndexes: %v, BannedPartIndices: %v)",
                    BlockIndexes_,
                    bannedPartIndicesList);
            }

            auto repairingReader = CreateRepairingErasureReader(Codec_, bannedPartIndicesList, readers);
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

TRepairingReader::TRepairingReader(
    ICodec* codec,
    TErasureReaderConfigPtr config,
    const std::vector<IChunkReaderAllowingRepairPtr>& readers,
    const TLogger& logger)
    : TErasureChunkReaderBase(codec, readers)
    , Config_(config)
    , Logger(logger)
    , SlowReaderBanTimes_(codec->GetTotalPartCount(), TCpuInstant())
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
            TDispatcher::Get()->GetReaderInvoker(),
            BIND(&TRepairingReader::CheckSlowReaders, MakeWeak(this)),
            Config_->SlowReaderExpirationTimeout
        );
        ExpirationTimesExecutor_->Start();
    }
}

TFuture<TRefCountedChunkMetaPtr> TRepairingReader::GetMeta(
    const TClientBlockReadOptions& options,
    std::optional<int> partitionTag,
    const std::optional<std::vector<int>>& extensionTags)
{
    YCHECK(!partitionTag);
    if (extensionTags) {
        for (const auto& forbiddenTag : {TProtoExtensionTag<TBlocksExt>::Value}) {
            auto it = std::find(extensionTags->begin(), extensionTags->end(), forbiddenTag);
            YCHECK(it == extensionTags->end());
        }
    }

    return BIND(
        &TRepairingReader::GetMetaAsync,
        MakeStrong(this),
        options,
        partitionTag,
        extensionTags)
        .AsyncVia(TDispatcher::Get()->GetReaderInvoker())
        .Run();
}

TFuture<std::vector<TBlock>> TRepairingReader::ReadBlocks(
    const TClientBlockReadOptions& options,
    const std::vector<int>& blockIndexes,
    const std::optional<i64>& estimatedSize)
{
    // We don't use estimatedSize here, because during repair actual use of bandwidth is much higher that the size of blocks;
    return PreparePlacementMeta(options).Apply(
        BIND([=, this_ = MakeStrong(this)] () {
            auto session = New<TRepairingReaderSession>(
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

TFuture<std::vector<TBlock>> TRepairingReader::ReadBlocks(
    const TClientBlockReadOptions& options,
    int firstBlockIndex,
    int blockCount,
    const std::optional<i64>& estimatedSize)
{
    Y_UNIMPLEMENTED();
}

bool TRepairingReader::IsValid() const
{
    return true;
}

void TRepairingReader::UpdateBannedPartIndices()
{
    TPartIndexList failedReaderIndices;
    {
        TReaderGuard guard(IndicesLock_);
        for (size_t index = 0; index < Readers_.size(); ++index) {
            if (!Readers_[index]->IsValid() && !BannedPartIndices_.test(index)) {
                failedReaderIndices.push_back(index);
            }
        }
    }

    if (failedReaderIndices.empty()) {
        return;
    }

    {
        TWriterGuard guard(IndicesLock_);
        for (auto index : failedReaderIndices) {
            BannedPartIndices_.set(index);
        }
    }
}

TPartIndexSet TRepairingReader::GetBannedIndices()
{
    TReaderGuard guard(IndicesLock_);
    return BannedPartIndices_;
}

void TRepairingReader::CheckSlowReaders()
{
    TWriterGuard guard(IndicesLock_);

    auto now = GetCpuInstant();
    for (size_t index = 0; index < SlowReaderBanTimes_.size(); ++index) {
        if (!SlowReaderBanTimes_[index]) {
            continue;
        }
        if (now >= SlowReaderBanTimes_[index] + DurationToCpuDuration(Config_->SlowReaderExpirationTimeout)) {
            SlowReaderBanTimes_[index] = TCpuInstant();
            if (Readers_[index]->IsValid()) {
                BannedPartIndices_.set(index, false);
            }
        }
    }
}

TError TRepairingReader::CheckPartReaderIsSlow(int partIndex, i64 bytesReceived, TDuration timePassed)
{
    double secondsPassed = timePassed.SecondsFloat();
    double speed = secondsPassed < SpeedComparisonPrecision ? 0.0 : static_cast<double>(bytesReceived) / secondsPassed;
    if (speed > Config_->ReplicationReaderSpeedLimitPerSec || timePassed < Config_->ReplicationReaderTimeout) {
        return TError();
    }

    {
        TWriterGuard guard(IndicesLock_);
        if (BannedPartIndices_.test(partIndex)) {
            return TError("Reader of part %v is already banned", partIndex);
        }
        BannedPartIndices_.set(partIndex);
        if (Codec_->CanRepair(BannedPartIndices_)) {
            SlowReaderBanTimes_[partIndex] = GetCpuInstant();
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

TRefCountedChunkMetaPtr TRepairingReader::GetMetaAsync(
    const TClientBlockReadOptions& options,
    std::optional<int> partitionTag,
    const std::optional<std::vector<int>>& extensionTags)
{
    std::vector<TError> errors;

    std::vector<int> indices(Readers_.size());
    std::iota(indices.begin(), indices.end(), 0);
    Shuffle(indices.begin(), indices.end());

    for (auto index : indices) {
        if (!Readers_[index]->IsValid()) {
            continue;
        }
        auto result = WaitFor(Readers_[index]->GetMeta(options, partitionTag, extensionTags));
        if (result.IsOK()) {
            return result.Value();
        }
        errors.push_back(result);
    }
    THROW_ERROR_EXCEPTION("Failed to get meta")
        << errors;
}

////////////////////////////////////////////////////////////////////////////////

IChunkReaderPtr CreateRepairingReader(
    ICodec* codec,
    TErasureReaderConfigPtr config,
    const std::vector<IChunkReaderAllowingRepairPtr>& readers,
    const TLogger& logger)
{
    return New<TRepairingReader>(
        codec,
        config,
        readers,
        logger);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
