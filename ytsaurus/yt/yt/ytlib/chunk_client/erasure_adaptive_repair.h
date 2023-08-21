#pragma once

#include "public.h"

#include "chunk_reader_allowing_repair.h"

#include <yt/yt/core/actions/callback.h>
#include <yt/yt/core/concurrency/periodic_executor.h>
#include <yt/yt/core/logging/log.h>

#include <yt/yt/library/erasure/impl/public.h>

#include <vector>
#include <functional>

namespace NYT::NChunkClient {

////////////////////////////////////////////////////////////////////////////////

class TRepairingReadersObserver
    : public TRefCounted
{
public:
    TRepairingReadersObserver(
        NErasure::ICodec* codec,
        TErasureReaderConfigPtr config,
        IInvokerPtr invoker,
        std::vector<IChunkReaderAllowingRepairPtr> readers);

    void UpdateBannedPartIndices();
    NErasure::TPartIndexSet GetBannedPartIndices();
    bool IsReaderRecentlyFailed(TInstant now, int index) const;

    const TErasureReaderConfigPtr& GetConfig() const;

private:
    const NErasure::ICodec* const Codec_;
    const TErasureReaderConfigPtr Config_;
    const std::vector<IChunkReaderAllowingRepairPtr> Readers_;

    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, IndicesLock_);
    NErasure::TPartIndexSet BannedPartIndices_;
    std::vector<TInstant> SlowReaderBanTimes_;

    NConcurrency::TPeriodicExecutorPtr CheckReadersForUnbanExecutor_;

    TError CheckPartReaderIsSlow(int partIndex, i64 bytesReceived, TDuration timePassed);
    void CheckReadersForUnban();
};

DECLARE_REFCOUNTED_TYPE(TRepairingReadersObserver)

////////////////////////////////////////////////////////////////////////////////

//! Combines direct reading of alive chunk parts with additional repair of temporarily unavailable parts.
class TAdaptiveErasureRepairingSession
    : public TRefCounted
{
public:
    struct TTarget
    {
        //! Presumably existing parts to read. Session will try to read this parts directly,
        //! and perform additional repairing attempts if some requested parts are not available.
        NErasure::TPartIndexList Existing;

        //! Target parts that known to be erased and should be repaired.
        //! Session will make only repairing attempts for this parts.
        NErasure::TPartIndexList Erased;
    };


    TAdaptiveErasureRepairingSession(
        TChunkId chunkId,
        const NErasure::ICodec* codec,
        TRepairingReadersObserverPtr observer,
        std::vector<IChunkReaderAllowingRepairPtr> readers,
        IInvokerPtr invoker,
        TTarget target,
        NLogging::TLogger logger,
        NProfiling::TCounter adaptivelyRepairedCounter = {});

    template <typename TResultType>
    using TDoRepairAttempt = std::function<TFuture<TResultType>(
        const NErasure::TPartIndexList& erasedParts,
        const std::vector<IChunkReaderAllowingRepairPtr>& availableReaders)>;

    template <typename TResultType>
    TFuture<TResultType> Run(TDoRepairAttempt<TResultType> doRepairAttempt);

private:
    const TChunkId ChunkId_;
    const NErasure::ICodec* const Codec_;
    const TRepairingReadersObserverPtr Observer_;
    const std::vector<IChunkReaderAllowingRepairPtr> Readers_;
    const IInvokerPtr Invoker_;
    const TTarget Target_;
    const NLogging::TLogger Logger;

    NProfiling::TCounter AdaptivelyRepairedCounter_;


    NErasure::TPartIndexSet CalculateBannedParts();

    NErasure::TPartIndexList ToReadersIndexList(NErasure::TPartIndexSet indexSet);

    template <typename TResultType>
    TResultType DoRun(TDoRepairAttempt<TResultType> doRepairAttempt);
};

////////////////////////////////////////////////////////////////////////////////

// Creates dummy object that returns error on reading attempt.
IChunkReaderAllowingRepairPtr CreateUnavailablePartReader(TChunkId chunkId);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient

#define ERASURE_ADAPTIVE_REPAIR_INL_H_
#include "erasure_adaptive_repair-inl.h"
#undef ERASURE_ADAPTIVE_REPAIR_INL_H_
