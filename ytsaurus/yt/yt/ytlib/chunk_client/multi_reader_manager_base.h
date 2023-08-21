#pragma once

#include "multi_reader_manager.h"
#include "config.h"
#include "dispatcher.h"
#include "parallel_reader_memory_manager.h"
#include "private.h"
#include "reader_factory.h"
#include "data_slice_descriptor.h"

#include <yt/yt/core/concurrency/scheduler.h>
#include <yt/yt/core/concurrency/action_queue.h>

namespace NYT::NChunkClient {

////////////////////////////////////////////////////////////////////////////////

class TMultiReaderManagerBase
    : public IMultiReaderManager
    , public TReadyEventReaderBase
{
public:
    TMultiReaderManagerBase(
        TMultiChunkReaderConfigPtr config,
        TMultiChunkReaderOptionsPtr options,
        std::vector<IReaderFactoryPtr> readerFactories,
        IMultiReaderMemoryManagerPtr multiReaderMemoryManager);

    ~TMultiReaderManagerBase();

    void Open() override;

    NProto::TDataStatistics GetDataStatistics() const override;
    TCodecStatistics GetDecompressionStatistics() const override;
    NTableClient::TTimingStatistics GetTimingStatistics() const override;

    std::vector<TChunkId> GetFailedChunkIds() const override;

    bool IsFetchingCompleted() const override;

    const NLogging::TLogger& GetLogger() const override;

    TMultiReaderManagerSession& GetCurrentSession() override;

    bool OnEmptyRead(bool readerFinished) override;

    void RegisterFailedReader(const IReaderBasePtr& reader) override;

    void Interrupt() override;

    TMultiReaderManagerUnreadState GetUnreadState() const override = 0;

    DEFINE_SIGNAL_OVERRIDE(void(), ReaderSwitched);

protected:
    const TGuid Id_;
    const TMultiChunkReaderConfigPtr Config_;
    const TMultiChunkReaderOptionsPtr Options_;
    const std::vector<IReaderFactoryPtr> ReaderFactories_;
    const IMultiReaderMemoryManagerPtr MultiReaderMemoryManager_;

    const NLogging::TLogger Logger;
    const TPromise<void> CompletionError_ = NewPromise<void>();
    const TFuture<void> UncancelableCompletionError_;
    const IInvokerPtr ReaderInvoker_;

    TMultiReaderManagerSession CurrentSession_;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, PrefetchLock_);
    int PrefetchIndex_ = 0;
    bool CreatingReader_ = false;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, FailedChunksLock_);
    THashSet<TChunkId> FailedChunks_;

    std::atomic<int> OpenedReaderCount_ = 0;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, ActiveReadersLock_);

    NProto::TDataStatistics DataStatistics_;
    TCodecStatistics DecompressionStatistics_;

    std::atomic<int> ActiveReaderCount_ = 0;
    THashSet<IReaderBasePtr> ActiveReaders_;
    THashSet<int> NonOpenedReaderIndexes_;

    // If KeepInMemory option is set, we store here references to finished readers.
    std::vector<IReaderBasePtr> FinishedReaders_;

    TFuture<void> CombineCompletionError(TFuture<void> future);

    NProfiling::TWallTimer TotalTimer_;

    void OpenNextReaders();
    void OnNextReaderCreated(const TError& error);
    void DoCreateReader(int index);
    void OnReaderCreated(int index, const TErrorOr<IReaderBasePtr>& readerOrError);
    void OnReaderReady(const IReaderBasePtr& reader, int index, const TError& error);

    virtual void OnReaderOpened(const IReaderBasePtr& chunkReader, int chunkIndex) = 0;
    virtual void OnReaderBlocked() = 0;
    virtual void OnReaderFinished();

    virtual TFuture<void> DoOpen() = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
