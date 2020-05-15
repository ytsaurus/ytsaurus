#pragma once

#include "multi_reader_manager.h"
#include "config.h"
#include "dispatcher.h"
#include "parallel_reader_memory_manager.h"
#include "private.h"
#include "reader_factory.h"
#include "data_slice_descriptor.h"

#include <yt/core/concurrency/scheduler.h>
#include <yt/core/concurrency/action_queue.h>

namespace NYT::NChunkClient {

////////////////////////////////////////////////////////////////////////////////

class TMultiReaderManagerBase
    : public IMultiReaderManager
{
public:
    TMultiReaderManagerBase(
        TMultiChunkReaderConfigPtr config,
        TMultiChunkReaderOptionsPtr options,
        const std::vector<IReaderFactoryPtr>& readerFactories,
        IMultiReaderMemoryManagerPtr multiReaderMemoryManager);

    void Open() override;

    TFuture<void> GetReadyEvent() override;

    NProto::TDataStatistics GetDataStatistics() const override;

    TCodecStatistics GetDecompressionStatistics() const override;

    std::vector<TChunkId> GetFailedChunkIds() const override;

    bool IsFetchingCompleted() const override;

    const NLogging::TLogger& GetLogger() const override;

    TMultiReaderManagerSession& GetCurrentSession() override;

    bool OnEmptyRead(bool readerFinished) override;

    void RegisterFailedReader(IReaderBasePtr reader) override;

    virtual void Interrupt() override;

    virtual TMultiReaderManagerUnreadState GetUnreadState() const override = 0;

    DEFINE_SIGNAL(void(), ReaderSwitched);

protected:
    const TGuid Id_;
    const TMultiChunkReaderConfigPtr Config_;
    const TMultiChunkReaderOptionsPtr Options_;
    const std::vector<IReaderFactoryPtr> ReaderFactories_;
    const IMultiReaderMemoryManagerPtr MultiReaderMemoryManager_;

    const NLogging::TLogger Logger;

    TMultiReaderManagerSession CurrentSession_;

    TFuture<void> ReadyEvent_;
    TPromise<void> CompletionError_ = NewPromise<void>();
    TFuture<void> UncancelableCompletionError_;

    IInvokerPtr ReaderInvoker_;

    TSpinLock PrefetchLock_;
    int PrefetchIndex_ = 0;

    TSpinLock FailedChunksLock_;
    THashSet<TChunkId> FailedChunks_;

    std::atomic<int> OpenedReaderCount_ = { 0 };

    TSpinLock ActiveReadersLock_;
    NProto::TDataStatistics DataStatistics_;
    TCodecStatistics DecompressionStatistics_;
    std::atomic<int> ActiveReaderCount_ = { 0 };
    THashSet<IReaderBasePtr> ActiveReaders_;
    THashSet<int> NonOpenedReaderIndexes_;

    // If KeepInMemory option is set, we store here references to finished readers.
    std::vector<IReaderBasePtr> FinishedReaders_;

    TFuture<void> CombineCompletionError(TFuture<void> future);

    void OpenNextChunks();

    void DoOpenReader(int index);

    virtual void OnReaderOpened(IReaderBasePtr chunkReader, int chunkIndex) = 0;

    virtual void OnReaderBlocked() = 0;

    virtual void OnReaderFinished();

    virtual void DoOpen() = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
