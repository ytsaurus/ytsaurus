#pragma once

#include "public.h"
#include "data_statistics.h"
#include "reader_base.h"

#include <yt/ytlib/api/public.h>

#include <yt/ytlib/chunk_client/chunk_spec.pb.h>

#include <yt/ytlib/node_tracker_client/public.h>

#include <yt/core/concurrency/nonblocking_queue.h>

#include <yt/core/logging/log.h>

#include <yt/core/rpc/public.h>

namespace NYT {
namespace NChunkClient {

////////////////////////////////////////////////////////////////////////////////

class TMultiReaderBase
    : public virtual IReaderBase
{
public:
    TMultiReaderBase(
        TMultiChunkReaderConfigPtr config,
        TMultiChunkReaderOptionsPtr options,
        const std::vector<IReaderFactoryPtr>& readerFactories);

    void Open();

    virtual TFuture<void> GetReadyEvent() override;

    virtual NProto::TDataStatistics GetDataStatistics() const override;

    virtual std::vector<TChunkId> GetFailedChunkIds() const override;

    virtual bool IsFetchingCompleted() const override;

protected:
    struct TSession
    {
        IReaderBasePtr Reader;
        int Index = -1;

        void Reset()
        {
            Reader.Reset();
            Index = -1;
        }
    };

    struct TUnreadState
    {
        IReaderBasePtr CurrentReader;
        std::vector<IReaderBasePtr> ActiveReaders;
        std::vector<IReaderFactoryPtr> ReaderFactories;
    };

    const TMultiChunkReaderConfigPtr Config_;
    const TMultiChunkReaderOptionsPtr Options_;

    const NLogging::TLogger Logger;

    TSession CurrentSession_;
    std::vector<IReaderFactoryPtr> ReaderFactories_;

    TFuture<void> ReadyEvent_;
    TPromise<void> CompletionError_ = NewPromise<void>();

    virtual void OnReaderOpened(IReaderBasePtr chunkReader, int chunkIndex) = 0;

    virtual void OnReaderBlocked() = 0;

    virtual void OnReaderSwitched() = 0;

    virtual void OnReaderFinished();

    virtual void DoOpen() = 0;

    bool OnEmptyRead(bool readerFinished);

    void RegisterFailedReader(IReaderBasePtr reader);

protected:
    virtual void OnInterrupt();

    virtual TUnreadState GetUnreadState() const = 0;

    TSpinLock PrefetchLock_;
    int PrefetchIndex_ = 0;
    i64 FreeBufferSize_;

    TSpinLock FailedChunksLock_;
    THashSet<TChunkId> FailedChunks_;

    std::atomic<int> OpenedReaderCount_ = { 0 };

    TSpinLock ActiveReadersLock_;
    NProto::TDataStatistics DataStatistics_;
    std::atomic<int> ActiveReaderCount_ = { 0 };
    THashSet<IReaderBasePtr> ActiveReaders_;
    THashSet<int> NonOpenedReaderIndexes_;

    // If KeepInMemory option is set, we store here references to finished readers.
    std::vector<IReaderBasePtr> FinishedReaders_;

    TFuture<void> CombineCompletionError(TFuture<void> future);

    void OpenNextChunks();
    void DoOpenReader(int index);

};

////////////////////////////////////////////////////////////////////////////////

class TSequentialMultiReaderBase
    : public TMultiReaderBase
{
public:
    TSequentialMultiReaderBase(
        TMultiChunkReaderConfigPtr config,
        TMultiChunkReaderOptionsPtr options,
        const std::vector<IReaderFactoryPtr>& readerFactories);

protected:
    virtual TUnreadState GetUnreadState() const override;

private:
    int NextReaderIndex_ = 0;
    int FinishedReaderCount_ = 0;
    std::vector<TPromise<IReaderBasePtr>> NextReaders_;

    virtual void DoOpen() override;

    virtual void OnReaderOpened(IReaderBasePtr chunkReader, int chunkIndex) override;

    virtual void OnReaderBlocked() override;

    virtual void OnReaderFinished() override;

    void WaitForNextReader();

    void WaitForCurrentReader();

    void PropagateError(const TError& error);

};

////////////////////////////////////////////////////////////////////////////////

class TParallelMultiReaderBase
    : public TMultiReaderBase
{
public:
    TParallelMultiReaderBase(
        TMultiChunkReaderConfigPtr config,
        TMultiChunkReaderOptionsPtr options,
        const std::vector<IReaderFactoryPtr>& readerFactories);

protected:
    virtual TUnreadState GetUnreadState() const override;

private:
    typedef NConcurrency::TNonblockingQueue<TSession> TSessionQueue;

    TSessionQueue ReadySessions_;
    int FinishedReaderCount_ = 0;

    virtual void DoOpen() override;

    virtual void OnReaderOpened(IReaderBasePtr chunkReader, int chunkIndex) override;

    virtual void OnReaderBlocked() override;

    virtual void OnReaderFinished() override;

    void WaitForReadyReader();

    void WaitForReader(TSession session);

    void PropagateError(const TError& error);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
