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

    NLogging::TLogger Logger;

    const TMultiChunkReaderConfigPtr Config_;
    const TMultiChunkReaderOptionsPtr Options_;

    TSession CurrentSession_;
    std::vector<IReaderFactoryPtr> ReaderFactories_;

    TFuture<void> ReadyEvent_;
    TPromise<void> CompletionError_ = NewPromise<void>();

    virtual void OnReaderOpened(IReaderBasePtr chunkReader, int chunkIndex) = 0;

    virtual void OnReaderBlocked() = 0;

    virtual void OnReaderSwitched() = 0;

    virtual void OnReaderFinished();

    virtual void OnError();

    bool OnEmptyRead(bool readerFinished);

    void RegisterFailedReader(IReaderBasePtr reader);

protected:
    TSpinLock PrefetchLock_;
    int PrefetchIndex_ = 0;
    i64 FreeBufferSize_;

    TSpinLock FailedChunksLock_;
    yhash_set<TChunkId> FailedChunks_;

    std::atomic<int> OpenedReaderCount_ = { 0 };

    TSpinLock ActiveReadersLock_;
    NProto::TDataStatistics DataStatistics_;
    std::atomic<int> ActiveReaderCount_ = { 0 };
    yhash_set<IReaderBasePtr> ActiveReaders_;

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

private:
    int NextReaderIndex_ = 0;
    int FinishedReaderCount_ = 0;
    std::vector<TPromise<IReaderBasePtr>> NextReaders_;

    void DoOpen();

    virtual void OnReaderOpened(IReaderBasePtr chunkReader, int chunkIndex) override;

    virtual void OnReaderBlocked() override;

    virtual void OnReaderFinished() override;

    virtual void OnError() override;

    void WaitForNextReader();

    void WaitForCurrentReader();

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

private:
    typedef NConcurrency::TNonblockingQueue<TSession> TSessionQueue;

    TSessionQueue ReadySessions_;
    int FinishedReaderCount_ = 0;

    void DoOpen();

    virtual void OnReaderOpened(IReaderBasePtr chunkReader, int chunkIndex) override;

    virtual void OnReaderBlocked() override;

    virtual void OnReaderFinished() override;

    virtual void OnError() override;

    void WaitForReadyReader();

    void WaitForReader(TSession session);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
