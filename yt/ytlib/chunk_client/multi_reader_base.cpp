#include "stdafx.h"

#include "multi_reader_base.h"

#include "config.h"
#include "dispatcher.h"
#include "private.h"
#include "reader_factory.h"

#include <core/concurrency/scheduler.h>

namespace NYT {
namespace NChunkClient {

using namespace NConcurrency;
using namespace NProto;

////////////////////////////////////////////////////////////////////////////////

TMultiReaderBase::TMultiReaderBase(
    TMultiChunkReaderConfigPtr config,
    TMultiChunkReaderOptionsPtr options,
    const std::vector<IReaderFactoryPtr>& readerFactories)
    : Logger(ChunkClientLogger)
    , Config_(config)
    , Options_(options)
    , ReaderFactories_(readerFactories)
    , FreeBufferSize_(Config_->MaxBufferSize)
{
    Logger.AddTag("MultiReader: %v", this);

    CurrentSession_.Reset();

    LOG_DEBUG("Creating multi reader for %v readers",
        readerFactories.size());

    if (readerFactories.empty()) {
        CompletionError_.Set(TError());
        ReadyEvent_ = CompletionError_.ToFuture();
        return;
    }
}

TFuture<void> TMultiReaderBase::GetReadyEvent()
{
    return ReadyEvent_;
}

TDataStatistics TMultiReaderBase::GetDataStatistics() const
{
    TGuard<TSpinLock> guard(ActiveReadersLock_);
    auto dataStatistics = DataStatistics_;
    for (auto reader : ActiveReaders_) {
        dataStatistics += reader->GetDataStatistics();
    }
    return dataStatistics;
}

bool TMultiReaderBase::IsFetchingCompleted() const
{
    if (OpenedReaderCount_ == ReaderFactories_.size()) {
        TGuard<TSpinLock> guard(ActiveReadersLock_);
        for (auto reader : ActiveReaders_) {
            if (!reader->IsFetchingCompleted()) {
                return false;
            }
        }
    }
    return true;
}

std::vector<TChunkId> TMultiReaderBase::GetFailedChunkIds() const
{
    TGuard<TSpinLock> guard(FailedChunksLock_);
    return std::vector<TChunkId>(FailedChunks_.begin(), FailedChunks_.end());
}

void TMultiReaderBase::OpenNextChunks()
{
    TGuard<TSpinLock> guard(PrefetchLock_);
    for (; PrefetchIndex_ < ReaderFactories_.size(); ++PrefetchIndex_) {
        if (ReaderFactories_[PrefetchIndex_]->GetMemoryFootprint() > FreeBufferSize_ &&
            ActiveReaderCount_ > 0 &&
            !Options_->KeepInMemory)
        {
            return;
        }

        if (ActiveReaderCount_ > MaxPrefetchWindow) {
            return;
        }

        ++ActiveReaderCount_;
        FreeBufferSize_ -= ReaderFactories_[PrefetchIndex_]->GetMemoryFootprint();
        BIND(
            &TMultiReaderBase::DoOpenReader,
            MakeWeak(this),
            PrefetchIndex_)
        .Via(TDispatcher::Get()->GetReaderInvoker())
        .Run();
    }
}

void TMultiReaderBase::DoOpenReader(int index)
{
    if (CompletionError_.IsSet())
        return;

    LOG_DEBUG("Opening reader (Index: %v)", index);

    auto reader = ReaderFactories_[index]->CreateReader();
    auto error = WaitFor(reader->GetReadyEvent());

    if (!error.IsOK()) {
        RegisterFailedReader(reader);
        CompletionError_.TrySet(error);
    }

    if (CompletionError_.IsSet())
        return;

    OnReaderOpened(reader, index);

    TGuard<TSpinLock> guard(ActiveReadersLock_);
    YCHECK(ActiveReaders_.insert(reader).second);
}

void TMultiReaderBase::OnReaderFinished()
{
    if (Options_->KeepInMemory) {
        FinishedReaders_.push_back(CurrentSession_.Reader);
    }

    {
        TGuard<TSpinLock> guard(ActiveReadersLock_);
        DataStatistics_ += CurrentSession_.Reader->GetDataStatistics();
        YCHECK(ActiveReaders_.erase(CurrentSession_.Reader));
    }

    --ActiveReaderCount_;
    FreeBufferSize_ += ReaderFactories_[CurrentSession_.Index]->GetMemoryFootprint();

    CurrentSession_.Reset();
    OpenNextChunks();
}

bool TMultiReaderBase::OnEmptyRead(bool readerFinished)
{
    if (readerFinished) {
        OnReaderFinished();
        return !CompletionError_.IsSet() || !CompletionError_.Get().IsOK();
    } else {
        OnReaderBlocked();
        return true;
    }
}

void TMultiReaderBase::OnError()
{ }

TFuture<void> TMultiReaderBase::CombineCompletionError(TFuture<void> future)
{
    auto promise = NewPromise<void>();
    promise.TrySetFrom(CompletionError_.ToFuture());
    promise.TrySetFrom(future);
    return promise.ToFuture();
}

void TMultiReaderBase::RegisterFailedReader(IReaderBasePtr reader)
{   
    auto chunkIds = reader->GetFailedChunkIds();
    LOG_WARNING("Chunk reader failed (ChunkIds: [%v])", JoinToString(chunkIds));

    OnError();

    TGuard<TSpinLock> guard(FailedChunksLock_);
    for (const auto& chunkId : chunkIds) {
        FailedChunks_.insert(chunkId);
    }
}

////////////////////////////////////////////////////////////////////////////////

TSequentialMultiReaderBase::TSequentialMultiReaderBase(
    TMultiChunkReaderConfigPtr config,
    TMultiChunkReaderOptionsPtr options,
    const std::vector<IReaderFactoryPtr>& readerFactories)
    : TMultiReaderBase(
        config,
        options,
        readerFactories)
{
    LOG_DEBUG("Multi chunk reader is sequential");
    NextReaders_.reserve(ReaderFactories_.size());
    for (int i = 0; i < ReaderFactories_.size(); ++i) {
        NextReaders_.push_back(NewPromise<IReaderBasePtr>());
    }

    ReadyEvent_ = CombineCompletionError(BIND(
            &TSequentialMultiReaderBase::DoOpen, 
            MakeStrong(this))
        .AsyncVia(TDispatcher::Get()->GetReaderInvoker())
        .Run());
}

void TSequentialMultiReaderBase::DoOpen()
{
    OpenNextChunks();
    WaitForNextReader();
}

void TSequentialMultiReaderBase::OnReaderOpened(IReaderBasePtr chunkReader, int chunkIndex)
{
    // May have already been set in case of error.
    NextReaders_[chunkIndex].TrySet(chunkReader);
}

void TSequentialMultiReaderBase::OnReaderBlocked()
{
    ReadyEvent_ = CombineCompletionError(BIND(
        &TSequentialMultiReaderBase::WaitForCurrentReader,
        MakeStrong(this))
    .AsyncVia(TDispatcher::Get()->GetReaderInvoker())
    .Run());
}

void TSequentialMultiReaderBase::OnReaderFinished()
{
    TMultiReaderBase::OnReaderFinished();

    ++FinishedReaderCount_;
    if (FinishedReaderCount_ == ReaderFactories_.size()) {
        CompletionError_.TrySet(TError());
        return;
    }

    ReadyEvent_ = CombineCompletionError(BIND(
        &TSequentialMultiReaderBase::WaitForNextReader,
        MakeStrong(this))
    .AsyncVia(TDispatcher::Get()->GetReaderInvoker())
    .Run());
}

void TSequentialMultiReaderBase::WaitForNextReader()
{
    if (NextReaderIndex_ == ReaderFactories_.size()) {
        return;
    }

    CurrentSession_.Index = NextReaderIndex_;
    CurrentSession_.Reader = WaitFor(NextReaders_[NextReaderIndex_].ToFuture())
        .ValueOrThrow();

    ++NextReaderIndex_;

    // Avoid memory leaks, drop smart pointer reference.
    NextReaders_[CurrentSession_.Index].Reset();

    OnReaderSwitched();
}

void TSequentialMultiReaderBase::WaitForCurrentReader()
{
    auto error = WaitFor(CurrentSession_.Reader->GetReadyEvent());
    if (!error.IsOK()) {
        RegisterFailedReader(CurrentSession_.Reader);
        CompletionError_.TrySet(error);
    }
}

void TSequentialMultiReaderBase::OnError()
{
    BIND([=, this_ = MakeStrong(this)] () {
        // This is to avoid infinite waiting and memory leaks.
        for (int i = NextReaderIndex_; i < NextReaders_.size(); ++i) {
            NextReaders_[i].Reset();
        }
        NextReaderIndex_ = NextReaders_.size();
    })
    .Via(TDispatcher::Get()->GetReaderInvoker())
    .Run();
}

////////////////////////////////////////////////////////////////////////////////

TParallelMultiReaderBase::TParallelMultiReaderBase(
    TMultiChunkReaderConfigPtr config,
    TMultiChunkReaderOptionsPtr options,
    const std::vector<IReaderFactoryPtr>& readerFactories)
    : TMultiReaderBase(
        config,
        options,
        readerFactories)
{
    LOG_DEBUG("Multi chunk reader is parallel");

    ReadyEvent_ = CombineCompletionError(BIND(
            &TParallelMultiReaderBase::DoOpen, 
            MakeStrong(this))
        .AsyncVia(TDispatcher::Get()->GetReaderInvoker())
        .Run());
}

void TParallelMultiReaderBase::DoOpen()
{
    OpenNextChunks();
    WaitForReadyReader();
}

void TParallelMultiReaderBase::OnReaderOpened(IReaderBasePtr chunkReader, int chunkIndex)
{
    TSession session;
    session.Reader = chunkReader;
    session.Index = chunkIndex;

    ReadySessions_.Enqueue(session);
}

void TParallelMultiReaderBase::OnReaderBlocked()
{
    BIND(
        &TParallelMultiReaderBase::WaitForReader,
        MakeStrong(this),
        CurrentSession_)
    .AsyncVia(TDispatcher::Get()->GetReaderInvoker())
    .Run();

    CurrentSession_.Reset();

    ReadyEvent_ = CombineCompletionError(BIND(
        &TParallelMultiReaderBase::WaitForReadyReader,
        MakeStrong(this))
    .AsyncVia(TDispatcher::Get()->GetReaderInvoker())
    .Run());
}

void TParallelMultiReaderBase::OnReaderFinished()
{
    TMultiReaderBase::OnReaderFinished();

    ++FinishedReaderCount_;
    if (FinishedReaderCount_ == ReaderFactories_.size()) {
        ReadySessions_.Enqueue(TError("Sentinel session"));
        CompletionError_.TrySet(TError());
    } else {
        ReadyEvent_ = CombineCompletionError(BIND(
            &TParallelMultiReaderBase::WaitForReadyReader,
            MakeStrong(this))
        .AsyncVia(TDispatcher::Get()->GetReaderInvoker())
        .Run());
    }
}

void TParallelMultiReaderBase::OnError()
{
    // Someone may wait for this future.
    ReadySessions_.Enqueue(TError("Sentinel session"));
}

void TParallelMultiReaderBase::WaitForReadyReader()
{
    auto asyncReadySession = ReadySessions_.Dequeue();
    CurrentSession_ = WaitFor(asyncReadySession)
        .ValueOrThrow();

    OnReaderSwitched();
}

void TParallelMultiReaderBase::WaitForReader(TSession session)
{
    auto error = WaitFor(session.Reader->GetReadyEvent());
    if (error.IsOK()) {
        ReadySessions_.Enqueue(session);
        return;
    }

    RegisterFailedReader(session.Reader);
    CompletionError_.TrySet(error);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
