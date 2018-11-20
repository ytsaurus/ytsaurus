#pragma once

#ifndef PARALLEL_READER_INL_H_
#error "Direct inclusion of this file is not allowed, use parallel_reader.h"
#endif
#undef PARALLEL_READER_INL_H_

#include <mapreduce/yt/interface/client.h>
#include <mapreduce/yt/interface/io.h>
#include <mapreduce/yt/interface/mpl.h>

#include <mapreduce/yt/interface/logging/log.h>

#include <library/threading/blocking_queue/blocking_queue.h>

#include <util/generic/maybe.h>
#include <util/generic/ptr.h>
#include <util/generic/queue.h>
#include <util/generic/scope.h>
#include <util/generic/yexception.h>

#include <util/random/shuffle.h>

#include <util/string/builder.h>

#include <util/system/thread.h>

namespace NYT {
namespace NDetail {

////////////////////////////////////////////////////////////////////////////////

//
// Interface of table reader producer. Methods are not requested to be thread-safe.
template <typename T>
class ITableReaderFactory
    : public TThrRefBase
{
public:
    virtual TTableReaderPtr<T> GetReader() = 0;
    virtual bool IsValid() const = 0;
    virtual void Next() = 0;
};

template <typename T>
using ITableReaderFactoryPtr = ::TIntrusivePtr<ITableReaderFactory<T>>;

////////////////////////////////////////////////////////////////////////////////

//
// Implementation of ITableReaderFactory that produces readers of (almost) equal slices.
template <typename T>
class TEqualRangeTableReaderFactory
    : public ITableReaderFactory<T>
{
public:
    TEqualRangeTableReaderFactory(
        IClientBasePtr client,
        const TRichYPath& path,
        size_t rangeSize,
        bool ordered,
        const TTableReaderOptions& options)
        : Client_(std::move(client))
        , Path_(Client_->CanonizeYPath(path))
        , Options_(options)
    {
        Y_ENSURE(!Path_.Ranges_.empty());
        for (const auto& range : Path_.Ranges_) {
            Y_ENSURE(range.LowerLimit_.RowIndex_.Defined(), "Lower limit must be specified as row index");
            Y_ENSURE(range.UpperLimit_.RowIndex_.Defined(), "Upper limit must be specified as row index");
            i64 lowerLimit = *range.LowerLimit_.RowIndex_;
            while (lowerLimit < *range.UpperLimit_.RowIndex_) {
                i64 upperLimit = std::min<i64>(lowerLimit + rangeSize, *range.UpperLimit_.RowIndex_);
                Ranges_.emplace_back(lowerLimit, upperLimit);
                lowerLimit = upperLimit;
            }
        }
        if (!ordered) {
            // Shuffle range to facilitate reading
            // from different chunks and thus from different nodes.
            Shuffle(Ranges_.begin(), Ranges_.end());
        }
        RangeIter_ = Ranges_.begin();
    }

    TTableReaderPtr<T> GetReader() override
    {
        CheckValidity();

        auto path = Path_;
        path.Ranges_.assign(1, TReadRange::FromRowIndices(RangeIter_->first, RangeIter_->second));
        return Client_->CreateTableReader<T>(path, Options_);
    }

    bool IsValid() const override
    {
        return RangeIter_ != Ranges_.end();
    }

    void Next() override
    {
        CheckValidity();
        ++RangeIter_;
    }

private:
    const IClientBasePtr Client_;
    TRichYPath Path_;
    TVector<std::pair<i64, i64>> Ranges_;
    TVector<std::pair<i64, i64>>::const_iterator RangeIter_;
    const TTableReaderOptions Options_;

private:
    void CheckValidity() const
    {
        Y_ENSURE(IsValid(), "Factory is invalid");
    }
};

////////////////////////////////////////////////////////////////////////////////

template <typename T>
struct TReaderBuffer
{
    TVector<T> Rows;
    ui64 FirstRowIndex = 0;
    size_t SequenceNumber = 0;
};

//
// Table reader reading the rows from given buffer.
template <typename TDerived, typename T>
class TBufferTableReaderBase
    : public TDerived
{
public:
    TBufferTableReaderBase(TReaderBuffer<T>* buffer)
        : Buffer_(buffer)
        , Iterator_(Buffer_->Rows.begin())
    { }

    bool IsValid() const override
    {
        return Iterator_ != Buffer_->Rows.end();
    }

    void Next() override
    {
        CheckValidity();
        ++Iterator_;
    }

    ui32 GetTableIndex() const override
    {
        Y_FAIL("Not implemented as this reader is intended for use on client");
    }

    ui64 GetRowIndex() const override
    {
        CheckValidity();
        return Buffer_->FirstRowIndex + (Iterator_ - Buffer_->Rows.begin());
    }

    void NextKey() override
    {
        Y_FAIL("Not implemented as this reader is intended for use on client");
    }

protected:
    TReaderBuffer<T>* const Buffer_;
    typename TVector<T>::iterator Iterator_;

protected:
    void CheckValidity() const
    {
        Y_ENSURE(IsValid(), "Iterator is not valid");
    }
};

template <typename T, typename = void>
class TBufferTableReader;

template <>
class TBufferTableReader<TNode>
    : public TBufferTableReaderBase<INodeReaderImpl, TNode>
{
public:
    using TBufferTableReaderBase<INodeReaderImpl, TNode>::TBufferTableReaderBase;

    const TNode& GetRow() const override
    {
        CheckValidity();
        return *Iterator_;
    }

    void MoveRow(TNode* row) override
    {
        CheckValidity();
        *row = std::move(*Iterator_);
    }
};

template <typename T>
class TBufferTableReader<
    T,
    std::enable_if_t<TIsBaseOf<Message, T>::Value>
>
    : public TBufferTableReaderBase<IProtoReaderImpl, T>
{
public:
    using TBufferTableReaderBase<IProtoReaderImpl, T>::TBufferTableReaderBase;

    void ReadRow(Message* row) override
    {
        this->CheckValidity();
        static_cast<T&>(*row).Swap(this->Iterator_);
    }
};

////////////////////////////////////////////////////////////////////////////////

template <typename T>
struct TGreaterBySequenceNumber
{
    bool operator()(const TReaderBuffer<T>& left, const TReaderBuffer<T>& right)
    {
        return left.SequenceNumber > right.SequenceNumber;
    }
};

//
// Base class for table reader using several threads to read the table in parallel.
// Readers are produced by the passed factory.
template <typename TDerived, typename T>
class TParallelTableReaderBase
    : public TDerived
{
public:
    using TRowType = T;

public:
    TParallelTableReaderBase(
        bool ordered,
        ITableReaderFactoryPtr<TRowType> factory,
        size_t threadCount,
        size_t bufferSize)
        : Ordered_(ordered)
        , Tasks_(0)
        , FilledBuffers_(0)
        , Factory_(std::move(factory))
    {
        if (Factory_->IsValid()) {
            CurrentReader_ = Factory_->GetReader();
            Factory_->Next();
        }

        Y_ENSURE(threadCount >= 1);

        RunningThreadCount_ = threadCount;
        for (size_t i = 0; i < threadCount; ++i) {
            TString threadName = TStringBuilder() << "par_reader_" << i;
            Threads_.push_back(::MakeHolder<TThread>(
                TThread::TParams(ReaderThread, this).SetName(threadName)));
            Threads_.back()->Start();
        }

        for (size_t i = 0; i < threadCount; ++i) {
            if (!Factory_->IsValid()) {
                break;
            }
            auto reader = Factory_->GetReader();
            Factory_->Next();
            TReaderBuffer<TRowType> buffer;
            buffer.Rows.reserve(bufferSize);
            buffer.SequenceNumber = NextInputSequenceNumber_++;
            if (reader->IsValid()) {
                buffer.FirstRowIndex = reader->GetRowIndex();
            }
            Tasks_.Push({reader, std::move(buffer)});
        }

    }

    ~TParallelTableReaderBase()
    {
        Tasks_.Stop();
        FilledBuffers_.Stop();
        for (auto& thread : Threads_) {
            thread->Join();
        }
    }

    bool IsValid() const override
    {
        return CurrentReader_ != nullptr && CurrentReader_->IsValid();
    }

    void Next() override
    {
        CheckValidity();

        CurrentReader_->Next();
        if (CurrentReader_->IsValid()) {
            return;
        }

        // Reset current reader pointer before dealing with CurrentBuffer_
        // to avoid use-after-free in TBufferReader.
        CurrentReader_.Reset();

        if (CurrentBuffer_ && Factory_->IsValid()) {
            // Buffer is freed, prepare the next task.
            auto reader = Factory_->GetReader();
            Factory_->Next();
            CurrentBuffer_->SequenceNumber = NextInputSequenceNumber_++;
            if (reader->IsValid()) {
                CurrentBuffer_->FirstRowIndex = reader->GetRowIndex();
            }
            bool pushed = Tasks_.Push({std::move(reader), std::move(*CurrentBuffer_)});
            Y_ENSURE(pushed, "Task queue can be stopped only by the main thread");
            CurrentBuffer_.Clear();
        }

        if (!Factory_->IsValid() && Tasks_.Empty()) {
            Tasks_.Stop();
        }

        if (Ordered_) {
            OrderedNext();
        } else {
            UnorderedNext();
        }
    }

    ui32 GetTableIndex() const override
    {
        Y_FAIL("Not implemented as this reader is intended for use on client");
    }

    ui64 GetRowIndex() const override
    {
        CheckValidity();
        return CurrentReader_->GetRowIndex();
    }

    void NextKey() override
    {
        Y_FAIL("Not implemented as this reader is intended for use on client");
    }

protected:
    TTableReaderPtr<TRowType> CurrentReader_;

protected:
    void CheckValidity() const
    {
        Y_ENSURE(IsValid(), "Iterator is invalid");
    }

private:
    using TTask = std::pair<TTableReaderPtr<TRowType>, TReaderBuffer<TRowType>>;

private:
    const bool Ordered_ = true;
    TMaybe<TReaderBuffer<TRowType>> CurrentBuffer_;
    TVector<THolder<TThread>> Threads_;
    int RunningThreadCount_ = 0;

    NThreading::TBlockingQueue<TTask> Tasks_;
    NThreading::TBlockingQueue<TReaderBuffer<TRowType>> FilledBuffers_;

    ITableReaderFactoryPtr<T> Factory_;

    TPriorityQueue<
        TReaderBuffer<TRowType>,
        TVector<TReaderBuffer<TRowType>>,
        TGreaterBySequenceNumber<TRowType>
    > ExtractedFilledBuffers_;

    size_t NextInputSequenceNumber_ = 0;
    size_t NextOutputSequenceNumber_ = 0;

    TMutex Lock_;
    TMaybe<yexception> Exception_;

private:
    void UnorderedNext()
    {
        if (FilledBuffers_.Empty() && Factory_->IsValid()) {
            CurrentReader_ = Factory_->GetReader();
            Factory_->Next();
            return;
        }

        CurrentBuffer_ = FilledBuffers_.Pop();
        if (!CurrentBuffer_) {
            TGuard<TMutex> guard(Lock_);
            if (Exception_) {
                throw *Exception_;
            }
            return;
        }
        auto bufferReader = ::MakeIntrusive<TBufferTableReader<TRowType>>(&*CurrentBuffer_);
        CurrentReader_.Reset(new TTableReader<TRowType>(std::move(bufferReader)));
    }

    void OrderedNext()
    {
        if (!ExtractedFilledBuffers_.empty() &&
            ExtractedFilledBuffers_.top().SequenceNumber == NextOutputSequenceNumber_)
        {
            CurrentBuffer_ = std::move(ExtractedFilledBuffers_.top());
            ExtractedFilledBuffers_.pop();
        } else {
            while (true) {
                auto maybeBuffer = FilledBuffers_.Pop();
                if (!maybeBuffer) {
                    TGuard<TMutex> guard(Lock_);
                    if (Exception_) {
                        throw *Exception_;
                    }
                    return;
                }
                if (maybeBuffer->SequenceNumber == NextOutputSequenceNumber_) {
                    CurrentBuffer_ = std::move(*maybeBuffer);
                    break;
                } else {
                    ExtractedFilledBuffers_.push(std::move(*maybeBuffer));
                }
            }
        }
        ++NextOutputSequenceNumber_;

        auto bufferReader = ::MakeIntrusive<TBufferTableReader<TRowType>>(&*CurrentBuffer_);
        CurrentReader_.Reset(new TTableReader<TRowType>(std::move(bufferReader)));
    }

    void ReaderThread()
    {
        try {
            while (true) {
                auto maybeTask = Tasks_.Pop();
                if (!maybeTask) {
                    break;
                }
                auto& [reader, buffer] = *maybeTask;

                // Don't clear the buffer to avoid unnecessary calls of destructors
                // and memory allocation/deallocation.
                size_t index = 0;
                for (; reader->IsValid(); reader->Next()) {
                    if (index >= buffer.Rows.size()) {
                        buffer.Rows.push_back(reader->MoveRow());
                    } else {
                        reader->MoveRow(&buffer.Rows[index]);
                    }
                    ++index;
                }
                buffer.Rows.resize(index);

                if (!FilledBuffers_.Push(std::move(buffer))) {
                    break;
                }
            }
        } catch (const yexception& exception) {
            TGuard<TMutex> guard(Lock_);
            Exception_ = exception;
            FilledBuffers_.Stop();
        }
        {
            TGuard<TMutex> guard(Lock_);
            if (RunningThreadCount_ == 1) {
                FilledBuffers_.Stop();
            }
            --RunningThreadCount_;
        }
    }

    static void* ReaderThread(void* opaque)
    {
        static_cast<TParallelTableReaderBase<TDerived, T>*>(opaque)->ReaderThread();
        return nullptr;
    }
};

template <typename T, typename = void>
class TParallelTableReader;

template <>
class TParallelTableReader<TNode>
    : public TParallelTableReaderBase<INodeReaderImpl, TNode>
{
public:
    using TParallelTableReaderBase<INodeReaderImpl, TNode>::TParallelTableReaderBase;

    const TNode& GetRow() const override
    {
        return CurrentReader_->GetRow();
    }

    void MoveRow(TNode* row) override
    {
        *row = CurrentReader_->MoveRow();
    }
};

template <typename T>
class TParallelTableReader<
    T,
    std::enable_if_t<TIsBaseOf<Message, T>::Value>
>
    : public TParallelTableReaderBase<IProtoReaderImpl, T>
{
public:
    using TParallelTableReaderBase<IProtoReaderImpl, T>::TParallelTableReaderBase;

    void ReadRow(Message* row) override
    {
        this->CurrentReader_->MoveRow(static_cast<T*>(row));
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

template <typename T>
TTableReaderPtr<T> CreateParallelTableReader(
    const IClientBasePtr& client,
    const TRichYPath& path,
    const TParallelTableReaderOptions& options)
{
    IClientBasePtr rangeReaderClient;
    auto rangeReaderPath = path;

    if (options.CreateTransaction_) {
        auto tx = client->StartTransaction();
        auto lock = tx->Lock(path.Path_, ELockMode::LM_SNAPSHOT);
        rangeReaderPath.Path("#" + GetGuidAsString(lock->GetLockedNodeId()));
        rangeReaderClient = std::move(tx);
    } else {
        rangeReaderClient = client;
    }

    if (rangeReaderPath.Ranges_.empty()) {
        auto rowCount = rangeReaderClient->Get(rangeReaderPath.Path_ + "/@row_count").AsInt64();
        rangeReaderPath.Ranges_.push_back(
            TReadRange()
                .LowerLimit(TReadLimit().RowIndex(0))
                .UpperLimit(TReadLimit().RowIndex(rowCount)));
    }

    auto rangeReaderOptions = TTableReaderOptions().CreateTransaction(false);
    rangeReaderOptions.Config_ = options.Config_;
    rangeReaderOptions.FormatHints_ = options.FormatHints_;

    Y_ENSURE_EX(options.ThreadCount_ >= 1, TApiUsageError() << "ThreadCount can not be zero");
    auto rangeSize = options.BufferedRowCountLimit_ / options.ThreadCount_;
    auto factory = ::MakeIntrusive<NDetail::TEqualRangeTableReaderFactory<T>>(
        std::move(rangeReaderClient),
        std::move(rangeReaderPath),
        rangeSize,
        options.Ordered_,
        rangeReaderOptions);

    return ::MakeIntrusive<TTableReader<T>>(
        new NDetail::TParallelTableReader<T>(
            options.Ordered_,
            std::move(factory),
            options.ThreadCount_,
            rangeSize));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
