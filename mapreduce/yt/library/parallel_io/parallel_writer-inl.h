#pragma once

#ifndef PARALLEL_WRITER_INL_H_
#error "Direct inclusion of this file is not allowed, use parallel_writer.h"
#endif
#undef PARALLEL_WRITER_INL_H_

#include <mapreduce/yt/interface/client.h>
#include <mapreduce/yt/interface/io.h>
#include <mapreduce/yt/interface/mpl.h>

#include <mapreduce/yt/interface/logging/log.h>

#include <library/cpp/threading/blocking_queue/blocking_queue.h>

#include <util/generic/maybe.h>
#include <util/generic/ptr.h>
#include <util/generic/scope.h>
#include <util/generic/yexception.h>

#include <util/string/builder.h>

#include <util/system/thread.h>

namespace NYT {
namespace NDetail {

////////////////////////////////////////////////////////////////////////////////

template <class T>
class TParallelUnorderedTableWriterBase
    : public TRowTraits<T>::IWriterImpl
{
public:
    TParallelUnorderedTableWriterBase(
        const IClientBasePtr& client,
        const TRichYPath &path,
        const TParallelTableWriterOptions& options)
        : Rows_(1000)
        , Transaction_(client->StartTransaction())
        , Path_(path)
        , Options_(options.TableWriterOptions_)
    {
        if (!Path_.Append_.GetOrElse(false)) {
            if (Transaction_->Exists(Path_.Path_)) {
                Transaction_->Lock(Path_.Path_, LM_EXCLUSIVE);
                //Clear table
                Transaction_->CreateTableWriter<T>(Path_, Options_)->Finish();
            } else {
                Transaction_->Create(Path_.Path_, NT_TABLE);
                Transaction_->Lock(Path_.Path_, LM_EXCLUSIVE);
            }
            Path_.Append(true);
        }

        Args_.resize(options.ThreadCount_);
        for (size_t i = 0; i < Args_.size(); ++i) {
            Args_[i] = {this, Transaction_->CreateTableWriter<T>(Path_, Options_)};
        }

        for (size_t i = 0; i < options.ThreadCount_; ++i) {
            TString threadName = ::TStringBuilder() << "par_writer_" << i;
            Threads_.push_back(::MakeHolder<TThread>(
                TThread::TParams(WriterThread, &Args_[i]).SetName(threadName)));
            Threads_.back()->Start();
        }
    }

    ~TParallelUnorderedTableWriterBase()
    {
        // This statement always true
        // because even in an exceptional situation
        // owner class call Finish in destructor
        Y_VERIFY(State_ == EWriterState::Finished);
    }

    void Abort() override
    {
        Transaction_->Abort();
        Rows_.Stop();
        WaitThreads();
        State_ = EWriterState::Finished;
    }

    size_t GetTableCount() const override
    {
        return 1;
    }

    void FinishTable(size_t) override
    {
        if (State_ != EWriterState::Finished) {
            Rows_.Stop();
            WaitThreads();
        }
        if (State_ == EWriterState::Exception) {
            State_ = EWriterState::Finished;
            Transaction_->Abort();
            std::rethrow_exception(Exception_);
        }
        if (State_ == EWriterState::Ok) {
            State_ = EWriterState::Finished;
            Transaction_->Commit();
        }
    }

    static void* WriterThread(void* opaque)
    {
        auto args = static_cast<TParallelUnorderedTableWriterBase<T>::ThreadArgs*>(opaque);
        args->This->WriterThread(args->Writer);
        return nullptr;
    }

private:
    void WriterThread(const TTableWriterPtr<T>& writer)
    {
        try {
            while (TMaybe<T> row = Rows_.Pop()) {
                writer->AddRow(row.GetRef());
            }
            writer->Finish();
        } catch (const yexception&) {
            auto state = State_.exchange(EWriterState::Exception);
            if (state == EWriterState::Ok) {
                Exception_ = std::current_exception();
                Rows_.Stop();
            }
        }
    }

    void WaitThreads()
    {
        for (auto& thread : Threads_) {
            thread->Join();
        }
    }

protected:
    NThreading::TBlockingQueue<T> Rows_;

protected:
    void AddRowError() {
        if (State_ == EWriterState::Exception) {
            std::rethrow_exception(Exception_);
        }
        ythrow TApiUsageError() << "Can't write after Finish or Abort";
    }

private:
    enum class EWriterState
    {
        Ok,
        Finished,
        Exception
    };

private:
    TVector<THolder<TThread>> Threads_;
    ITransactionPtr Transaction_;
    TRichYPath Path_;
    const TTableWriterOptions Options_;
    std::exception_ptr Exception_ = nullptr;

    struct ThreadArgs
    {
        TParallelUnorderedTableWriterBase* This;
        TTableWriterPtr<T> Writer;
    };
    TVector<ThreadArgs> Args_;

    std::atomic<EWriterState> State_ = EWriterState::Ok;
};

template <typename T, typename = void>
class TParallelUnorderedTableWriter
    : public TParallelUnorderedTableWriterBase<T>
{
public:
    using TBase = TParallelUnorderedTableWriterBase<T>;
    using TBase::TParallelUnorderedTableWriterBase;

    void AddRow(T&& row, size_t) override
    {
        if (!TBase::Rows_.Push(std::move(row))) {
            TBase::AddRowError();
        }
    }

    void AddRow(const T& row, size_t) override
    {
        if (!TBase::Rows_.Push(row)) {
            TBase::AddRowError();
        }
    }
};

template <typename T>
class TParallelUnorderedTableWriter<
    T,
    std::enable_if_t<TIsBaseOf<Message, T>::Value>
>
    : public TParallelUnorderedTableWriterBase<T>
{
public:
    using TBase = TParallelUnorderedTableWriterBase<T>;
    using TBase::TParallelUnorderedTableWriterBase;

    void AddRow(Message&& row, size_t) override
    {
        if (!TBase::Rows_.Push(std::move(static_cast<T&&>(row)))) {
            TBase::AddRowError();
        }
    }

    void AddRow(const Message& row, size_t) override
    {
        if (!TBase::Rows_.Push(static_cast<const T&>(row))) {
            TBase::AddRowError();
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NDetail

template <typename T>
TTableWriterPtr<T> CreateParallelUnorderedTableWriter(
    const IClientBasePtr& client,
    const TRichYPath& path,
    const TParallelTableWriterOptions& options)
{
    return ::MakeIntrusive<TTableWriter<T>>(new NDetail::TParallelUnorderedTableWriter<T>(client, path, options));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
