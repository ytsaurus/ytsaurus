#pragma once

#ifndef PARALLEL_WRITER_INL_H_
#error "Direct inclusion of this file is not allowed, use parallel_writer.h"
#include "parallel_writer.h"
#endif
#undef PARALLEL_WRITER_INL_H_

#include "resource_limiter.h"

#include <yt/cpp/mapreduce/interface/client.h>
#include <yt/cpp/mapreduce/interface/fwd.h>
#include <yt/cpp/mapreduce/interface/io.h>
#include <yt/cpp/mapreduce/interface/mpl.h>

#include <yt/cpp/mapreduce/interface/logging/yt_log.h>

#include <yt/cpp/mapreduce/common/helpers.h>

#include <library/cpp/threading/blocking_queue/blocking_queue.h>
#include <library/cpp/threading/future/future.h>
#include <library/cpp/threading/future/async.h>

#include <util/generic/maybe.h>
#include <util/generic/ptr.h>
#include <util/generic/scope.h>

#include <util/string/builder.h>

#include <util/system/thread.h>

#include <util/thread/pool.h>

namespace NYT {
namespace NDetail {

////////////////////////////////////////////////////////////////////////////////

template <typename T>
class TWriteTask
{
public:
    TWriteTask(const T& row, size_t rowWeight)
        : Row_(row)
        , Weight_(rowWeight)
    { }

    TWriteTask(T&& row, size_t rowWeight)
        : Row_(std::move(row))
        , Weight_(rowWeight)
    { }

    TWriteTask(const TVector<T>& rowBatch, size_t rowBatchWeight)
        : RowBatch_(rowBatch)
        , Weight_(rowBatchWeight)
    { }

    TWriteTask(TVector<T>&& rowBatch, size_t rowBatchWeight)
        : RowBatch_(std::move(rowBatch))
        , Weight_(rowBatchWeight)
    { }

    void Process(const TTableWriterPtr<T>& writer)
    {
        if (Row_) {
            writer->AddRow(std::move(Row_).value());
        } else {
            writer->AddRowBatch(std::move(RowBatch_).value());
        }
    }

    size_t GetWeight() const
    {
        return Weight_;
    }

private:
    std::optional<T> Row_;
    std::optional<TVector<T>> RowBatch_;
    size_t Weight_;
};

////////////////////////////////////////////////////////////////////////////////

template <class T>
class TParallelUnorderedTableWriterBase
    : public TRowTraits<T>::IWriterImpl
{
public:
    TParallelUnorderedTableWriterBase(
        const IClientBasePtr& client,
        const TRichYPath& path,
        const std::shared_ptr<IThreadPool>& threadPool,
        const TParallelTableWriterOptions& options)
        : Transaction_(client->StartTransaction())
        , Path_(path)
        , ThreadCount_(options.ThreadCount_)
        , ThreadPool_(threadPool)
        , WritersPool_(options.ThreadCount_)
        , Options_(options.TableWriterOptions_)
        , RamLimiter_(options.RamLimiter_)
        , AcquireRamForBuffers_(options.AcquireRamForBuffers_)
    {
        if (options.TaskCount_ > 0) {
            TaskCountLimiter_ = MakeIntrusive<TResourceLimiter>(options.TaskCount_, ::TStringBuilder() << "TParallelUnorderedTableWriterBase[" << NodeToYsonString(PathToNode(Path_)) << "]_TaskCounter");
        }

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

        if (Transaction_->Exists(Path_.Path_)) {
            auto sortedBy = Transaction_->Get(Path_.Path_ + "/@sorted");
            Y_ENSURE_EX(!sortedBy.AsBool(),
                TApiUsageError() <<
                    "ParallelUnorderedTableWriter cannot be used " <<
                    "to write to sorted table " << Path_.Path_);
        }

        Transaction_->Lock(Path_.Path_, LM_SHARED);

        StartWritersFuture_ = ::NThreading::Async([this, threadCount = options.ThreadCount_]() mutable {
            try {
                for (size_t i = 0; i < threadCount; ++i) {
                    auto writer = Transaction_->CreateTableWriter<T>(Path_, Options_);

                    if (RamLimiter_ && AcquireRamForBuffers_) {
                        WriterMemoryGuards_.push_back({RamLimiter_, writer->GetBufferMemoryUsage(), EResourceLimiterLockType::HARD});
                    }

                    WritersPool_.Push(std::move(writer));
                }
                YT_LOG_DEBUG("All %v writers were created", threadCount);
            } catch (std::exception& ex) {
                WritersCreationWasFailed_ = true;
                auto state = State_.exchange(EWriterState::Exception);
                if (state == EWriterState::Ok) {
                    Exception_ = std::current_exception();
                    Stopped_ = true;
                }
            }
        }, *ThreadPool_);
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
        Stopped_ = true;
        WaitAndFinish();
        State_ = EWriterState::Finished;
    }

    size_t GetBufferMemoryUsage() const override
    {
        throw TApiUsageError() << "Unimplemented method";
    }

    size_t GetTableCount() const override
    {
        return 1;
    }

    void FinishTable(size_t) override
    {
        if (State_ != EWriterState::Finished) {
            Stopped_ = true;
            WaitAndFinish();
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

private:
    void WaitAndFinish()
    {
        StartWritersFuture_.GetValueSync();

        if (WritersCreationWasFailed_) {
            WritersPool_.Stop();
        }

        ::NThreading::WaitAll(Futures_).GetValueSync();

        WritersPool_.Stop();

        while (auto writer = WritersPool_.Pop()) {
            try {
                (*writer)->Finish();
            } catch (const std::exception&) {
                auto state = State_.exchange(EWriterState::Exception);
                if (state == EWriterState::Ok) {
                    Exception_ = std::current_exception();
                    Stopped_ = true;
                }
            }
        }
    }

protected:
    void AddRowError()
    {
        if (State_ == EWriterState::Exception) {
            std::rethrow_exception(Exception_);
        }
        ythrow TApiUsageError() << "Can't write after Finish or Abort";
    }

    bool AddWriteTask(TWriteTask<T> task)
    {
        if (Stopped_) {
            return false;
        }

        if (RamLimiter_ && RamLimiter_->GetLimit() < task.GetWeight()) {
            throw TApiUsageError() << "Weight of row/rowBatch is greater than RamLimiter limit";
        }

        std::optional<TResourceGuard> taskCountGuard = TaskCountLimiter_
            ? std::make_optional<TResourceGuard>(TaskCountLimiter_, 1)
            : std::nullopt;

        std::optional<TResourceGuard> memoryGuard = RamLimiter_
            ? std::make_optional<TResourceGuard>(RamLimiter_, task.GetWeight())
            : std::nullopt;

        Futures_.emplace_back(::NThreading::Async([
            this,
            task=std::move(task),
            taskCountGuard=std::move(taskCountGuard),
            memoryGuard=std::move(memoryGuard)
        ] () mutable {
            try {
                TMaybe<TTableWriterPtr<T>> writer = WritersPool_.Pop();
                if (!writer) {
                    return;
                }
                task.Process(*writer);
                WritersPool_.Push(std::move(*writer));
            } catch (const std::exception&) {
                auto state = State_.exchange(EWriterState::Exception);
                if (state == EWriterState::Ok) {
                    Exception_ = std::current_exception();
                    Stopped_ = true;
                }
            }

            return;
        }, *ThreadPool_));

        return true;
    }

private:
    enum class EWriterState
    {
        Ok,
        Finished,
        Exception
    };

private:
    ITransactionPtr Transaction_;
    TRichYPath Path_;

    const size_t ThreadCount_ = 0;
    std::shared_ptr<IThreadPool> ThreadPool_;
    std::atomic<bool> Stopped_{false};
    std::atomic<bool> WritersCreationWasFailed_{false};
    ::NThreading::TBlockingQueue<TTableWriterPtr<T>> WritersPool_;
    std::vector<::NThreading::TFuture<void>> Futures_;
    ::NThreading::TFuture<void> StartWritersFuture_;

    const TTableWriterOptions Options_;
    std::exception_ptr Exception_ = nullptr;

    IResourceLimiterPtr TaskCountLimiter_;
    IResourceLimiterPtr RamLimiter_;

    bool AcquireRamForBuffers_;

    std::vector<TResourceGuard> WriterMemoryGuards_;

    std::atomic<EWriterState> State_ = EWriterState::Ok;
};

template <typename T, typename = void>
class TParallelUnorderedTableWriter
    : public TParallelUnorderedTableWriterBase<T>
{
public:
    using TBase = TParallelUnorderedTableWriterBase<T>;
    using TBase::TParallelUnorderedTableWriterBase;

    void AddRow(T&& row, size_t tableIndex) override
    {
        AddRow(std::move(row), tableIndex, 0 /*rowWeight*/);
    }

    void AddRow(T&& row, size_t /*tableIndex*/, size_t rowWeight) override
    {
        if (!TBase::AddWriteTask(TWriteTask<T>(std::move(row), rowWeight))) {
            TBase::AddRowError();
        }
    }

    void AddRow(const T& row, size_t tableIndex) override
    {
        AddRow(row, tableIndex, 0 /*rowWeight*/);
    }

    void AddRow(const T& row, size_t /*tableIndex*/, size_t rowWeight) override
    {
        if (!TBase::AddWriteTask(TWriteTask<T>(row, rowWeight))) {
            TBase::AddRowError();
        }
    }

    void AddRowBatch(TVector<T>&& rowBatch, size_t /*tableIndex*/, size_t rowBatchWeight) override
    {
        if (!TBase::AddWriteTask(TWriteTask<T>(std::move(rowBatch), rowBatchWeight))) {
            TBase::AddRowError();
        }
    }

    void AddRowBatch(const TVector<T>& rowBatch, size_t /*tableIndex*/, size_t rowBatchWeight) override
    {
        if (!TBase::AddWriteTask(TWriteTask<T>(rowBatch, rowBatchWeight))) {
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

    void AddRow(Message&& row, size_t tableIndex) override
    {
        AddRow(std::move(row), tableIndex, 0 /*rowWeight*/);
    }

    void AddRow(Message&& row, size_t /*tableIndex*/, size_t rowWeight) override
    {
        if (!TBase::AddWriteTask(TWriteTask<T>(std::move(static_cast<T&&>(row)), rowWeight))) {
            TBase::AddRowError();
        }
    }

    void AddRow(const Message& row, size_t tableIndex) override
    {
        AddRow(row, tableIndex, 0 /*rowWeight*/);
    }

    void AddRow(const Message& row, size_t /*tableIndex*/, size_t rowWeight) override
    {
        if (!TBase::AddWriteTask(TWriteTask<T>(static_cast<const T&>(row), rowWeight))) {
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
    auto threadPool = std::make_shared<TSimpleThreadPool>();
    threadPool->Start(options.ThreadCount_);
    return CreateParallelUnorderedTableWriter<T>(client, path, threadPool, options);
}

template <typename T>
TTableWriterPtr<T> CreateParallelUnorderedTableWriter(
    const IClientBasePtr& client,
    const TRichYPath& path,
    const std::shared_ptr<IThreadPool>& threadPool,
    const TParallelTableWriterOptions& options)
{
    return ::MakeIntrusive<TTableWriter<T>>(new NDetail::TParallelUnorderedTableWriter<T>(client, path, threadPool, options));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
