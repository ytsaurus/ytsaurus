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
        : ThreadCount_(options.ThreadCount_)
        , Options_(WithoutAutoFinish(options.TableWriterOptions_))
        , Transaction_(client->StartTransaction())
        , Path_(path)
        , ThreadPool_(threadPool)
        , WriterPool_(ThreadCount_)
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

        // Warming up. Create at least 1 writer for future use.
        AddWriteTask(TWriteTask(TVector<T>{}, 0));
    }

    ~TParallelUnorderedTableWriterBase()
    {
        // This statement always true
        // because even in an exceptional situation
        // owner class call Finish in destructor
        Y_ABORT_UNLESS(Stopped_ == true);
    }

    void Abort() override
    {
        Transaction_->Abort();
        Stopped_ = true;
        ::NThreading::WaitAll(TaskFutures_).Wait();
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
        if (!Stopped_) {
            Stopped_ = true;

            ::NThreading::WaitAll(TaskFutures_).GetValueSync();
            auto writerList = WriterPool_.Finish();
            for (auto&& writer : writerList) {
                try {
                    writer->Finish();
                } catch (const std::exception& ex) {
                    Transaction_->Abort();
                    throw;
                }
            }
            Transaction_->Commit();
        }
    }

protected:
    void AddWriteTask(TWriteTask<T> task)
    {
        if (Stopped_) {
            ythrow TApiUsageError() << "Can't write after Finish or Abort";
        }

        if (RamLimiter_ && RamLimiter_->GetLimit() < task.GetWeight()) {
            ythrow TApiUsageError() << "Weight of row/rowBatch is greater than RamLimiter limit";
        }

        auto taskCountGuard = TaskCountLimiter_
            ? std::make_optional<TResourceGuard>(TaskCountLimiter_, 1)
            : std::nullopt;

        auto memoryGuard = RamLimiter_
            ? std::make_optional<TResourceGuard>(RamLimiter_, task.GetWeight())
            : std::nullopt;

        while (TaskFutures_.size() > 0) {
            if (TaskFutures_.front().HasValue()) {
                TaskFutures_.pop_front();
                continue;
            } else if (TaskFutures_.front().HasException()) {
                Stopped_ = true;
                TaskFutures_.front().TryRethrow();
            } else {
                break;
            }
        }

        TaskFutures_.push_back(::NThreading::Async([
            this,
            task=std::move(task),
            taskCountGuard=std::move(taskCountGuard),
            memoryGuard=std::move(memoryGuard)
        ] () mutable {
            auto writer = WriterPool_.Acquire([&] {
                auto writer = Transaction_->CreateTableWriter<T>(Path_, Options_);

                if (RamLimiter_ && AcquireRamForBuffers_) {
                    WriterMemoryGuards_.push_back({RamLimiter_, writer->GetBufferMemoryUsage(), EResourceLimiterLockType::HARD});
                }

                return writer;
            });
            task.Process(writer);

            // If exception in Process is thrown parallel writer is going to be aborted.
            // No need to Release writer in this case.
            WriterPool_.Release(std::move(writer));
        }, *ThreadPool_));
    }

private:
    class TLazyWriterPool
    {
    public:
        TLazyWriterPool(size_t maxPoolSize)
            : MaxPoolSize_(maxPoolSize)
        { }

        TTableWriterPtr<T> Acquire(const std::function<TTableWriterPtr<T>()>& createFunc)
        {
            auto g = Guard(Lock_);
            Y_ABORT_IF(!Running_);
            while (WriterPool_.empty() && PoolSize_ >= MaxPoolSize_) {
                HasWriterCV_.Wait(Lock_);
            }
            if (!WriterPool_.empty()) {
                auto writer = std::move(WriterPool_.front());
                WriterPool_.pop_front();
                return writer;
            } else {
                PoolSize_ += 1;
                g.Release();

                return createFunc();
            }
        }

        void Release(TTableWriterPtr<T>&& writer)
        {
            auto g = Guard(Lock_);
            if (!Running_) {
                return;
            }
            WriterPool_.push_back(std::move(writer));
            HasWriterCV_.Signal();
        }

        void Abort()
        {
            auto g = Guard(Lock_);
            Running_ = false;
        }

        std::deque<TTableWriterPtr<T>> Finish()
        {
            auto g = Guard(Lock_);
            Y_ABORT_IF(!Running_);
            Running_ = false;

            Y_ABORT_IF(WriterPool_.size() != PoolSize_,
                "Finish is expected to be called after all writers are released, %ld != %ld",
                WriterPool_.size(), PoolSize_);

            return std::move(WriterPool_);
        }

    private:
        const size_t MaxPoolSize_;

        TMutex Lock_;
        TCondVar HasWriterCV_;
        std::deque<TTableWriterPtr<T>> WriterPool_;
        size_t PoolSize_ = 0;
        bool Running_ = true;
    };

private:
    static TTableWriterOptions WithoutAutoFinish(const TTableWriterOptions& options)
    {
        auto result = options;
        result.AutoFinish(false);
        return result;
    }

private:
    const size_t ThreadCount_ = 0;
    const TTableWriterOptions Options_;

    ITransactionPtr Transaction_;
    TRichYPath Path_;

    std::shared_ptr<IThreadPool> ThreadPool_;
    std::atomic<bool> Stopped_{false};
    TLazyWriterPool WriterPool_;
    std::deque<::NThreading::TFuture<void>> TaskFutures_;

    std::exception_ptr Exception_ = nullptr;

    IResourceLimiterPtr TaskCountLimiter_;
    IResourceLimiterPtr RamLimiter_;

    bool AcquireRamForBuffers_;

    std::vector<TResourceGuard> WriterMemoryGuards_;
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
        TBase::AddWriteTask(TWriteTask<T>(std::move(row), rowWeight));
    }

    void AddRow(const T& row, size_t tableIndex) override
    {
        AddRow(row, tableIndex, 0 /*rowWeight*/);
    }

    void AddRow(const T& row, size_t /*tableIndex*/, size_t rowWeight) override
    {
        TBase::AddWriteTask(TWriteTask<T>(row, rowWeight));
    }

    void AddRowBatch(TVector<T>&& rowBatch, size_t /*tableIndex*/, size_t rowBatchWeight) override
    {
        TBase::AddWriteTask(TWriteTask<T>(std::move(rowBatch), rowBatchWeight));
    }

    void AddRowBatch(const TVector<T>& rowBatch, size_t /*tableIndex*/, size_t rowBatchWeight) override
    {
        TBase::AddWriteTask(TWriteTask<T>(rowBatch, rowBatchWeight));
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
        TBase::AddWriteTask(TWriteTask<T>(std::move(static_cast<T&&>(row)), rowWeight));
    }

    void AddRow(const Message& row, size_t tableIndex) override
    {
        AddRow(row, tableIndex, 0 /*rowWeight*/);
    }

    void AddRow(const Message& row, size_t /*tableIndex*/, size_t rowWeight) override
    {
        TBase::AddWriteTask(TWriteTask<T>(static_cast<const T&>(row), rowWeight));
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
