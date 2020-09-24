#pragma once

#ifndef PARALLEL_READER_INL_H_
#error "Direct inclusion of this file is not allowed, use parallel_reader.h"
#endif
#undef PARALLEL_READER_INL_H_

#include "parallel_reader.h"

#include <mapreduce/yt/interface/client.h>
#include <mapreduce/yt/interface/io.h>
#include <mapreduce/yt/interface/mpl.h>

#include <mapreduce/yt/interface/logging/log.h>

#include <library/cpp/threading/blocking_queue/blocking_queue.h>

#include <util/generic/maybe.h>
#include <util/generic/ptr.h>
#include <util/generic/queue.h>
#include <util/generic/scope.h>
#include <util/generic/yexception.h>

#include <util/string/builder.h>

#include <util/system/thread.h>

namespace NYT {
namespace NDetail {

////////////////////////////////////////////////////////////////////////////////

template <typename TRow>
struct TReaderEntry
{
    TTableReaderPtr<TRow> Reader;
    int TableIndex;
    i64 SeqNum;
};

template <typename T>
struct TReaderBuffer
    : public TThrRefBase
{
    TVector<T> Rows;
    ui32 TableIndex = 0;
    ui64 FirstRowIndex = 0;
    i64 SeqNum = 0;
};

template <typename T>
using TReaderBufferPtr = ::TIntrusivePtr<TReaderBuffer<T>>;

struct TReadManagerConfigBase
{
    int ThreadCount;
    i64 BatchSize;
    i64 BatchCount;
};

struct TUnorderedReadManagerConfig
    : TReadManagerConfigBase
{ };

struct TOrderedReadManagerConfig
    : TReadManagerConfigBase
{
    i64 TotalRangeCount;
};

using TReadManagerConfigVariant = TVariant<
    TUnorderedReadManagerConfig,
    TOrderedReadManagerConfig>;

////////////////////////////////////////////////////////////////////////////////

//
// Interface of table reader producer. GetNextEntry() must be thread-safe.
template <typename TRow>
class ITableReaderFactory
    : public TThrRefBase
{
public:
    // Returns Nothing when factory is exhausted.
    virtual TMaybe<TReaderEntry<TRow>> GetNextEntry() = 0;
};

template <typename TRow>
using ITableReaderFactoryPtr = ::TIntrusivePtr<ITableReaderFactory<TRow>>;

////////////////////////////////////////////////////////////////////////////////

i64 GetRowCount(const TRichYPath& path);

class TTableSlicer
{
public:
    // Default constructor for empty iterator.
    TTableSlicer() = default;
    TTableSlicer(TRichYPath path, i64 batchSize);

    void Next();
    bool IsValid() const;
    TReadRange GetRange() const;

private:
    i64 GetLowerLimit() const;
    i64 GetUpperLimit() const;

private:
    TRichYPath Path_;
    i64 BatchSize_ = 0;
    i64 RangeIndex_ = 0;
    i64 Offset_ = 0;
};

////////////////////////////////////////////////////////////////////////////////

template <typename TRow>
class TUnorderedTableReaderFactory
    : public ITableReaderFactory<TRow>
{
public:
    // NB: `paths` must be canonized.
    TUnorderedTableReaderFactory(
        IClientBasePtr client,
        const TVector<TRichYPath>& paths,
        const TUnorderedReadManagerConfig& config,
        const TTableReaderOptions& options)
        : ThreadCount_(config.ThreadCount)
        , Client_(std::move(client))
        , Paths_(paths)
        , Options_(options)
    {
        Y_VERIFY(!Paths_.empty());
        auto totalRowCount = GetRowCount(Paths_[TableIndex_]);
        TableSlicer_ = TTableSlicer(Paths_[TableIndex_], totalRowCount / ThreadCount_ + 1);
    }

    TMaybe<TReaderEntry<TRow>> GetNextEntry() override
    {
        TRichYPath path;
        TReaderEntry<TRow> entry;
        {
            TGuard<TMutex> guard(Lock_);
            while (!TableSlicer_.IsValid()) {
                if (TableIndex_ >= static_cast<int>(Paths_.size()) - 1) {
                    return Nothing();
                }
                ++TableIndex_;
                auto totalRowCount = GetRowCount(Paths_[TableIndex_]);
                TableSlicer_ = TTableSlicer(Paths_[TableIndex_], totalRowCount / ThreadCount_ + 1);
            }
            entry.TableIndex = TableIndex_;
            path = TRichYPath(Paths_[TableIndex_]);
            path.Ranges_ = {TableSlicer_.GetRange()};
            TableSlicer_.Next();
        }
        entry.Reader = Client_->CreateTableReader<TRow>(std::move(path), Options_);
        return entry;
    }

private:
    const int ThreadCount_;
    const IClientBasePtr Client_;
    const TVector<TRichYPath> Paths_;
    const TTableReaderOptions Options_;

    TMutex Lock_;
    int TableIndex_ = 0;
    TTableSlicer TableSlicer_;
};

////////////////////////////////////////////////////////////////////////////////

template <typename TRow>
class TOrderedTableReaderFactory
    : public ITableReaderFactory<TRow>
{
public:
    // NB: `paths` must be canonized.
    TOrderedTableReaderFactory(
        IClientBasePtr client,
        const TVector<TRichYPath>& paths,
        const TOrderedReadManagerConfig& config,
        const TTableReaderOptions& options)
        : Config_(config)
        , Client_(std::move(client))
        , Paths_(paths)
        , Options_(options)
    {
        Y_VERIFY(!Paths_.empty());
        TableSlicer_ = TTableSlicer(Paths_[TableIndex_], Config_.BatchSize);
    }

    TMaybe<TReaderEntry<TRow>> GetNextEntry() override
    {
        TRichYPath path;
        TReaderEntry<TRow> entry;
        {
            TGuard<TMutex> guard(Lock_);
            if (TableSlices_.empty()) {
                RefillCurrentTableSlices();
            }
            if (TableSlices_.empty()) {
                return Nothing();
            }
            std::tie(path, entry.SeqNum) = std::move(TableSlices_.front());
            TableSlices_.pop();
            entry.TableIndex = TableIndex_;
        }
        entry.Reader = Client_->CreateTableReader<TRow>(std::move(path), Options_);
        return entry;
    }

private:
    // Lock_ must be taken on entry.
    void RefillCurrentTableSlices()
    {
        while (!TableSlicer_.IsValid()) {
            if (TableIndex_ >= static_cast<int>(Paths_.size()) - 1) {
                return;
            }
            ++TableIndex_;
            TableSlicer_ = TTableSlicer(Paths_[TableIndex_], Config_.BatchSize);
        }

        auto path = Paths_[TableIndex_];
        // Add empty range to avoid having a path without ranges (meaning the whole table).
        path.Ranges_ = {TReadRange::FromRowIndices(0, 0)};
        TVector<TRichYPath> slices(Config_.ThreadCount, path);
        i64 totalBatchCount = 0;
        for (i64 batchIndex = 0; TableSlicer_.IsValid() && batchIndex < Config_.TotalRangeCount; ++batchIndex) {
            slices[batchIndex % Config_.ThreadCount].AddRange(TableSlicer_.GetRange());
            ++totalBatchCount;
            TableSlicer_.Next();
        }
        auto seqNum = SeqNumBegin_;
        for (auto& slice : slices) {
            TableSlices_.push({std::move(slice), seqNum});
            ++seqNum;
        }
        SeqNumBegin_ += totalBatchCount;
    }

private:
    const TOrderedReadManagerConfig Config_;
    const IClientBasePtr Client_;
    const TVector<TRichYPath> Paths_;
    const TTableReaderOptions Options_;

    TMutex Lock_;
    int TableIndex_ = 0;
    i64 SeqNumBegin_ = 0;
    // {Path, SeqNumBegin}.
    TQueue<std::pair<TRichYPath, i64>> TableSlices_;
    TTableSlicer TableSlicer_;
};

////////////////////////////////////////////////////////////////////////////////

template <typename TRow>
static void ReadRows(const TTableReaderPtr<TRow>& reader, i64 batchSize, TVector<TRow>& rows)
{
    // Don't clear the buffer to avoid unnecessary calls of destructors
    // and memory allocation/deallocation.
    i64 index = 0;
    auto rowIndex = reader->GetRowIndex();
    while (reader->IsValid() && rowIndex == reader->GetRowIndex() && index < batchSize) {
        if (index >= static_cast<i64>(rows.size())) {
            rows.push_back(reader->MoveRow());
        } else {
            reader->MoveRow(&rows[index]);
        }
        ++index;
        ++rowIndex;
        reader->Next();
    }
    rows.resize(index);
}

template <typename TRow>
class TReadManagerBase
{
public:
    TReadManagerBase(ITableReaderFactoryPtr<TRow> factory, int threadCount)
        : Factory_(std::move(factory))
        , RunningThreadCount_(threadCount)
    {
        for (int i = 0; i < threadCount; ++i) {
            TString threadName = ::TStringBuilder() << "par_reader_" << i;
            Threads_.push_back(::MakeHolder<TThread>(
                TThread::TParams(ReaderThread, this).SetName(threadName)));
        }
    }

    virtual ~TReadManagerBase() = default;

    void Start()
    {
        for (auto& thread : Threads_) {
            thread->Start();
        }
    }

    void Stop()
    {
        DoStop();
        for (auto& thread : Threads_) {
            thread->Join();
        }
    }

    TReaderBufferPtr<TRow> GetNextFilledBuffer()
    {
        auto buffer = DoGetNextFilledBuffer();
        if (!buffer) {
            TGuard<TMutex> guard(Lock_);
            if (Exception_) {
                std::rethrow_exception(Exception_);
            }
        }
        return buffer;
    }

    virtual void OnBufferDrained(TReaderBufferPtr<TRow> buffer) = 0;

private:
    virtual TReaderBufferPtr<TRow> DoGetNextFilledBuffer() = 0;
    virtual bool ProcessEntry(TReaderEntry<TRow> entry) = 0;
    virtual void DoStop() = 0;

    void ReaderThread()
    {
        try {
            while (auto entry = Factory_->GetNextEntry()) {
                if (!ProcessEntry(std::move(*entry))) {
                    break;
                }
            }
        } catch (const yexception& exception) {
            LOG_ERROR("Exception in parallel reader thread: %s",
                exception.what());
            TGuard<TMutex> guard(Lock_);
            Exception_ = std::make_exception_ptr(exception);
            DoStop();
        }
        {
            TGuard<TMutex> guard(Lock_);
            if (RunningThreadCount_ == 1) {
                DoStop();
            }
            --RunningThreadCount_;
            LOG_DEBUG("%d/%d parallel reader threads finished",
                static_cast<int>(Threads_.size()) - RunningThreadCount_,
                Threads_.size());
        }
    }

    static void* ReaderThread(void* opaque)
    {
        static_cast<TReadManagerBase<TRow>*>(opaque)->ReaderThread();
        return nullptr;
    }

private:
    const ITableReaderFactoryPtr<TRow> Factory_;

    TVector<THolder<TThread>> Threads_;

    TMutex Lock_;
    int RunningThreadCount_ = 0;
    std::exception_ptr Exception_;

};

template <typename TRow>
class TUnorderedReadManager
    : public TReadManagerBase<TRow>
{
public:
    TUnorderedReadManager(
        IClientBasePtr client,
        TVector<TRichYPath> paths,
        const TUnorderedReadManagerConfig& config,
        const TTableReaderOptions& options)
        : TReadManagerBase<TRow>(
            ::MakeIntrusive<TUnorderedTableReaderFactory<TRow>>(
                std::move(client),
                std::move(paths),
                config,
                options),
            config.ThreadCount)
        , Config_(config)
        , EmptyBuffers_(0)
        , FilledBuffers_(0)
    {
        for (i64 i = 0; i < Config_.BatchCount; ++i) {
            EmptyBuffers_.Push(MakeIntrusive<TReaderBuffer<TRow>>());
        }
    }

    void OnBufferDrained(TReaderBufferPtr<TRow> buffer) override
    {
        EmptyBuffers_.Push(std::move(buffer));
    }

private:
    bool ProcessEntry(TReaderEntry<TRow> entry) override
    {
        const auto& reader = entry.Reader;
        while (reader->IsValid()) {
            auto buffer = EmptyBuffers_.Pop().GetOrElse(nullptr);
            if (!buffer) {
                return false;
            }
            buffer->TableIndex = entry.TableIndex;
            buffer->FirstRowIndex = reader->GetRowIndex();
            ReadRows(reader, Config_.BatchSize, buffer->Rows);
            if (!FilledBuffers_.Push(std::move(buffer))) {
                return false;
            }
        }
        return true;
    }

    TReaderBufferPtr<TRow> DoGetNextFilledBuffer() override
    {
        return FilledBuffers_.Pop().GetOrElse(nullptr);
    }

    void DoStop() override
    {
        FilledBuffers_.Stop();
        EmptyBuffers_.Stop();
    }

private:
    const TUnorderedReadManagerConfig Config_;

    NThreading::TBlockingQueue<TReaderBufferPtr<TRow>> EmptyBuffers_;
    NThreading::TBlockingQueue<TReaderBufferPtr<TRow>> FilledBuffers_;
};

template <typename TRow>
class TOrderedReadManager
    : public TReadManagerBase<TRow>
{
public:
    TOrderedReadManager(
        IClientBasePtr client,
        TVector<TRichYPath> paths,
        const TOrderedReadManagerConfig& config,
        const TTableReaderOptions& options)
        : TReadManagerBase<TRow>(
            ::MakeIntrusive<TOrderedTableReaderFactory<TRow>>(
                std::move(client),
                std::move(paths),
                config,
                options),
            config.ThreadCount)
        , Config_(config)
        , BucketCount_(config.ThreadCount)
        , FilledBuffers_(0)
    {
        auto batchCountPerBucket = Config_.BatchCount / BucketCount_ + 1;
        for (int bucketIndex = 0; bucketIndex < BucketCount_; ++bucketIndex) {
            auto& bufferQueue = EmptyBuffers_.emplace_back(::MakeHolder<TQueue>(0));
            for (int j = 0; j < batchCountPerBucket; ++j) {
                bufferQueue->Push(MakeIntrusive<TReaderBuffer<TRow>>());
            }
        }
    }

    void OnBufferDrained(TReaderBufferPtr<TRow> buffer) override
    {
        auto bucketIndex = GetBucketIndex(buffer->SeqNum);
        EmptyBuffers_[bucketIndex]->Push(std::move(buffer));
    }

private:
    int GetBucketIndex(i64 seqNum) const
    {
        return seqNum % EmptyBuffers_.size();
    }

    TReaderBufferPtr<TRow> DoGetNextFilledBuffer() override
    {
        while (FilledBuffersOrdered_.empty() || FilledBuffersOrdered_.top()->SeqNum != CurrentSeqNum_) {
            auto waitStart = TInstant::Now();
            auto buffer = FilledBuffers_.Pop().GetOrElse(nullptr);
            WaitTime_ += TInstant::Now() - waitStart;
            if (!buffer) {
                return nullptr;
            }
            FilledBuffersOrdered_.push(std::move(buffer));
        }
        ++CurrentSeqNum_;
        auto buffer = FilledBuffersOrdered_.top();
        FilledBuffersOrdered_.pop();
        return buffer;
    }

    bool ProcessEntry(TReaderEntry<TRow> entry) override
    {
        const auto& reader = entry.Reader;
        auto seqNum = entry.SeqNum;
        auto bucketIndex = GetBucketIndex(seqNum);
        while (reader->IsValid()) {
            auto buffer = EmptyBuffers_[bucketIndex]->Pop().GetOrElse(nullptr);
            if (!buffer) {
                return false;
            }
            buffer->SeqNum = seqNum;
            buffer->TableIndex = entry.TableIndex;
            buffer->FirstRowIndex = reader->GetRowIndex();
            ReadRows(reader, Config_.BatchSize, buffer->Rows);
            seqNum += BucketCount_;
            if (!FilledBuffers_.Push(std::move(buffer))) {
                return false;
            }
        }
        return true;
    }

    void DoStop() override
    {
        LOG_DEBUG("Finishing ordered parallel read manager; total wait time is %f seconds",
            WaitTime_.SecondsFloat());
        FilledBuffers_.Stop();
        for (auto& queue : EmptyBuffers_) {
            queue->Stop();
        }
    }

private:
    const TOrderedReadManagerConfig Config_;
    const int BucketCount_;

    TDuration WaitTime_;

    using TQueue = NThreading::TBlockingQueue<TReaderBufferPtr<TRow>>;
    TVector<THolder<TQueue>> EmptyBuffers_;
    TQueue FilledBuffers_;

    template <typename T>
    struct TGreaterBySeqNum
    {
        bool operator()(const TReaderBufferPtr<T>& left, const TReaderBufferPtr<T>& right)
        {
            return left->SeqNum > right->SeqNum;
        }
    };

    TPriorityQueue<
        TReaderBufferPtr<TRow>,
        TVector<TReaderBufferPtr<TRow>>,
        TGreaterBySeqNum<TRow>
    > FilledBuffersOrdered_;
    i64 CurrentSeqNum_ = 0;
};

//
// Base class for table reader using several threads to read the table in parallel.
// Readers are produced by the passed factory.
template <typename TDerived, typename TRow>
class TParallelTableReaderBase
    : public TDerived
{
public:
    TParallelTableReaderBase(
        IClientBasePtr client,
        TVector<TRichYPath> paths,
        const TReadManagerConfigVariant& config,
        const TTableReaderOptions& options)
    {
        if (HoldsAlternative<TUnorderedReadManagerConfig>(config)) {
            const auto& unorderedConfig = Get<TUnorderedReadManagerConfig>(config);
            LOG_DEBUG("Starting unordered parallel reader: "
                "ThreadCount = %d, BatchSize = %d, BatchCount = %d",
                unorderedConfig.ThreadCount,
                unorderedConfig.BatchSize,
                unorderedConfig.BatchCount);
            ReadManager_ = MakeHolder<TUnorderedReadManager<TRow>>(
                std::move(client),
                std::move(paths),
                unorderedConfig,
                options);
        } else if (HoldsAlternative<TOrderedReadManagerConfig>(config)) {
            const auto& orderedConfig = Get<TOrderedReadManagerConfig>(config);
            LOG_DEBUG("Starting ordered parallel reader: "
                "ThreadCount = %d, BatchSize = %d, BatchCount = %d, TotalRangeCount = %d",
                orderedConfig.ThreadCount,
                orderedConfig.BatchSize,
                orderedConfig.BatchCount,
                orderedConfig.TotalRangeCount);
            ReadManager_ = MakeHolder<TOrderedReadManager<TRow>>(
                std::move(client),
                std::move(paths),
                orderedConfig,
                options);
        } else {
            Y_FAIL();
        }
        ReadManager_->Start();
        NextBuffer();
    }

    bool IsValid() const override
    {
        return CurrentBuffer_ != nullptr;
    }

    void Next() override
    {
        if (!CurrentBuffer_) {
            return;
        }
        ++CurrentBufferIt_;
        while (CurrentBuffer_ && CurrentBufferIt_ == CurrentBuffer_->Rows.end()) {
            ReadManager_->OnBufferDrained(std::move(CurrentBuffer_));
            NextBuffer();
        }
    }

    void NextBuffer()
    {
        CurrentBuffer_ = ReadManager_->GetNextFilledBuffer();
        if (!CurrentBuffer_) {
            return;
        }
        CurrentBufferIt_ = CurrentBuffer_->Rows.begin();
    }

    ui32 GetTableIndex() const override
    {
        Y_VERIFY(CurrentBuffer_);
        return CurrentBuffer_->TableIndex;
    }

    ui32 GetRangeIndex() const override
    {
        Y_FAIL("Not implemented");
    }

    ui64 GetRowIndex() const override
    {
        Y_VERIFY(CurrentBuffer_);
        return CurrentBuffer_->FirstRowIndex + (CurrentBufferIt_ - CurrentBuffer_->Rows.begin());
    }

    void NextKey() override
    {
        Y_FAIL("Not implemented");
    }

    ~TParallelTableReaderBase()
    {
        ReadManager_->Stop();
    }

protected:
    typename TVector<TRow>::iterator CurrentBufferIt_;

private:
    TReaderBufferPtr<TRow> CurrentBuffer_;
    THolder<TReadManagerBase<TRow>> ReadManager_;
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
        return *CurrentBufferIt_;
    }

    void MoveRow(TNode* row) override
    {
        *row = *CurrentBufferIt_;
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
        static_cast<T&>(*row) = *this->CurrentBufferIt_;
    }
};

std::pair<IClientBasePtr, TVector<TRichYPath>> CreateRangeReaderClientAndPaths(
    const IClientBasePtr& client,
    const TVector<TRichYPath>& paths,
    bool createTransaction);

i64 EstimateTableRowWeight(const IClientBasePtr& client, const TVector<TRichYPath>& paths);

////////////////////////////////////////////////////////////////////////////////

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

template <typename T>
TTableReaderPtr<T> CreateParallelTableReader(
    const IClientBasePtr& client,
    const TVector<TRichYPath>& paths,
    const TParallelTableReaderOptions& options)
{
    Y_ENSURE_EX(!paths.empty(), TApiUsageError() << "Parallel table reader: paths should not be empty");
    Y_ENSURE_EX(options.ThreadCount_ >= 1, TApiUsageError() << "ThreadCount can not be zero");

    auto [rangeReaderClient, rangeReaderPaths] = NDetail::CreateRangeReaderClientAndPaths(
        client,
        paths,
        options.CreateTransaction_);

    auto rangeReaderOptions = TTableReaderOptions()
        .CreateTransaction(false);
    rangeReaderOptions.FormatHints_ = options.FormatHints_;
    rangeReaderOptions.Config_ = options.Config_;

    auto rowWeight = NDetail::EstimateTableRowWeight(rangeReaderClient, rangeReaderPaths);

    auto rowCount = Max<i64>(
        1,
        Min(options.MemoryLimit_ / rowWeight, options.BufferedRowCountLimit_));
    auto batchSize = Max<i64>(
        1,
        Min(options.BatchSizeBytes_ / rowWeight, rowCount / options.ThreadCount_ + 1));
    auto batchCount = Max<i64>(
        1,
        rowCount / batchSize);

    NDetail::TReadManagerConfigVariant config;
    if (options.Ordered_) {
        NDetail::TOrderedReadManagerConfig cfg;
        cfg.ThreadCount = options.ThreadCount_;
        cfg.BatchSize = batchSize;
        cfg.BatchCount = batchCount;
        cfg.TotalRangeCount = options.TotalRangeCount_;
        config = cfg;
    } else {
        NDetail::TUnorderedReadManagerConfig cfg;
        cfg.ThreadCount = options.ThreadCount_;
        cfg.BatchSize = batchSize;
        cfg.BatchCount = batchCount;
        config = cfg;
    }

    return ::MakeIntrusive<TTableReader<T>>(
        new NDetail::TParallelTableReader<T>(
            std::move(rangeReaderClient),
            std::move(rangeReaderPaths),
            config,
            rangeReaderOptions));
}

template <typename T>
TTableReaderPtr<T> CreateParallelTableReader(
    const IClientBasePtr& client,
    const TRichYPath& path,
    const TParallelTableReaderOptions& options)
{
    return CreateParallelTableReader<T>(client, TVector<TRichYPath>{path}, options);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
