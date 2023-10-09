#pragma once

#ifndef PARALLEL_READER_INL_H_
#error "Direct inclusion of this file is not allowed, use parallel_reader.h"
#endif
#undef PARALLEL_READER_INL_H_

#include "parallel_reader.h"

#include <yt/cpp/mapreduce/interface/client.h>
#include <yt/cpp/mapreduce/interface/io.h>
#include <yt/cpp/mapreduce/interface/mpl.h>

#include <yt/cpp/mapreduce/interface/logging/yt_log.h>

#include <library/cpp/threading/blocking_queue/blocking_queue.h>

#include <util/generic/maybe.h>
#include <util/generic/ptr.h>
#include <util/generic/queue.h>
#include <util/generic/scope.h>

#include <util/memory/segmented_string_pool.h>

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
    i64 RangeCount = 1;
};

template <typename TRow>
class TReaderBufferBase
    : public TThrRefBase
{
public:
    TVector<TRow> Rows;
    ui32 TableIndex = 0;
    ui64 FirstRowIndex = 0;
    i64 SeqNum = 0;

    // Currently used only in ordered reader.
    int ThreadIndex = -1;
};

template <typename TRow>
class TReaderBuffer
    : public TReaderBufferBase<TRow>
{
public:
    void CaptureRow(TRow* /* row */)
    { }
};

template <>
class TReaderBuffer<TYaMRRow>
    : public TReaderBufferBase<TYaMRRow>
{
public:
    void CaptureRow(TYaMRRow* row)
    {
        auto key = Pool_.append(row->Key.data(), row->Key.size());
        auto subkey = Pool_.append(row->SubKey.data(), row->SubKey.size());
        auto value = Pool_.append(row->Value.data(), row->Value.size());
        row->Key = {key, row->Key.size()};
        row->SubKey = {subkey, row->SubKey.size()};
        row->Value = {value, row->Value.size()};
    }

private:
    static constexpr int ChunkSize = 64 << 10;

private:
    segmented_pool<char> Pool_{ChunkSize};
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
    int RangeCount;
};

using TReadManagerConfigVariant = std::variant<
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
        Y_ABORT_UNLESS(!Paths_.empty());
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
            path.MutableRanges() = {TableSlicer_.GetRange()};
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
        Y_ABORT_UNLESS(!Paths_.empty());
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

            const auto& ranges = path.GetRanges();
            entry.RangeCount = ranges ? ranges->size() : 0;
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

        path.MutableRanges() = {};
        TVector<TRichYPath> slices(Config_.ThreadCount, path);

        i64 createdBatchCount = 0;
        auto desiredBatchCount = static_cast<i64>(Config_.RangeCount) * Config_.ThreadCount;

        for (createdBatchCount = 0; TableSlicer_.IsValid() && createdBatchCount < desiredBatchCount; ++createdBatchCount) {
            i64 threadIndex = createdBatchCount % Config_.ThreadCount;
            slices[threadIndex].AddRange(TableSlicer_.GetRange());
            TableSlicer_.Next();
        }

        auto seqNum = SeqNumBegin_;
        for (auto& slice : slices) {
            const auto& ranges = slice.GetRanges();
            // This slice was not filled. Skip it.
            if (!ranges || ranges->empty()) {
                continue;
            }
            TableSlices_.push({std::move(slice), seqNum});
            ++seqNum;
        }
        SeqNumBegin_ += createdBatchCount;
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
static void ReadRows(const TTableReaderPtr<TRow>& reader, i64 batchSize, const TReaderBufferPtr<TRow>& buffer)
{
    // Don't clear the buffer to avoid unnecessary calls of destructors
    // and memory allocation/deallocation.
    i64 index = 0;

    if (!reader->IsValid()) {
        buffer->Rows.resize(0);
        return;
    }

    auto rowIndex = reader->GetRowIndex();
    auto& rows = buffer->Rows;

    while (reader->IsValid() && rowIndex == reader->GetRowIndex() && index < batchSize) {
        if (index >= static_cast<i64>(rows.size())) {
            rows.push_back(reader->MoveRow());
            buffer->CaptureRow(&rows.back());
        } else {
            reader->MoveRow(&rows[index]);
            buffer->CaptureRow(&rows[index]);
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
    struct TThreadData
    {
        TReadManagerBase* Self;
        int ThreadIndex;
    };

    TReadManagerBase(ITableReaderFactoryPtr<TRow> factory, int threadCount)
        : Factory_(std::move(factory))
        , RunningThreadCount_(threadCount)
    {
        ThreadData_.reserve(threadCount);
        for (int i = 0; i < threadCount; ++i) {
            TString threadName = ::TStringBuilder() << "par_reader_" << i;
            auto& data = ThreadData_.emplace_back();
            data.Self = this;
            data.ThreadIndex = i;
            Threads_.push_back(::MakeHolder<TThread>(
                TThread::TParams(ReaderThread, &data).SetName(threadName)));
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
        if (Exception_) {
            std::rethrow_exception(Exception_);
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
    virtual bool ProcessEntry(TReaderEntry<TRow> entry, int threadIndex) = 0;
    virtual void DoStop() = 0;

    void ReaderThread(int threadIndex)
    {
        try {
            while (auto entry = Factory_->GetNextEntry()) {
                if (!ProcessEntry(std::move(*entry), threadIndex)) {
                    break;
                }
            }
        } catch (const std::exception& exception) {
            YT_LOG_ERROR("Exception in parallel reader thread: %v",
                exception.what());
            TGuard<TMutex> guard(Lock_);
            Exception_ = std::current_exception();
            DoStop();
        }
        {
            TGuard<TMutex> guard(Lock_);
            if (RunningThreadCount_ == 1) {
                DoStop();
            }
            --RunningThreadCount_;
            YT_LOG_DEBUG("%v/%v parallel reader threads finished",
                static_cast<int>(Threads_.size()) - RunningThreadCount_,
                Threads_.size());
        }
    }

    static void* ReaderThread(void* opaque)
    {
        auto& data = *static_cast<TThreadData*>(opaque);
        data.Self->ReaderThread(data.ThreadIndex);
        return nullptr;
    }

private:
    const ITableReaderFactoryPtr<TRow> Factory_;

    TVector<TThreadData> ThreadData_;
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
    bool ProcessEntry(TReaderEntry<TRow> entry, int /* threadIndex */) override
    {
        const auto& reader = entry.Reader;
        while (reader->IsValid()) {
            auto buffer = EmptyBuffers_.Pop().GetOrElse(nullptr);
            if (!buffer) {
                return false;
            }
            buffer->TableIndex = entry.TableIndex;
            buffer->FirstRowIndex = reader->GetRowIndex();
            ReadRows(reader, Config_.BatchSize, buffer);
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

    ::NThreading::TBlockingQueue<TReaderBufferPtr<TRow>> EmptyBuffers_;
    ::NThreading::TBlockingQueue<TReaderBufferPtr<TRow>> FilledBuffers_;
};

template <typename TRow>
class TProcessingUnorderedReadManager
    : public TReadManagerBase<TRow>
{
public:
    TProcessingUnorderedReadManager(
        IClientBasePtr client,
        TVector<TRichYPath> paths,
        TParallelReaderRowProcessor<TRow> processor,
        const TUnorderedReadManagerConfig& config,
        const TTableReaderOptions& options)
        : TReadManagerBase<TRow>(
            ::MakeIntrusive<TUnorderedTableReaderFactory<TRow>>(
                std::move(client),
                std::move(paths),
                config,
                options),
            config.ThreadCount)
        , Processor_(std::move(processor))
    { }

    void OnBufferDrained(TReaderBufferPtr<TRow> /* buffer */) override
    {
        Y_FAIL();
    }

private:
    bool ProcessEntry(TReaderEntry<TRow> entry, int /* threadIndex */) override
    {
        Processor_(entry.Reader, entry.TableIndex);
        return true;
    }

    TReaderBufferPtr<TRow> DoGetNextFilledBuffer() override
    {
        Y_FAIL();
    }

    void DoStop() override
    { }

private:
    TParallelReaderRowProcessor<TRow> Processor_;
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
        , FilledBuffers_(0)
    {
        auto batchCountPerThread = Config_.BatchCount / Config_.ThreadCount + 1;
        for (int threadIndex = 0; threadIndex < Config_.ThreadCount; ++threadIndex) {
            auto& bufferQueue = EmptyBuffers_.emplace_back(::MakeHolder<TQueue>(0));
            for (int j = 0; j < batchCountPerThread; ++j) {
                auto buffer = MakeIntrusive<TReaderBuffer<TRow>>();
                buffer->ThreadIndex = threadIndex;
                bufferQueue->Push(std::move(buffer));
            }
        }
    }

    void OnBufferDrained(TReaderBufferPtr<TRow> buffer) override
    {
        EmptyBuffers_[buffer->ThreadIndex]->Push(std::move(buffer));
    }

private:
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

    bool ProcessEntry(TReaderEntry<TRow> entry, int threadIndex) override
    {
        const auto& reader = entry.Reader;
        auto seqNum = entry.SeqNum;

        i64 expectedRangeIndex = 0;

        while (reader->IsValid() || expectedRangeIndex < entry.RangeCount) {
            auto buffer = EmptyBuffers_[threadIndex]->Pop().GetOrElse(nullptr);
            if (!buffer) {
                return false;
            }

            i64 rangeIndex = reader->IsValid() ? reader->GetRangeIndex() : 0;
            i64 rowIndex = reader->IsValid() ? reader->GetRowIndex() : 0;

            buffer->SeqNum = seqNum;
            buffer->TableIndex = entry.TableIndex;
            buffer->FirstRowIndex = rowIndex;

            if (rangeIndex != expectedRangeIndex) {
                YT_LOG_DEBUG("Expected range_index does not exist in the response, it is empty: RangeIndex = %d, ExpectedRangeIndex = %d", rangeIndex, expectedRangeIndex);
                buffer->Rows.resize(0);
            } else {
                ReadRows(reader, Config_.BatchSize, buffer);
            }
            ++expectedRangeIndex;

            seqNum += Config_.ThreadCount;

            if (!FilledBuffers_.Push(std::move(buffer))) {
                return false;
            }
        }
        return true;
    }

    void DoStop() override
    {
        YT_LOG_DEBUG("Finishing ordered parallel read manager; total wait time is %v seconds",
            WaitTime_.SecondsFloat());
        FilledBuffers_.Stop();
        for (auto& queue : EmptyBuffers_) {
            queue->Stop();
        }
    }

private:
    const TOrderedReadManagerConfig Config_;

    TDuration WaitTime_;

    using TQueue = ::NThreading::TBlockingQueue<TReaderBufferPtr<TRow>>;
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

////////////////////////////////////////////////////////////////////////////////

//
// Base class for table reader using several threads to read the table in parallel.
// Readers are produced by the passed factory.
template <typename TDerived, typename TRow>
class TParallelTableReaderBase
    : public TDerived
{
public:
    TParallelTableReaderBase(
        THolder<TReadManagerBase<TRow>> readManager)
        : ReadManager_(std::move(readManager))
    {
        ReadManager_->Start();
        NextBuffer();
        NextNotEmptyBuffer();
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
        NextNotEmptyBuffer();
    }

    ui32 GetTableIndex() const override
    {
        Y_ABORT_UNLESS(CurrentBuffer_);
        return CurrentBuffer_->TableIndex;
    }

    ui32 GetRangeIndex() const override
    {
        Y_FAIL("Not implemented");
    }

    ui64 GetRowIndex() const override
    {
        Y_ABORT_UNLESS(CurrentBuffer_);
        return CurrentBuffer_->FirstRowIndex + (CurrentBufferIt_ - CurrentBuffer_->Rows.begin());
    }

    void NextKey() override
    {
        Y_FAIL("Not implemented");
    }

    ~TParallelTableReaderBase()
    {
        try {
            ReadManager_->Stop();
        } catch (const std::exception& ex) {
            YT_LOG_WARNING("Parallel table reader was finished with exception: %v", ex.what());
        }
    }

private:
    void NextNotEmptyBuffer()
    {
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
        *row = std::move(*CurrentBufferIt_);
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
        static_cast<T&>(*row) = std::move(*this->CurrentBufferIt_);
    }
};

template <>
class TParallelTableReader<TYaMRRow>
    : public TParallelTableReaderBase<IYaMRReaderImpl, TYaMRRow>
{
public:
    using TParallelTableReaderBase<IYaMRReaderImpl, TYaMRRow>::TParallelTableReaderBase;

    const TYaMRRow& GetRow() const override
    {
        return *CurrentBufferIt_;
    }

    void MoveRow(TYaMRRow* row) override
    {
        *row = *CurrentBufferIt_;
    }
};

template <typename TRow>
class IParallelRowReaderImpl
    : public IReaderImplBase
{
public:
    virtual const TRow& GetRow() const = 0;
    virtual void MoveRow(TRow* row) = 0;
};

template <typename TRow>
class TParallelTableReader<TParallelRow<TRow>>
    : public TParallelTableReaderBase<IParallelRowReaderImpl<TRow>, TRow>
{
public:
    using TParallelTableReaderBase<IParallelRowReaderImpl<TRow>, TRow>::TParallelTableReaderBase;

    const TRow& GetRow() const
    {
        return *(this->CurrentBufferIt_);
    }

    void MoveRow(TRow* row)
    {
        *row = std::move(*(this->CurrentBufferIt_));
    }
};

////////////////////////////////////////////////////////////////////////////////

std::pair<IClientBasePtr, TVector<TRichYPath>> CreateRangeReaderClientAndPaths(
    const IClientBasePtr& client,
    const TVector<TRichYPath>& paths,
    bool createTransaction);

i64 EstimateTableRowWeight(const IClientBasePtr& client, const TVector<TRichYPath>& paths);

template <typename TRow>
THolder<TReadManagerBase<TRow>> CreateReadManager(
    const IClientBasePtr& client,
    const TVector<TRichYPath>& paths,
    TParallelReaderRowProcessor<TRow> processor,
    const TParallelTableReaderOptions& options)
{
    Y_ENSURE_EX(
        !paths.empty(),
        TApiUsageError() << "Parallel table reader: paths should not be empty");
    Y_ENSURE_EX(
        options.ThreadCount_ >= 1,
        TApiUsageError() << "ThreadCount can not be zero");
    Y_ENSURE_EX(
        !processor || !options.Ordered_,
        TApiUsageError() << "ReadTableInParallel can be called only in unordered mode");

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

    if (processor) {
        TUnorderedReadManagerConfig config;
        config.ThreadCount = options.ThreadCount_;
        config.BatchSize = batchSize;
        config.BatchCount = batchCount;
        YT_LOG_DEBUG("Starting unordered processing parallel reader: "
            "ThreadCount = %d, BatchSize = %d, BatchCount = %d",
            config.ThreadCount,
            config.BatchSize,
            config.BatchCount);
        return ::MakeHolder<TProcessingUnorderedReadManager<TRow>>(
            std::move(rangeReaderClient),
            std::move(rangeReaderPaths),
            std::move(processor),
            config,
            rangeReaderOptions);
    }

    THolder<TReadManagerBase<TRow>> readManager;
    if (options.Ordered_) {
        TOrderedReadManagerConfig config;
        config.ThreadCount = options.ThreadCount_;
        config.BatchSize = batchSize;
        config.BatchCount = batchCount;
        config.RangeCount = options.RangeCount_;
        YT_LOG_DEBUG("Starting ordered parallel reader: "
            "ThreadCount = %d, BatchSize = %d, BatchCount = %d, RangeCount = %d",
            config.ThreadCount,
            config.BatchSize,
            config.BatchCount,
            config.RangeCount);
        return ::MakeHolder<TOrderedReadManager<TRow>>(
            std::move(rangeReaderClient),
            std::move(rangeReaderPaths),
            config,
            rangeReaderOptions);
    } else {
        TUnorderedReadManagerConfig config;
        config.ThreadCount = options.ThreadCount_;
        config.BatchSize = batchSize;
        config.BatchCount = batchCount;
        YT_LOG_DEBUG("Starting unordered parallel reader: "
            "ThreadCount = %d, BatchSize = %d, BatchCount = %d",
            config.ThreadCount,
            config.BatchSize,
            config.BatchCount);
        return ::MakeHolder<TUnorderedReadManager<TRow>>(
            std::move(rangeReaderClient),
            std::move(rangeReaderPaths),
            config,
            rangeReaderOptions);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

template <class T>
struct TRowTraits<TParallelRow<T>>
{
    using TRowType = T;
    using IReaderImpl = NDetail::IParallelRowReaderImpl<T>;
};

template <typename T>
class TTableReader<TParallelRow<T>>
    : public NDetail::TSimpleTableReader<TParallelRow<T>>
{
    using NDetail::TSimpleTableReader<TParallelRow<T>>::TSimpleTableReader;
};

template <typename TRow>
using TParallelTableReaderPtr = TTableReaderPtr<TParallelRow<TRow>>;

template <typename TRow>
auto CreateParallelTableReader(
    const IClientBasePtr& client,
    const TVector<TRichYPath>& paths,
    const TParallelTableReaderOptions& options)
{
    auto readManager = NDetail::CreateReadManager<TRow>(
        client,
        paths,
        TParallelReaderRowProcessor<TRow>{},
        options);
    if constexpr (TIsSkiffRow<TRow>::value) {
        return ::MakeIntrusive<TTableReader<TParallelRow<TRow>>>(
            ::MakeIntrusive<NDetail::TParallelTableReader<TParallelRow<TRow>>>(std::move(readManager)));
    } else {
        return ::MakeIntrusive<TTableReader<TRow>>(
            ::MakeIntrusive<NDetail::TParallelTableReader<TRow>>(std::move(readManager)));
    }
}

template <typename TRow>
auto CreateParallelTableReader(
    const IClientBasePtr& client,
    const TRichYPath& path,
    const TParallelTableReaderOptions& options)
{
    return CreateParallelTableReader<TRow>(client, TVector<TRichYPath>{path}, options);
}

template <typename TRow>
void ReadTablesInParallel(
    const IClientBasePtr& client,
    const TVector<TRichYPath>& paths,
    TParallelReaderRowProcessor<TRow> processor,
    const TParallelTableReaderOptions& options)
{
    auto readManager = NDetail::CreateReadManager<TRow>(
        client,
        paths,
        std::move(processor),
        options);
    readManager->Start();
    readManager->Stop();
}

template <typename TRow>
void ReadTableInParallel(
    const IClientBasePtr& client,
    const TRichYPath& path,
    TParallelReaderRowProcessor<TRow> processor,
    const TParallelTableReaderOptions& options)
{
    ReadTablesInParallel(client, TVector<TRichYPath>{path}, std::move(processor), options);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
