#include "parallel_file_reader.h"

#include <yt/cpp/mapreduce/common/helpers.h>

#include <yt/cpp/mapreduce/interface/config.h>

#include <library/cpp/iterator/functools.h>
#include <library/cpp/threading/blocking_queue/blocking_queue.h>
#include <library/cpp/threading/future/async.h>

#include <util/string/builder.h>

#include <util/system/mutex.h>
#include <util/system/thread.h>
#include <util/system/filemap.h>

#include <util/thread/pool.h>

namespace NYT {
namespace NDetail {

using ::TBlob;

////////////////////////////////////////////////////////////////////////////////

constexpr size_t DefaultRamLimit = 2_GB;

size_t GetFileSize(const NYT::TYPath& path, const NYT::IClientBasePtr& client)
{
    return client->Get(path + "/@uncompressed_data_size").ConvertTo<size_t>();
}

IResourceLimiterPtr GetRamLimiter(const IResourceLimiterPtr& ramLimiter, const TString& name)
{
    if (ramLimiter) {
        return ramLimiter;
    }
    return ::MakeIntrusive<TResourceLimiter>(DefaultRamLimit, name);
}

struct TRange
{
    size_t Begin;
    size_t End;

    size_t Length() const
    {
        Y_ABORT_UNLESS(Begin <= End);
        return End - Begin;
    }
};

class TSplitter
{
public:
    TSplitter(size_t length, size_t batchSize, size_t offset = 0);

    std::optional<TRange> Next();

private:
    size_t Offset_;
    size_t Length_;
    size_t BatchSize_;
};

/// @brief Emulate std::atomic<std::exception_ptr> that can be changed only once
class TAtomicExceptionPtr
{
public:
    bool TrySetException(std::exception&& ex);
    bool TrySetException(const std::exception_ptr& ptr);

    std::exception_ptr GetException() const;

private:
    std::exception_ptr Exception_ = nullptr;
    std::atomic<bool> HasException_ = false;
    TMutex Mutex_;
};

////////////////////////////////////////////////////////////////////////////////

struct TReadTask
{
    size_t Size;
    std::function<TBlob()> Read;
};

struct IReadTaskGenerator
{
    virtual ~IReadTaskGenerator() = default;

    virtual void Initialize() = 0;
    virtual std::optional<TReadTask> NextTask() = 0;
};

class TFileRangeTaskGenerator
    : public IReadTaskGenerator
{
public:
    TFileRangeTaskGenerator(IClientBasePtr client, TRichYPath path, TParallelFileReaderOptions options)
        : Client_(std::move(client))
        , Path_(std::move(path))
        , Options_(std::move(options))
    { }

    void Initialize() override
    {
        if (Options_.CreateTransaction_) {
            auto tx = Client_->StartTransaction();
            auto lockPtr = tx->Lock(Path_.Path_, ELockMode::LM_SNAPSHOT);
            auto lockedNodeId = lockPtr->GetLockedNodeId();
            ToReadPath_ = "#" + lockedNodeId.AsGuidString();
            Client_ = tx;
        } else {
            ToReadPath_ = Path_.Path_;
        }

        auto fileSize = GetFileSize(ToReadPath_, Client_);
        auto readerOptions = Options_.ReaderOptions_.GetOrElse({});
        if (readerOptions.Length_) {
            fileSize = std::min<i64>(fileSize, *readerOptions.Length_);
        }
        if (fileSize > 0) {
            Splitter_.emplace(fileSize, Options_.BatchSize_, readerOptions.Offset_);
        }
    }

    std::optional<TReadTask> NextTask() override
    {
        if (!Splitter_) {
            return std::nullopt;
        }
        auto range = Splitter_->Next();
        if (!range) {
            return std::nullopt;
        }
        return TReadTask{
            .Size = range->Length(),
            .Read = [this, range = *range] {
                return ReadRange(range);
            },
        };
    }

private:
    TBlob ReadRange(const TRange& range)
    {
        auto options = Options_.ReaderOptions_.GetOrElse({});
        options.Offset(range.Begin);
        options.Length(range.Length());
        auto reader = Client_->CreateFileReader(ToReadPath_, options);
        return TBlob::FromString(reader->ReadAll());
    }

private:
    IClientBasePtr Client_;
    const TRichYPath Path_;
    const TParallelFileReaderOptions Options_;

    TYPath ToReadPath_;
    std::optional<TSplitter> Splitter_;
};

class TFilePartitionTaskGenerator
    : public IReadTaskGenerator
{
public:
    TFilePartitionTaskGenerator(IClientBasePtr client, TVector<TFilePartition> partitions, TParallelFilePartitionReaderOptions options)
        : Client_(std::move(client))
        , Partitions_(std::move(partitions))
        , Options_(std::move(options))
    { }

    void Initialize() override
    { }

    std::optional<TReadTask> NextTask() override
    {
        while (NextIndex_ < std::ssize(Partitions_) && Partitions_[NextIndex_].Length == 0) {
            ++NextIndex_;
        }
        if (NextIndex_ >= std::ssize(Partitions_)) {
            return std::nullopt;
        }
        const auto& partition = Partitions_[NextIndex_++];
        return TReadTask{
            .Size = static_cast<size_t>(partition.Length),
            .Read = [this, &partition] {
                return ReadPartition(partition);
            },
        };
    }

private:
    TBlob ReadPartition(const TFilePartition& partition)
    {
        auto reader = Client_->CreateFilePartitionReader(partition.Cookie, Options_.ReaderOptions_.GetOrElse({}));
        auto data = reader->ReadAll();
        Y_ENSURE(
            std::ssize(data) == partition.Length,
            "File partition read returned " << data.size() << " bytes instead of " << partition.Length);
        return TBlob::FromString(std::move(data));
    }

private:
    const IClientBasePtr Client_;
    const TVector<TFilePartition> Partitions_;
    const TParallelFilePartitionReaderOptions Options_;

    int NextIndex_ = 0;
};

////////////////////////////////////////////////////////////////////////////////

class TParallelFileReader
    : public IParallelFileReader
{
public:
    TParallelFileReader(
        std::unique_ptr<IReadTaskGenerator> generator,
        std::shared_ptr<IThreadPool> threadPool,
        IResourceLimiterPtr ramLimiter,
        size_t maxTaskSize);

    ~TParallelFileReader();

    std::optional<TBlob> ReadNextBatch() override;

protected:
    size_t DoRead(void* buf, size_t len) override;

    size_t DoSkip(size_t len) override;

private:
    using DoReadCallback = std::function<void(void* dst, const void* src, size_t size)>;
    size_t DoReadWithCallback(void* buf, size_t len, DoReadCallback&& callback);

    void LazyInit();

    void SupervisorJob() noexcept;
    TBlob RunTask(const TReadTask& task);

private:
    const std::unique_ptr<IReadTaskGenerator> Generator_;
    const std::shared_ptr<IThreadPool> ThreadPool_;
    const IResourceLimiterPtr RamLimiter_;

    TThread Supervisor_{std::bind(&TParallelFileReader::SupervisorJob, this)};

    TAtomicExceptionPtr ReadJobException_;

    ::NThreading::TBlockingQueue<std::pair<::NThreading::TFuture<TBlob>, TResourceGuard>> Batches_{0};
    std::optional<TBlob> BatchTail_;

    std::atomic<bool> Initialized_ = false;
};

////////////////////////////////////////////////////////////////////////////////

TParallelFileReader::TParallelFileReader(
    std::unique_ptr<IReadTaskGenerator> generator,
    std::shared_ptr<IThreadPool> threadPool,
    IResourceLimiterPtr ramLimiter,
    size_t maxTaskSize)
    : Generator_(std::move(generator))
    , ThreadPool_(std::move(threadPool))
    , RamLimiter_(std::move(ramLimiter))
{
    // Otherwise we will deadlock on trying to assign job.
    Y_ENSURE(maxTaskSize <= RamLimiter_->GetLimit());
}

TParallelFileReader::~TParallelFileReader()
{
    ReadJobException_.TrySetException(yexception()  << "Called TParallelFileReader destructor!");
    Batches_.Stop();
    while (auto future = Batches_.Pop()) {
        future->first.Wait();
    }

    if (Initialized_){
        Supervisor_.Join();
    }
}

void TParallelFileReader::SupervisorJob() noexcept
{
    while (auto task = Generator_->NextTask()) {
        TResourceGuard guard(RamLimiter_, task->Size);
        if (ReadJobException_.GetException()) {
            break;
        }
        ::NThreading::TFuture<::TBlob> future = ::NThreading::Async(
            [this, task = std::move(*task)] () -> TBlob {
                return RunTask(task);
            },
            *ThreadPool_);
        Batches_.Push({std::move(future), std::move(guard)});
    }
    Batches_.Stop();
}

TBlob TParallelFileReader::RunTask(const TReadTask& task)
{
    if (auto ex = ReadJobException_.GetException()) {
        std::rethrow_exception(ex);
    }
    try {
        return task.Read();
    } catch (...) {
        ReadJobException_.TrySetException(std::current_exception());
        auto ex = ReadJobException_.GetException();
        std::rethrow_exception(ex);
    }
}

void TParallelFileReader::LazyInit()
{
    if (Initialized_) {
        return;
    }

    Generator_->Initialize();
    Supervisor_.Start();

    Initialized_ = true;
}

size_t TParallelFileReader::DoReadWithCallback(void* ptr, size_t size, DoReadCallback&& callback)
{
    LazyInit();
    size_t curIdx = 0;
    std::optional<TBlob> curBlob;

    for (;;) {
        curBlob = ReadNextBatch();
        if (!curBlob) {
            break;
        }
        if (curIdx + curBlob->Size() <= size) {
            callback(reinterpret_cast<uint8_t*>(ptr) + curIdx, curBlob->Data(), curBlob->Size());
            curIdx += curBlob->Size();
            if (curIdx == size) {
                break;
            }
        } else {
            callback(reinterpret_cast<uint8_t*>(ptr) + curIdx, curBlob->Data(), size - curIdx);
            curIdx += curBlob->Size();
            break;
        }
    }

    if (curIdx <= size) {
        return curIdx;
    } else {
        Y_ABORT_UNLESS(!BatchTail_);
        Y_ABORT_UNLESS(curBlob.has_value());
        size_t prevIdx = curIdx - curBlob->Size();

        BatchTail_ = curBlob->SubBlob(size - prevIdx, curBlob->Size());

        return size;
    }
}

size_t TParallelFileReader::DoSkip(size_t len)
{
    return DoReadWithCallback(nullptr, len, [](void*, const void*, size_t) {});
}

size_t TParallelFileReader::DoRead(void* buf, size_t len)
{
    return DoReadWithCallback(buf, len, [](void* src, const void* dst, size_t size) {
        std::memcpy(src, dst, size);
    });
}

std::optional<TBlob> TParallelFileReader::ReadNextBatch()
{
    LazyInit();

    if (BatchTail_) {
        return std::move(*std::exchange(BatchTail_, std::nullopt));
    }

    auto result = Batches_.Pop();
    if (!result) {
        return std::nullopt;
    }
    auto blob = result->first.ExtractValueSync();
    Y_ABORT_UNLESS(blob.Size() == result->second.GetLockedAmount());
    return blob;
}

TSplitter::TSplitter(size_t length, size_t batchSize, size_t offset)
    : Offset_(offset)
    , Length_(offset + length)
    , BatchSize_(batchSize)
{
    Y_ABORT_UNLESS(length > 0 && batchSize > 0);
}

std::optional<TRange> TSplitter::Next()
{
    if (Offset_ >= Length_) {
        return std::nullopt;
    }

    auto range = TRange{Offset_, std::min(Offset_ + BatchSize_, Length_)};
    Offset_ += BatchSize_;
    return range;
}

bool TAtomicExceptionPtr::TrySetException(std::exception&& ex)
{
    with_lock(Mutex_) {
        if (HasException_.load()) {
            return false;
        }
        Exception_ = std::make_exception_ptr(std::move(ex));
        HasException_.store(true);
        return true;
    }
}

bool TAtomicExceptionPtr::TrySetException(const std::exception_ptr& ptr)
{
    with_lock(Mutex_) {
        if (HasException_.load()) {
            return false;
        }
        Exception_ = ptr;
        HasException_.store(true);
        return true;
    }
}

std::exception_ptr TAtomicExceptionPtr::GetException() const
{
    if (HasException_.load()) {
        return Exception_;
    }
    return nullptr;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

void SaveFileParallel(
    const IClientBasePtr& client,
    const TRichYPath& path,
    const TString& localPath,
    const TParallelFileReaderOptions& options)
{
    auto reader = CreateParallelFileReader(client, path, options);
    TFileOutput file(localPath);
    reader->ReadAll(file);
}

////////////////////////////////////////////////////////////////////////////////

::TIntrusivePtr<IParallelFileReader> CreateParallelFileReader(
    const IClientBasePtr& client,
    const TRichYPath& path,
    const std::shared_ptr<IThreadPool>& threadPool,
    const TParallelFileReaderOptions& options)
{
    auto ramLimiter = NDetail::GetRamLimiter(
        options.RamLimiter_,
        ::TStringBuilder() << "TParallelFileReader[" << NodeToYsonString(PathToNode(path)) << "]");
    return ::MakeIntrusive<NDetail::TParallelFileReader>(
        std::make_unique<NDetail::TFileRangeTaskGenerator>(client, path, options),
        threadPool,
        std::move(ramLimiter),
        options.BatchSize_);
}

////////////////////////////////////////////////////////////////////////////////

::TIntrusivePtr<IParallelFileReader> CreateParallelFileReader(
    const IClientBasePtr& client,
    const TRichYPath& path,
    const TParallelFileReaderOptions& options)
{
    auto threadPool = std::make_shared<TSimpleThreadPool>();
    threadPool->Start(options.ThreadCount_);
    return CreateParallelFileReader(client, path, threadPool, options);
}

////////////////////////////////////////////////////////////////////////////////

::TIntrusivePtr<IParallelFileReader> CreateParallelFilePartitionReader(
    const IClientBasePtr& client,
    const TVector<TFilePartition>& partitions,
    const std::shared_ptr<IThreadPool>& threadPool,
    const TParallelFilePartitionReaderOptions& options)
{
    size_t maxPartitionLength = 0;
    for (const auto& partition : partitions) {
        Y_ENSURE(partition.Length >= 0, "File partition length must be non-negative, got " << partition.Length);
        maxPartitionLength = std::max<size_t>(maxPartitionLength, partition.Length);
    }
    auto ramLimiter = NDetail::GetRamLimiter(
        options.RamLimiter_,
        ::TStringBuilder() << "TParallelFilePartitionReader[" << partitions.size() << " partitions]");
    return ::MakeIntrusive<NDetail::TParallelFileReader>(
        std::make_unique<NDetail::TFilePartitionTaskGenerator>(client, partitions, options),
        threadPool,
        std::move(ramLimiter),
        maxPartitionLength);
}

////////////////////////////////////////////////////////////////////////////////

::TIntrusivePtr<IParallelFileReader> CreateParallelFilePartitionReader(
    const IClientBasePtr& client,
    const TVector<TFilePartition>& partitions,
    const TParallelFilePartitionReaderOptions& options)
{
    auto threadPool = std::make_shared<TSimpleThreadPool>();
    threadPool->Start(options.ThreadCount_);
    return CreateParallelFilePartitionReader(client, partitions, threadPool, options);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
