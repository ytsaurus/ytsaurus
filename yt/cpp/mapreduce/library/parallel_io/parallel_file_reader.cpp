#include "parallel_file_reader.h"

#include <yt/cpp/mapreduce/interface/config.h>

#include <yt/yt/core/actions/future.h>

#include <library/cpp/iterator/functools.h>
#include <library/cpp/threading/blocking_queue/blocking_queue.h>
#include <library/cpp/threading/future/async.h>

#include <util/system/mutex.h>
#include <util/system/thread.h>
#include <util/system/filemap.h>
#include <util/thread/pool.h>

namespace NYT {
namespace NDetail {

using ::TBlob;

////////////////////////////////////////////////////////////////////////////////

size_t GetFileSize(const NYT::TYPath& path, const NYT::ITransactionPtr& transaction)
{
    return transaction->Get(path + "/@uncompressed_data_size").ConvertTo<size_t>();
}

struct TRange
{
    size_t Begin;
    size_t End;

    size_t Length() const
    {
        Y_VERIFY(Begin <= End);
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

class TParallelFileReader
    : public IParallelFileReader
{
public:
    TParallelFileReader(
        const IClientBasePtr& client,
        const TRichYPath& path,
        const std::shared_ptr<IThreadPool>& threadPool,
        const TParallelFileReaderOptions& options);

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
    TBlob ReadJob(const TRange& range);

private:
    const TParallelFileReaderOptions Options_;
    const TRichYPath Path_;

    std::shared_ptr<IThreadPool> ThreadPool_;
    TThread Supervisor_{std::bind(&TParallelFileReader::SupervisorJob, this)};

    TAtomicExceptionPtr ReadJobException_;

    ::NThreading::TBlockingQueue<std::pair<::NThreading::TFuture<TBlob>, TResourceGuard>> Batches_{0};
    std::optional<TBlob> BatchTail_;

    std::atomic<bool> Initialized_ = false;

    ITransactionPtr Transaction_;
    ::TIntrusivePtr<TResourceLimiter> RamLimiter_;

    /// Optional because FileSize/LockedPath_ is known after @ref LazyInit()
    /// @note If length is set in options then FileSize == Length
    std::optional<size_t> FileSize_;
    std::optional<TYPath> LockedPath_;
};

////////////////////////////////////////////////////////////////////////////////

TParallelFileReader::TParallelFileReader(
    const IClientBasePtr& client,
    const TRichYPath& path,
    const std::shared_ptr<IThreadPool>& threadPool,
    const TParallelFileReaderOptions& options)
    : Options_(options)
    , Path_(path)
    , ThreadPool_(threadPool)
    , Transaction_(client->StartTransaction())
    , RamLimiter_(options.RamLimiter_)
{
    constexpr size_t DefaultRamLimit = 2_GB;

    if (Options_.RamLimiter_ == nullptr){
        RamLimiter_ = MakeIntrusive<TResourceLimiter>(DefaultRamLimit);
    }
    // Otherwise we will deadlock on trying to assign job.
    Y_ENSURE(Options_.BatchSize_ <= RamLimiter_->GetLimit());
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
    Y_VERIFY(FileSize_);
    TSplitter splitter(
        FileSize_.value(),
        Options_.BatchSize_,
        Options_.ReaderOptions_.GetOrElse({}).Offset_.GetOrElse(0));
    while (auto range = splitter.Next()) {
        TResourceGuard guard(RamLimiter_, range->Length());
        if (ReadJobException_.GetException()) {
            break;
        }
        ::NThreading::TFuture<::TBlob> future = ::NThreading::Async(
            [this, range = std::move(*range)]() -> TBlob {
                return ReadJob(range);
            },
            *ThreadPool_);
        Batches_.Push({std::move(future), std::move(guard)});
    }
    Batches_.Stop();
}

TBlob TParallelFileReader::ReadJob(const TRange& range)
{
    if (auto ex = ReadJobException_.GetException()) {
        std::rethrow_exception(ex);
    }
    try {
        auto options = Options_.ReaderOptions_.GetOrElse({});
        options.Offset(range.Begin);
        options.Length(range.Length());
        Y_VERIFY(LockedPath_);
        auto reader = Transaction_->CreateFileReader(LockedPath_.value(), options);
        auto data = reader->ReadAll();

        return TBlob::FromString(data);
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

    auto lockPtr = Transaction_->Lock(Path_.Path_, ELockMode::LM_SNAPSHOT);
    auto lockedNodeId = lockPtr->GetLockedNodeId();
    LockedPath_ = "#" + lockedNodeId.AsGuidString();

    if (Options_.ReaderOptions_.GetOrElse({}).Length_) {
        FileSize_ = *Options_.ReaderOptions_->Length_;
    } else {
        FileSize_ = GetFileSize(LockedPath_.value(), Transaction_);
    }

    Supervisor_.Start();

    Initialized_ = true;
}

size_t TParallelFileReader::DoReadWithCallback(void* ptr, size_t size, DoReadCallback&& callback)
{
    LazyInit();
    size_t curIdx = 0;
    std::optional<TBlob> curBlob;

    while (curBlob = ReadNextBatch()) {
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
        size_t prevIdx = curIdx - curBlob->Size();
        Y_VERIFY(!BatchTail_);
        Y_VERIFY(curBlob.has_value());

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
    Y_VERIFY(blob.Size() == result->second.GetLockedAmount());
    return blob;
}

TSplitter::TSplitter(size_t length, size_t batchSize, size_t offset)
    : Offset_(offset)
    , Length_(offset + length)
    , BatchSize_(batchSize)
{
    Y_VERIFY(length > 0 && batchSize > 0);
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
    return ::MakeIntrusive<NDetail::TParallelFileReader>(client, path, threadPool, options);
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

} // namespace NYT
