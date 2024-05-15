#include "parallel_file_writer.h"

#include <yt/cpp/mapreduce/common/helpers.h>

#include <yt/cpp/mapreduce/interface/config.h>

#include <library/cpp/iterator/functools.h>

#include <library/cpp/threading/blocking_queue/blocking_queue.h>
#include <library/cpp/threading/future/future.h>
#include <library/cpp/threading/future/async.h>

#include <util/generic/guid.h>

#include <util/string/builder.h>

#include <util/system/fstat.h>
#include <util/system/info.h>
#include <util/system/mutex.h>
#include <util/system/thread.h>
#include <util/generic/size_literals.h>

#include <util/thread/pool.h>

namespace NYT {
namespace NDetail {

using ::ToString;

////////////////////////////////////////////////////////////////////////////////

class TParallelFileWriter
    : public IParallelFileWriter
{
public:
    /// @brief Write file in parallel.
    /// @param client       Client which used for write file on server.
    /// @param fileName     Source path to file in local storage.
    /// @param path         Dist path to file on server.
    TParallelFileWriter(
        const IClientBasePtr& client,
        const TRichYPath& path,
        const std::shared_ptr<IThreadPool>& threadPool,
        const TParallelFileWriterOptions& options);

    ~TParallelFileWriter();

    void Write(TSharedRef blob) override;

    void WriteFile(const TString& fileName) override;

    void Finish() override;

private:
    struct IWriteTask;

    void DoFinish(bool commit);

    void DoWriteTask(
        std::unique_ptr<IWriteTask>&& task,
        const TRichYPath& filePath,
        std::optional<TResourceGuard> &&preallocatedGuard);

    TRichYPath CreateFilePath(const std::pair<size_t, size_t>& taskId);

    static void* ThreadWrite(void* opaque);

    void ThreadWrite(int index, i64 start, i64 length);

    void ThrowIfDead();

    TResourceGuard LockRamForTask(const std::unique_ptr<IWriteTask>& task);

private:
    struct IWriteTask
    {
        virtual ~IWriteTask() = default;
        virtual void Write(const IFileWriterPtr& writer, std::atomic<bool>& hasException) = 0;
        virtual size_t GetDataSize() const = 0;
    };

    struct TBlobWriteTask
        : public IWriteTask
    {
        TBlobWriteTask(TSharedRef blob)
            : Blob(std::move(blob))
        { }

        void Write(const IFileWriterPtr& writer, std::atomic<bool>& /*hasException*/) override
        {
            writer->Write(Blob.begin(), Blob.size());
        }

        size_t GetDataSize() const override
        {
            return Blob.Size();
        }

        TSharedRef Blob;
    };

    class TFileWriteTask
        : public IWriteTask
    {
    public:
        TFileWriteTask(TString fileName, i64 startPosition, i64 length)
            : FileName_(std::move(fileName))
            , StartPosition_(startPosition)
            , Length_(length)
        { }

        void Write(const IFileWriterPtr& writer, std::atomic<bool>& hasException) override
        {
            TFile file(FileName_, RdOnly);
            file.Seek(StartPosition_, SeekDir::sSet);

            TFileInput input(file);
            while (Length_ > 0 && !hasException) {
                void *buffer;
                i64 bufSize = input.Next(&buffer);
                if (Length_ < bufSize) {
                    bufSize = Length_;
                }
                writer->Write(buffer, bufSize);
                Length_ -= bufSize;
            }
        }

        size_t GetDataSize() const override
        {
            return Length_;
        }

    private:
        TString FileName_;
        i64 StartPosition_;
        i64 Length_;
    };

    struct TTaskDescription
    {
        TRichYPath Path;
        // First value is original blobId, second - split inside this blob.
        std::pair<size_t, size_t> Order;
    };

private:
    const std::shared_ptr<IThreadPool> ThreadPool_;
    const TParallelFileWriterOptions Options_;
    const TRichYPath Path_;
    const TString TemporaryDirectory_;

    ITransactionPtr Transaction_;
    IResourceLimiterPtr RamLimiter_;
    TMutex ExceptionLock_;
    std::exception_ptr Exception_ = nullptr;
    std::atomic<bool> HasException_ = false;
    std::atomic<bool> Finished_ = false;
    std::vector<TTaskDescription> Tasks_;
    std::vector<NThreading::TFuture<void>> Futures_;
    size_t NextBlobId_ = 0;

    // Temporary debug code to investigate YT-21457
    std::atomic<i64> TaskCount_ = 0;
};

////////////////////////////////////////////////////////////////////////////////

TParallelFileWriter::TParallelFileWriter(
    const IClientBasePtr &client,
    const TRichYPath& path,
    const std::shared_ptr<IThreadPool>& threadPool,
    const TParallelFileWriterOptions& options)
    : ThreadPool_(threadPool)
    , Options_(options)
    , Path_(path)
    , TemporaryDirectory_(Options_.TmpDirectory_
        ? *Options_.TmpDirectory_ + "/" + CreateGuidAsString()
        : Path_.Path_ + "__ParallelFileWriter__/" + CreateGuidAsString())
    , Transaction_(client->StartTransaction())
    , RamLimiter_(options.RamLimiter_)
{
    Transaction_->Create(TemporaryDirectory_, NYT::NT_MAP, NYT::TCreateOptions().Recursive(true));
    static constexpr size_t DefaultRamLimit = 2_GB;

    if (RamLimiter_ == nullptr) {
        RamLimiter_ = MakeIntrusive<TResourceLimiter>(DefaultRamLimit, ::TStringBuilder() << "TParallelFileWriter[" << NodeToYsonString(PathToNode(Path_)) << "]");
    }

    // Otherwise we will deadlock on trying to assign job.
    Y_ENSURE(Options_.MaxBlobSize_ <= RamLimiter_->GetLimit());
}

TParallelFileWriter::~TParallelFileWriter()
{
    if (Options_.AutoFinish_) {
        NDetail::FinishOrDie(this, /*autoFinish*/ true, "TParallelFileWriter");
    } else {
        try {
            DoFinish(false);
        } catch (...) {
        }
    }
}

TResourceGuard TParallelFileWriter::LockRamForTask(const std::unique_ptr<IWriteTask>& task)
{
    size_t lockAmount = task->GetDataSize();
    if (Options_.AcquireRamForBuffers_) {
        // fixme cannot properly do it without heuristic
        // exact lock amount for inner buffer will require us to create writer
        // AFTER locking
        lockAmount *= 2;
    }
    return TResourceGuard(RamLimiter_, lockAmount);
}

void TParallelFileWriter::DoWriteTask(
    std::unique_ptr<IWriteTask>&& task,
    const TRichYPath& filePath,
    std::optional<TResourceGuard>&& guard)
{
    if (HasException_) {
        return;
    }

    try {
        auto currentGuard = [&]() -> TResourceGuard {
            if (guard.has_value()) {
                return std::move(*guard);
            }
            return LockRamForTask(task);
        }();
        if (HasException_) {
            // fail-fast after unlocking
            return;
        }
        Transaction_->Create(filePath.Path_, NT_FILE, TCreateOptions().Attributes(Options_.FileAttributes_));
        IFileWriterPtr writer = Transaction_->CreateFileWriter(filePath, TFileWriterOptions().WriterOptions(Options_.WriterOptions_.GetOrElse({})));
        // DO NOT take additional softLimiter lock here for difference betweeh heuristic and exact buffer size!
        // It's deadlockable scheme!
        task->Write(writer, HasException_);
        writer->Finish();
    } catch (const std::exception& e) {
        auto guard = Guard(ExceptionLock_);
        HasException_ = true;
        if (!Exception_) {
            Exception_ = std::current_exception();
        }
        try {
            Transaction_->Abort();
        } catch (...) {
            // Never mind if tx is already dead - we won't commit it anyway => no data will be written.
        }
    }
}

TRichYPath TParallelFileWriter::CreateFilePath(const std::pair<size_t, size_t>& taskId)
{
    TString filePathStr = TemporaryDirectory_ + "/" + "__ParallelFileWriter__" + ToString(taskId.first) + "_" + ToString(taskId.second);
    auto filePath = Path_;
    filePath.Path(filePathStr).Append(false);
    return filePath;
}

void TParallelFileWriter::Write(TSharedRef blob)
{
    ThrowIfDead();
    Y_ABORT_IF(Finished_, "Tried to push blob to already finished writer");

    auto blobId = NextBlobId_++;

    for (const auto& [subBlobId, subBlob] : Enumerate(blob.Split(Options_.MaxBlobSize_))) {
        auto taskId = std::pair(blobId, subBlobId);
        auto filePath = CreateFilePath(taskId);

        std::unique_ptr<IWriteTask> task = std::make_unique<TBlobWriteTask>(std::move(subBlob));
        // Lock memory before scheduling future since our writer already "owns" blob
        // It already consumes memory
        auto ramGuard = LockRamForTask(task);
        auto future = NThreading::Async([this, task=std::move(task), ramGuard=std::move(ramGuard), filePath]() mutable {
            DoWriteTask(std::move(task), filePath, std::move(ramGuard));
        }, *ThreadPool_);
        ++TaskCount_;

        Tasks_.push_back({
            .Path = std::move(filePath),
            .Order = std::move(taskId)
        });
        Futures_.push_back(std::move(future));
    }
}

void TParallelFileWriter::WriteFile(const TString& fileName)
{
    ThrowIfDead();
    Y_ABORT_IF(Finished_, "Tried to push file to already finished writer");

    auto blobId = NextBlobId_++;

    auto length = GetFileLength(fileName);
    size_t subBlobId = 0;
    for (i64 pos = 0; pos < length; pos += Options_.MaxBlobSize_, ++subBlobId) {
        auto taskId = std::pair(blobId, subBlobId);
        auto filePath = CreateFilePath(taskId);

        i64 begin = pos;
        i64 end = std::min(begin + static_cast<i64>(Options_.MaxBlobSize_), length);

        // Unlike in Write, we do not lock resource limiter inside this "job scheduler" function
        // since our TFileWriteTask will be materialized in memory only inside thread,
        // where we read chunk of data

        auto task = std::make_unique<TFileWriteTask>(fileName, begin, end - begin);
        auto future = NThreading::Async([this, task=std::move(task), filePath]() mutable {
            DoWriteTask(std::move(task), filePath, std::nullopt);
        }, *ThreadPool_);
        ++TaskCount_;

        Tasks_.push_back({
            .Path = std::move(filePath),
            .Order = std::move(taskId)
        });
        Futures_.push_back(std::move(future));
    }
}

void TParallelFileWriter::ThrowIfDead() {
    if (HasException_) {
        auto guard = Guard(ExceptionLock_);
        std::rethrow_exception(Exception_);
    }
}

void TParallelFileWriter::Finish()
{
    DoFinish(true);
}

void TParallelFileWriter::DoFinish(bool commit)
{
    if (Finished_) {
        return;
    }
    Finished_ = true;

    NThreading::WaitAll(Futures_).GetValueSync();
    Y_ABORT_IF(std::ssize(Futures_) != TaskCount_.load());
    for (const auto& f : Futures_) {
        Y_ABORT_IF(!f.HasValue() && !f.HasException());
    }

    if (Exception_ || !commit) {
        Transaction_->Abort();
        if (!commit) {
            return;
        }
        if (Exception_) {
            std::rethrow_exception(Exception_);
        }
    }
    auto createOptions = TCreateOptions().Attributes(Options_.FileAttributes_);
    auto concatenateOptions = TConcatenateOptions();
    if (Path_.Append_.GetOrElse(false)) {
        createOptions.IgnoreExisting(true);
        concatenateOptions.Append(true);
    } else {
        createOptions.Force(true);
        concatenateOptions.Append(false);
    }
    Transaction_->Create(Path_.Path_, NT_FILE, createOptions);

    Sort(Tasks_, [] (const TTaskDescription& lhs, const TTaskDescription& rhs) {
        return lhs.Order < rhs.Order;
    });

    TVector<TYPath> tempPaths;
    tempPaths.reserve(Tasks_.size());
    for (const auto& task : Tasks_) {
        tempPaths.emplace_back(task.Path.Path_);
    }

    Transaction_->Concatenate(tempPaths, Path_.Path_, concatenateOptions);
    Transaction_->Remove(TemporaryDirectory_, NYT::TRemoveOptions().Recursive(true).Force(true));
    Transaction_->Commit();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

void WriteFileParallel(
    const IClientBasePtr &client,
    const TString& fileName,
    const TRichYPath& path,
    const TParallelFileWriterOptions& options)
{
    auto writer = CreateParallelFileWriter(client, path, options);
    writer->WriteFile(fileName);
    writer->Finish();
}

////////////////////////////////////////////////////////////////////////////////
::TIntrusivePtr<IParallelFileWriter> CreateParallelFileWriter(
    const IClientBasePtr& client,
    const TRichYPath& path,
    const TParallelFileWriterOptions& options)
{
    auto threadPool = std::make_shared<TSimpleThreadPool>();
    threadPool->Start(options.ThreadCount_);
    return CreateParallelFileWriter(client, path, threadPool, options);
}

////////////////////////////////////////////////////////////////////////////////

::TIntrusivePtr<IParallelFileWriter> CreateParallelFileWriter(
    const IClientBasePtr& client,
    const TRichYPath& path,
    const std::shared_ptr<IThreadPool>& threadPool,
    const TParallelFileWriterOptions& options)
{
    return ::MakeIntrusive<NDetail::TParallelFileWriter>(client, path, threadPool, options);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
