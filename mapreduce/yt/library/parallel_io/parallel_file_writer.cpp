#include "parallel_file_writer.h"

#include <mapreduce/yt/common/config.h>

#include <util/system/fstat.h>
#include <util/system/info.h>
#include <util/system/mutex.h>
#include <util/system/thread.h>

namespace NYT {
namespace NDetail {

////////////////////////////////////////////////////////////////////////////////

class TParallelFileWriter {
public:
    /// @brief Write file in parallel.
    /// @param client       Client which used for write file on server.
    /// @param fileName     Source path to file in local storage.
    /// @param path         Dist path to file on server.
    TParallelFileWriter(
        const IClientBasePtr& client,
        const TString& fileName,
        const TRichYPath& path,
        const TParallelFileWriterOptions& options);

    ~TParallelFileWriter();

    void Finish();

private:
    static void* ThreadWrite(void* opaque);

    void ThreadWrite(int index, i64 start, i64 length);

private:
    struct TParam {
        TParallelFileWriter* Writer;
        int Index;
        i64 Start;
        i64 Length;
    };

private:
    TVector<TParam> Params_;
    TVector<THolder<TThread>> Threads_;
    ITransactionPtr Transaction_;
    TString FileName_;
    TRichYPath Path_;
    TVector<TYPath> TempPaths_;
    TParallelFileWriterOptions Options_;
    std::exception_ptr Exception_ = nullptr;
    std::atomic<bool> Finished_ = false;
    TMutex MutexForException_;
};

////////////////////////////////////////////////////////////////////////////////

TParallelFileWriter::TParallelFileWriter(
    const IClientBasePtr &client,
    const TString& fileName,
    const TRichYPath& path,
    const TParallelFileWriterOptions& options)
    : Transaction_(client->StartTransaction())
    , FileName_(fileName)
    , Path_(path)
    , Options_(options)
{
    int threadCount;
    auto length = GetFileLength(fileName);
    if (options.ThreadCount_) {
        threadCount = *options.ThreadCount_;
    } else {
        static constexpr i64 SingleThreadWriteSize = (1u << 30u);
        threadCount = Min<int>(
            length / SingleThreadWriteSize + 1,
            NSystemInfo::NumberOfCpus());
    }
    Params_.reserve(threadCount);
    Threads_.reserve(threadCount);
    TempPaths_.reserve(threadCount);
    for (int i = 0; i < threadCount; ++i) {
        i64 begin = length * i / threadCount;
        i64 end = length * (i + 1) / threadCount;
        Params_.push_back(TParam{
            .Writer = this,
            .Index = i,
            .Start = begin,
            .Length = end - begin,
        });
        TempPaths_.emplace_back(Path_.Path_ + "__ParallelFileWriter__" + ::ToString(i));
        Threads_.push_back(::MakeHolder<TThread>(
            TThread::TParams(ThreadWrite, &Params_.back())
                .SetName("ParallelFW " + ::ToString(i))));
    }
    for (auto& thread : Threads_) {
        thread->Start();
    }
}

TParallelFileWriter::~TParallelFileWriter()
{
    NDetail::FinishOrDie(this, "TParallelFileWriter");
}

void TParallelFileWriter::Finish()
{
    if (Finished_) {
        return;
    }
    Finished_ = true;
    for (auto& thread : Threads_) {
        thread->Join();
    }
    if (Exception_) {
        Transaction_->Abort();
        std::rethrow_exception(Exception_);
    }
    auto createOptions = TCreateOptions();
    auto concatenateOptions = TConcatenateOptions();
    if (Path_.Append_.GetOrElse(false)) {
        createOptions.IgnoreExisting(true);
        concatenateOptions.Append(true);
    } else {
        createOptions.Force(true);
        concatenateOptions.Append(false);
    }
    Transaction_->Create(Path_.Path_, NT_FILE, createOptions);
    Transaction_->Concatenate(TempPaths_, Path_.Path_, concatenateOptions);
    for (const auto& path : TempPaths_) {
        Transaction_->Remove(path);
    }
    Transaction_->Commit();
}

void* TParallelFileWriter::ThreadWrite(void* opaque)
{
    auto* param = static_cast<TParam*>(opaque);
    param->Writer->ThreadWrite(param->Index, param->Start, param->Length);
    return nullptr;
}

void TParallelFileWriter::ThreadWrite(int index, i64 start, i64 length)
{
    try {
        auto thisPath = Path_;
        thisPath.Path(TempPaths_[index]).Append(false);
        TFile file(FileName_, RdOnly);
        file.Seek(start, SeekDir::sSet);
        IFileWriterPtr writer;
        if (Options_.WriterOptions_) {
            writer = Transaction_->CreateFileWriter(
                thisPath,
                TFileWriterOptions().WriterOptions(*Options_.WriterOptions_));
        } else {
            writer = Transaction_->CreateFileWriter(thisPath);
        }
        TFileInput input(file);
        while (length > 0 && !Exception_) {
            void *buffer;
            i64 bufSize = input.Next(&buffer);
            if (length < bufSize) {
                bufSize = length;
            }
            writer->Write(buffer, bufSize);
            length -= bufSize;
        }
        writer->Finish();
    } catch (const yexception& e) {
        auto guard = Guard(MutexForException_);
        Exception_ = std::current_exception();
    }
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
    NDetail::TParallelFileWriter(client, fileName, path, options).Finish();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
