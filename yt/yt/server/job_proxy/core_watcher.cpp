#include "core_watcher.h"

#include "job.h"

#include <yt/server/lib/core_dump/helpers.h>

#include <yt/server/lib/job_proxy/config.h>

#include <yt/server/lib/misc/job_table_schema.h>

#include <yt/ytlib/table_client/blob_table_writer.h>

#include <yt/library/process/io_dispatcher.h>
#include <yt/library/process/pipe.h>

#include <yt/core/concurrency/action_queue.h>

#include <yt/core/logging/log.h>

#include <yt/core/net/connection.h>

#include <yt/core/misc/fs.h>
#include <yt/core/misc/proc.h>

#include <yt/core/rpc/dispatcher.h>

#include <util/folder/iterator.h>
#include <util/folder/path.h>

#include <sys/ioctl.h>

namespace NYT::NJobProxy {

using namespace NChunkClient;
using namespace NConcurrency;
using namespace NCoreDump;
using namespace NCoreDump::NProto;
using namespace NCypressClient;
using namespace NFS;
using namespace NLogging;
using namespace NNet;
using namespace NPipes;
using namespace NTableClient;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

static const TLogger CoreWatcherLogger("CoreWatcher");

////////////////////////////////////////////////////////////////////////////////

TCoreResult::TCoreResult()
{
    BoundaryKeys.set_empty(true);
}

////////////////////////////////////////////////////////////////////////////////

TGpuCoreReader::TGpuCoreReader(const TString& corePipePath)
    : Path_(corePipePath)
{
    Fd_ = HandleEintr(::open, corePipePath.c_str(), O_RDONLY | O_CLOEXEC | O_NONBLOCK);
    if (Fd_ < 0) {
        THROW_ERROR_EXCEPTION("Failed to open GPU core dump pipe")
            << TErrorAttribute("path", Path_)
            << TError::FromSystem();
    }   
}

i64 TGpuCoreReader::GetBytesAvailable() const
{
    int pipeSize;
    if (::ioctl(Fd_, FIONREAD, &pipeSize) < 0) {
        THROW_ERROR_EXCEPTION("Fail to perform ioctl on GPU core dump pipe")
            << TErrorAttribute("path", Path_)
            << TError::FromSystem();
    }

    return pipeSize;
}

IConnectionReaderPtr TGpuCoreReader::CreateAsyncReader()
{
    return CreateInputConnectionFromFD(Fd_, Path_, TIODispatcher::Get()->GetPoller(), MakeStrong(this));
}

////////////////////////////////////////////////////////////////////////////////

TCoreWatcher::TCoreWatcher(
    TCoreWatcherConfigPtr config,
    TString coreDirectoryPath,
    IJobHostPtr jobHost,
    IInvokerPtr controlInvoker,
    TBlobTableWriterConfigPtr blobTableWriterConfig,
    TTableWriterOptionsPtr tableWriterOptions,
    TTransactionId transaction,
    TChunkListId chunkList)
    : Config_(std::move(config))
    , ControlInvoker_(std::move(controlInvoker))
    , IOInvoker_(CreateSerializedInvoker(NRpc::TDispatcher::Get()->GetHeavyInvoker()))
    , PeriodicExecutor_(New<TPeriodicExecutor>(
        ControlInvoker_,
        BIND(&TCoreWatcher::DoWatchCores, MakeWeak(this)),
        Config_->Period))
    , CoreDirectoryPath_(std::move(coreDirectoryPath))
    , JobHost_(std::move(jobHost))
    , BlobTableWriterConfig_(std::move(blobTableWriterConfig))
    , TableWriterOptions_(std::move(tableWriterOptions))
    , Transaction_(transaction)
    , ChunkList_(chunkList)
    , Logger(TLogger(CoreWatcherLogger).AddTag("JobId: %v", JobHost_->GetJobId()))
{
    PeriodicExecutor_->Start();
}

TCoreResult TCoreWatcher::Finalize(std::optional<TDuration> finalizationTimeout)
{
    VERIFY_THREAD_AFFINITY_ANY();

    if (finalizationTimeout && !CoreAppearedPromise_.IsSet()) {
        YT_LOG_DEBUG("Waiting for at least one core to appear (FinalizationTimeout: %v)",
            *finalizationTimeout);
        auto waitResult = WaitFor(GetCoreAppearedEvent()
            .WithTimeout(*finalizationTimeout));
        if (!waitResult.IsOK()) {
            YT_VERIFY(waitResult.FindMatching(NYT::EErrorCode::Timeout));
            YT_LOG_INFO("Core did not appear during finalization timeout");
        }
    }

    // Stop watching for new cores.
    WaitFor(PeriodicExecutor_->GetExecutedEvent())
        .ThrowOnError();
    WaitFor(PeriodicExecutor_->Stop())
        .ThrowOnError();

    YT_LOG_DEBUG("Core watcher has stopped, no new cores will be processed");

    auto expectedCoreCount = NextCoreIndex_;
    if (finalizationTimeout && CoreFutures_.empty()) {
        expectedCoreCount = std::max(expectedCoreCount, 1);
    }

    auto waitResult = WaitFor(Combine(CoreFutures_)
        .WithTimeout(Config_->CoresProcessingTimeout));
    if (!waitResult.IsOK()) {
        YT_VERIFY(waitResult.FindMatching(NYT::EErrorCode::Timeout));
        YT_LOG_INFO("Cores processing did not finish within timeout");
    }

    auto& coreInfos = CoreResult_.CoreInfos;
    if (expectedCoreCount > coreInfos.size()) {
        coreInfos.resize(expectedCoreCount);
    }

    YT_LOG_DEBUG("Finalizing core watcher (ExpectedCoreCount: %v)",
        expectedCoreCount);

    for (int coreIndex = 0; coreIndex < expectedCoreCount; ++coreIndex) {
        auto& coreInfo = coreInfos[coreIndex];
        if (!coreInfo.has_process_id()) {
            YT_LOG_INFO("Core processing did not complete, adding dummy core instead (CoreIndex: %v)",
                coreIndex);
            coreInfo.set_process_id(-1);
            coreInfo.set_executable_name("n/a");
            coreInfo.set_core_index(coreIndex);
            if (CoreFutures_.empty()) {
                YT_VERIFY(coreIndex == 0);
                ToProto(coreInfo.mutable_error(), TError("Timeout while waiting for a core dump"));
            } else {
                ToProto(coreInfo.mutable_error(), TError("Cores processing timed out"));
            }
        }
    }

    return CoreResult_;
}

void TCoreWatcher::DoWatchCores()
{
    VERIFY_INVOKER_AFFINITY(ControlInvoker_);

    YT_LOG_DEBUG("Looking for new cores (CoreDirectoryPath: %v)",
        CoreDirectoryPath_);

    for (const auto& file : TDirIterator(CoreDirectoryPath_)) {
        auto fileName = TFsPath{file.fts_path}.GetName();
        if (GetFileExtension(fileName) == "pipe") {
            auto name = GetFileNameWithoutExtension(fileName);
            if (!SeenCoreNames_.contains(name)) {
                YT_LOG_INFO("New core pipe found (CorePipeFileName: %v)",
                    fileName);

                int coreIndex = NextCoreIndex_;
                ++NextCoreIndex_;

                SeenCoreNames_.insert(name);

                auto coreInfoFuture = BIND(&TCoreWatcher::DoProcessLinuxCore, MakeStrong(this), name, coreIndex)
                    .AsyncVia(IOInvoker_)
                    .Run();
                CoreFutures_.push_back(coreInfoFuture
                    .Apply(BIND(&TCoreWatcher::DoAddCoreInfo, MakeStrong(this))
                        .Via(ControlInvoker_)));
            }
        } else if (fileName == CudaGpuCoreDumpPipeName) {
            if (!SeenCoreNames_.contains(fileName)) {
                YT_LOG_DEBUG("GPU core dump pipe found (FileName: %v)",
                    fileName);

                if (!GpuCoreReader_) {
                    const auto gpuCoreDumpPath = CoreDirectoryPath_ + "/" + fileName;
                    GpuCoreReader_ = New<TGpuCoreReader>(gpuCoreDumpPath);
                    YT_LOG_DEBUG("GPU core reader created (CoreDumpPath: %v)",
                        gpuCoreDumpPath);
                }
                
                auto bytesAvailable = GpuCoreReader_->GetBytesAvailable();
                if (bytesAvailable > 0) {
                    YT_LOG_INFO("GPU core dump streaming started (GpuCorePipeFileName: %v, BytesAvailable: %v)",
                        fileName,
                        bytesAvailable);

                    int coreIndex = NextCoreIndex_++;

                    SeenCoreNames_.insert(fileName);

                    auto coreInfoFuture = BIND(
                        &TCoreWatcher::DoProcessGpuCore, 
                        MakeStrong(this), 
                        GpuCoreReader_->CreateAsyncReader(), 
                        coreIndex)
                        .AsyncVia(IOInvoker_)
                        .Run();
                    CoreFutures_.push_back(coreInfoFuture
                        .Apply(BIND(&TCoreWatcher::DoAddCoreInfo, MakeStrong(this))
                            .Via(ControlInvoker_)));
                }
            }
        }
    }
}

TCoreInfo TCoreWatcher::DoProcessLinuxCore(const TString& coreName, int coreIndex)
{
    VERIFY_INVOKER_AFFINITY(IOInvoker_);

    auto Logger = TLogger(this->Logger)
        .AddTag("CoreName: %v", coreName)
        .AddTag("CoreIndex: %v", coreIndex);

    TCoreInfo coreInfo;
    coreInfo.set_core_index(coreIndex);

    if (coreIndex == 0) {
        CoreAppearedPromise_.Set();
    }

    auto guard = WaitFor(TAsyncLockWriterGuard::Acquire(&WriterLock_))
        .ValueOrThrow();

    YT_LOG_INFO("Started processing core dump");

    try {
        {
            auto coreInfoFile = CoreDirectoryPath_ + "/" + coreName + ".info";
            YT_LOG_DEBUG("Reading core info file (CoreInfoFilePath: %v)",
                coreInfoFile);

            auto coreInfoReader = TUnbufferedFileInput(coreInfoFile);
            auto executableName = coreInfoReader.ReadLine();
            auto processId = FromString<int>(coreInfoReader.ReadLine());
            auto threadId = FromString<int>(coreInfoReader.ReadLine());
            auto signal = FromString<int>(coreInfoReader.ReadLine());
            auto container = coreInfoReader.ReadLine();
            auto datetime = coreInfoReader.ReadLine();

            coreInfo.set_executable_name(executableName);
            coreInfo.set_process_id(processId);
            coreInfo.set_thread_id(threadId);
            coreInfo.set_signal(signal);
            coreInfo.set_container(container);
            coreInfo.set_datetime(datetime);

            YT_LOG_DEBUG("Info file read completed (ExecutableName: %v, ProcessId: %v)",
                executableName,
                processId);
        }

        auto corePipePath = CoreDirectoryPath_ + "/" + coreName + ".pipe";
        auto corePipe = TNamedPipe::FromPath(corePipePath);
        auto coreSize = DoReadCore(corePipe->CreateAsyncReader(), coreName, coreIndex);
        coreInfo.set_size(coreSize);

        YT_LOG_DEBUG("Finished processing core dump");
    } catch (const std::exception& ex) {
        YT_LOG_ERROR(ex, "Error while processing core");
        auto error = TError("Error while processing core")
            << ex;
        ToProto(coreInfo.mutable_error(), error);
        if (!coreInfo.has_executable_name()) {
            coreInfo.set_executable_name("(n/a)");
        }
        if (!coreInfo.has_process_id()) {
            coreInfo.set_process_id(-1);
        }
    }

    return coreInfo;
}

TCoreInfo TCoreWatcher::DoProcessGpuCore(IAsyncInputStreamPtr coreStream, int coreIndex)
{
    VERIFY_INVOKER_AFFINITY(IOInvoker_);

    const TString coreName = "cuda_gpu_core_dump";

    TCoreInfo coreInfo;
    coreInfo.set_core_index(coreIndex);
    coreInfo.set_executable_name(coreName);
    coreInfo.set_process_id(0);

    if (coreIndex == 0) {
        CoreAppearedPromise_.Set();
    }

    auto guard = WaitFor(TAsyncLockWriterGuard::Acquire(&WriterLock_))
        .ValueOrThrow();

    YT_LOG_INFO("Started processing GPU core dump");

    try {
        auto coreSize = DoReadCore(coreStream, coreName, coreIndex);
        coreInfo.set_size(coreSize);

        YT_LOG_DEBUG("Finished processing GPU core dump");
    } catch (const std::exception& ex) {
        YT_LOG_ERROR(ex, "Error processing GPU core dump");
        auto error = TError("Error processing GPU core dump")
            << ex;
        ToProto(coreInfo.mutable_error(), error);
    }

    return coreInfo;
}

i64 TCoreWatcher::DoReadCore(const IAsyncInputStreamPtr& coreStream, const TString& coreName, int coreIndex)
{
    VERIFY_INVOKER_AFFINITY(IOInvoker_);

    auto Logger = TLogger(this->Logger)
        .AddTag("CoreName: %v", coreName)
        .AddTag("CoreIndex: %v", coreIndex);

    TBlobTableWriter blobTableWriter(
        GetCoreBlobTableSchema(),
        {ConvertToYsonString(JobHost_->GetJobId()), ConvertToYsonString(coreIndex)},
        JobHost_->GetClient(),
        BlobTableWriterConfig_,
        TableWriterOptions_,
        Transaction_,
        ChunkList_,
        JobHost_->GetTrafficMeter(),
        JobHost_->GetOutRpsThrottler());

    YT_LOG_DEBUG("Started writing core dump (CoreName: %v, TransactionId: %v, ChunkListId: %v)",
        coreName,
        Transaction_,
        ChunkList_);

    auto coreWriter = New<TStreamSparseCoreDumpWriter>(
        CreateAsyncAdapter(&blobTableWriter),
        Config_->IOTimeout);
    auto coreSize = SparsifyCoreDump(coreStream, coreWriter, Config_->IOTimeout);

    blobTableWriter.Finish();

    YT_LOG_DEBUG("Finished writing core dump (CoreSize: %v)",
        coreSize);

    {
        auto outputResult = blobTableWriter.GetOutputResult();
        YT_VERIFY(!outputResult.empty() || coreSize == 0);

        auto& boundaryKeys = CoreResult_.BoundaryKeys;
        if (boundaryKeys.empty()) {
            boundaryKeys.MergeFrom(outputResult);
        } else if (!outputResult.empty()) {
            boundaryKeys.mutable_max()->swap(*outputResult.mutable_max());
        }
    }

    return coreSize;
}

void TCoreWatcher::DoAddCoreInfo(const TErrorOr<TCoreInfo>& coreInfo)
{
    VERIFY_INVOKER_AFFINITY(ControlInvoker_);

    TGuard<TSpinLock> guard(CoreInfosLock_);
    if (coreInfo.IsOK()) {
        auto coreIndex = coreInfo.Value().core_index();
        if (coreIndex >= CoreResult_.CoreInfos.size()) {
            CoreResult_.CoreInfos.resize(coreIndex + 1);
        }
        CoreResult_.CoreInfos[coreIndex] = coreInfo.Value();
    } else {
        YT_LOG_INFO(coreInfo, "Failed to get core info");
    }
}

TFuture<void> TCoreWatcher::GetCoreAppearedEvent() const
{
    return CoreAppearedPromise_.ToFuture();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobProxy
