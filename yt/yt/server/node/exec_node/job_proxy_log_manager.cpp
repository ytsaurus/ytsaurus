#include "job_proxy_log_manager.h"

#include "bootstrap.h"
#include "private.h"

#include <yt/yt/server/lib/exec_node/config.h>
#include <yt/yt/server/lib/scheduler/helpers.h>
#include <yt/yt/server/node/cluster_node/config.h>
#include <yt/yt/server/node/exec_node/job_controller.h>

#include <yt/yt/ytlib/api/native/client.h>

#include <yt/yt/ytlib/cypress_client/cypress_ypath_proxy.h>

#include <yt/yt/ytlib/file_client/file_chunk_output.h>

#include <yt/yt/ytlib/job_proxy/job_spec_helper.h>

#include <yt/yt/ytlib/object_client/object_service_proxy.h>

#include <yt/yt/client/api/file_writer.h>

#include <yt/yt/core/misc/fs.h>

#include <util/datetime/base.h>
#include <util/generic/ymath.h>
#include <util/system/fstat.h>

namespace NYT::NExecNode {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

static constexpr auto& Logger = ExecNodeLogger;

////////////////////////////////////////////////////////////////////////////////

class TJobProxyLogManager
    : public IJobProxyLogManager
{
public:
    explicit TJobProxyLogManager(IBootstrap* bootstrap)
        : Bootstrap_(bootstrap)
        , Config_(Bootstrap_->GetConfig()->ExecNode->JobProxyLogManager)
        , Directory_(Config_->Directory)
        , ShardingKeyLength_(Config_->ShardingKeyLength)
        , LogsStoragePeriod_(Config_->LogsStoragePeriod)
        , DirectoryTraversalConcurrency_(Config_->DirectoryTraversalConcurrency)
        , AsyncSemaphore_(New<TAsyncSemaphore>(Config_->DirectoryTraversalConcurrency.value_or(0)))
        , DumpJobProxyLogBufferSize_(Config_->DumpJobProxyLogBufferSize)
    { }

    void Start() override
    {
        Bootstrap_->GetStorageHeavyInvoker()->Invoke(BIND(
            [this, this_ = MakeStrong(this)] {
                CreateShardingDirectories();
                TraverseJobDirectoriesAndScheduleRemovals();
            }));
    }

    void OnJobUnregistered(TJobId jobId) override
    {
        TDelayedExecutor::Submit(
            BIND(&TJobProxyLogManager::RemoveJobDirectory, MakeStrong(this), JobIdToLogsPath(jobId)),
            LogsStoragePeriod_,
            Bootstrap_->GetStorageHeavyInvoker());
    }

    TString GetShardingKey(TJobId jobId) override
    {
        auto entropy = NScheduler::EntropyFromAllocationId(
            NScheduler::AllocationIdFromJobId(jobId));
        auto entropyHex = Format("%016lx", entropy);
        return entropyHex.substr(0, ShardingKeyLength_);
    }

    void OnDynamicConfigChanged(
        TJobProxyLogManagerDynamicConfigPtr /*oldConfig*/,
        TJobProxyLogManagerDynamicConfigPtr newConfig) override
    {
        LogsStoragePeriod_ = newConfig->LogsStoragePeriod;
        DirectoryTraversalConcurrency_ = newConfig->DirectoryTraversalConcurrency;
        AsyncSemaphore_->SetTotal(DirectoryTraversalConcurrency_.value_or(0));
    }

    TFuture<void> DumpJobProxyLog(
        TJobId jobId,
        const NYPath::TYPath& path,
        NObjectClient::TTransactionId transactionId) override
    {
        return BIND(&TJobProxyLogManager::DoDumpJobProxyLog, MakeStrong(this), jobId, path, transactionId)
            .AsyncVia(Bootstrap_->GetStorageHeavyInvoker())
            .Run();
    }

private:
    IBootstrap* const Bootstrap_;

    const TJobProxyLogManagerConfigPtr Config_;

    TString Directory_;
    int ShardingKeyLength_;
    TDuration LogsStoragePeriod_;

    std::optional<int> DirectoryTraversalConcurrency_;
    TAsyncSemaphorePtr AsyncSemaphore_;

    i64 DumpJobProxyLogBufferSize_;

    void CreateShardingDirectories() noexcept
    {
        YT_LOG_INFO("Start creating job proxy sharding key directories");

        try {
            TRuntimeFormat formatString{Format("%%0%dx", ShardingKeyLength_)};
            for (int i = 0; i < Power(16, ShardingKeyLength_); i++) {
                auto dirName = Format(formatString, i);

                auto dirPath = NFS::CombinePaths(Directory_, dirName);
                NFS::MakeDirRecursive(dirPath);
            }

            YT_LOG_INFO("Finish creating job proxy sharding key directories");
        } catch (const std::exception& ex) {
            YT_LOG_ERROR(ex, "Failed to create sharding key directories");
        }
    }

    void TraverseJobDirectoriesAndScheduleRemovals() noexcept
    {
        auto currentTime = Now();

        for (const auto& shardingDirName : NFS::EnumerateDirectories(Directory_)) {
            auto shardingDirPath = NFS::CombinePaths(Directory_, shardingDirName);
            Bootstrap_->GetStorageHeavyInvoker()->Invoke(BIND(
                &TJobProxyLogManager::TraverseShardingDirectoryAndScheduleRemovals,
                MakeStrong(this),
                currentTime,
                std::move(shardingDirPath)));
        }
    }

    void TraverseShardingDirectoryAndScheduleRemovals(TInstant currentTime, TString shardingDirPath) noexcept
    {
        auto Logger = ExecNodeLogger()
            .WithTag("ShardingDirPath: %v", shardingDirPath);

        auto guard = TAsyncSemaphoreGuard();
        if (DirectoryTraversalConcurrency_.has_value()) {
            YT_LOG_INFO("Waiting semaphore to traverse job directory");
            guard = WaitForUnique(AsyncSemaphore_->AsyncAcquire()).ValueOrThrow();
        }

        YT_LOG_INFO("Start traversing job directory");

        for (const auto& jobDirName : NFS::EnumerateDirectories(shardingDirPath)) {
            auto jobDirPath = NFS::CombinePaths(shardingDirPath, jobDirName);
            auto jobLogsDirModificationTime = TInstant::Seconds(TFileStat(jobDirPath).MTime);
            auto removeJobDirectory = BIND(&TJobProxyLogManager::RemoveJobDirectory, MakeStrong(this), Passed(std::move(jobDirPath)));
            if (jobLogsDirModificationTime + LogsStoragePeriod_ <= currentTime) {
                Bootstrap_->GetStorageHeavyInvoker()->Invoke(removeJobDirectory);
            } else {
                TDelayedExecutor::Submit(
                    removeJobDirectory,
                    jobLogsDirModificationTime + LogsStoragePeriod_,
                    Bootstrap_->GetStorageHeavyInvoker());
            }
        }

        YT_LOG_INFO("Finish traversing job directory");
    }

    void RemoveJobDirectory(const TString& path) noexcept
    {
        try {
            NFS::RemoveRecursive(path);
            YT_LOG_INFO("Job directory removed (Path: %v)", path);
        } catch (const std::exception& ex) {
            YT_LOG_ERROR(ex, "Failed to remove job directory (Path: %v)", path);
        }
    }

    TString JobIdToLogsPath(TJobId jobId)
    {
        auto shardingKey = GetShardingKey(jobId);
        return NFS::CombinePaths({Directory_, shardingKey, ToString(jobId)});
    }


    void DoDumpJobProxyLog(
        TJobId jobId,
        const NYPath::TYPath& path,
        NObjectClient::TTransactionId transactionId)
    {
        auto logsPath = JobIdToLogsPath(jobId);

        if (!NFS::Exists(logsPath)) {
            THROW_ERROR_EXCEPTION("Job directory is not found")
                << TErrorAttribute("job_id", jobId)
                << TErrorAttribute("path", logsPath);
        }

        auto logFile = TFile(NFS::CombinePaths(logsPath, "job_proxy.log"), OpenExisting | RdOnly);
        auto buffer = TSharedMutableRef::Allocate(DumpJobProxyLogBufferSize_);

        NApi::TFileWriterOptions options;
        options.TransactionId = transactionId;
        auto writer = Bootstrap_->GetClient()->CreateFileWriter(path, options);

        WaitFor(writer->Open())
            .ThrowOnError();

        // TODO(pogorelov): Support compressed logs.
        while (true) {
            auto bytesRead = logFile.Read((void*)buffer.Data(), DumpJobProxyLogBufferSize_);

            if (bytesRead == 0) {
                break;
            }

            WaitFor(writer->Write(buffer.Slice(0, bytesRead)))
                .ThrowOnError();
        }

        WaitFor(writer->Close())
            .ThrowOnError();
    }
};

////////////////////////////////////////////////////////////////////////////////

class TMockJobProxyLogManager
    : public IJobProxyLogManager
{
public:
    void Start() override
    { }

    void OnJobUnregistered(TJobId /*jobId*/) override
    { }

    TString GetShardingKey(TJobId /*jobId*/) override
    {
        THROW_ERROR_EXCEPTION("Dumping job proxy logs is not supported in simple logging mode");
    }

    void OnDynamicConfigChanged(
        TJobProxyLogManagerDynamicConfigPtr /*oldConfig*/,
        TJobProxyLogManagerDynamicConfigPtr /*newConfig*/) override
    { }

    TFuture<void> DumpJobProxyLog(
        TJobId /*jobId*/,
        const NYPath::TYPath& /*path*/,
        NObjectClient::TTransactionId /*transactionId*/) override
    {
        THROW_ERROR_EXCEPTION("Dumping job proxy logs is not supported in simple logging mode");
    }
};

////////////////////////////////////////////////////////////////////////////////

IJobProxyLogManagerPtr CreateJobProxyLogManager(IBootstrap* bootstrap)
{
    switch (bootstrap->GetConfig()->ExecNode->JobProxy->JobProxyLogging->Mode) {
        case EJobProxyLoggingMode::Simple:
            return New<TMockJobProxyLogManager>();
        case EJobProxyLoggingMode::PerJobDirectory:
            return New<TJobProxyLogManager>(bootstrap);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
