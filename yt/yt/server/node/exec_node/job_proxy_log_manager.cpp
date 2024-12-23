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

#include <yt/yt/core/logging/config.h>

#include <yt/yt/core/misc/fs.h>

#include <util/datetime/base.h>
#include <util/generic/ymath.h>
#include <util/system/fstat.h>

namespace NYT::NExecNode {

using namespace NConcurrency;
using namespace NLogging;

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
        , DynamicConfig_(New<TJobProxyLogManagerDynamicConfig>())
        , Directory_(Config_->Directory)
        , ShardingKeyLength_(Config_->ShardingKeyLength)
        , AsyncSemaphore_(New<TAsyncSemaphore>(Config_->DirectoryTraversalConcurrency))
    { }

    void Initialize() final
    {
        const auto& logWriterName = Config_->LogDump->LogWriterName;

        // GetLogFileNameToDump may throw.
        LogFileName_.Store(GetNewLogFileNameToDump(logWriterName));

        Bootstrap_->GetJobController()->SubscribeJobCompletelyRemoved(
            BIND_NO_PROPAGATE(&TJobProxyLogManager::OnJobCompletelyRemoved, MakeStrong(this)));
    }

    void Start() final
    {
        CreateShardingDirectories();

        Bootstrap_->GetStorageHeavyInvoker()->Invoke(BIND(
            [this, this_ = MakeStrong(this)] {
                TraverseJobDirectoriesAndScheduleRemovals();
            }));
    }

    void OnJobCompletelyRemoved(TJobId jobId)
    {
        auto dynamicConfig = DynamicConfig_.Acquire();

        auto logsStoragePeriod = dynamicConfig->LogsStoragePeriod.value_or(Config_->LogsStoragePeriod);

        YT_LOG_DEBUG(
            "Job proxy log removal scheduled (JobId: %v, Delay: %v)",
            jobId,
            logsStoragePeriod);

        TDelayedExecutor::Submit(
            BIND(&TJobProxyLogManager::RemoveJobDirectory, MakeStrong(this), JobIdToLogsPath(jobId)),
            logsStoragePeriod,
            Bootstrap_->GetStorageHeavyInvoker());
    }

    void OnDynamicConfigChanged(
        const TJobProxyLogManagerDynamicConfigPtr& oldConfig,
        const TJobProxyLogManagerDynamicConfigPtr& newConfig) final
    {
        if (!oldConfig && !newConfig) {
            return;
        }

        if (oldConfig && newConfig && *newConfig == *oldConfig) {
            return;
        }

        const auto& logWriterName = newConfig->LogDump && newConfig->LogDump->LogWriterName
            ? *newConfig->LogDump->LogWriterName
            : Config_->LogDump->LogWriterName;

        // GetLogFileNameToDump may throw.
        LogFileName_.Store(GetNewLogFileNameToDump(logWriterName));

        AsyncSemaphore_->SetTotal(newConfig->DirectoryTraversalConcurrency.value_or(Config_->DirectoryTraversalConcurrency));

        DynamicConfig_.Store(std::move(newConfig));
    }

    TString AdjustLogPath(TJobId jobId, const TString& logFilePath) final
    {
        return NFS::CombinePaths(JobIdToLogsPath(jobId), NFS::GetFileName(logFilePath));
    }

    TFuture<void> DumpJobProxyLog(
        TJobId jobId,
        const NYPath::TYPath& path,
        NObjectClient::TTransactionId transactionId) final
    {
        return BIND(&TJobProxyLogManager::DoDumpJobProxyLog, MakeStrong(this), jobId, path, transactionId)
            .AsyncVia(Bootstrap_->GetStorageHeavyInvoker())
            .Run();
    }

private:
    IBootstrap* const Bootstrap_;

    const TJobProxyLogManagerConfigPtr Config_;

    TAtomicIntrusivePtr<TJobProxyLogManagerDynamicConfig> DynamicConfig_;

    TString Directory_;
    int ShardingKeyLength_;

    NThreading::TAtomicObject<TString> LogFileName_;

    TAsyncSemaphorePtr AsyncSemaphore_;

    void CreateShardingDirectories() noexcept
    {
        YT_LOG_INFO("Start creating job proxy sharding key directories");

        try {
            auto formatString = Format("%%0%dx", ShardingKeyLength_);
            for (int i = 0; i < Power(16, ShardingKeyLength_); ++i) {
                auto dirName = Format(TRuntimeFormat{formatString}, i);

                auto dirPath = NFS::CombinePaths(Directory_, dirName);

                YT_LOG_INFO("Creating job proxy logs sharding key directory (DirPath: %v)", dirPath);
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

    void TraverseShardingDirectoryAndScheduleRemovals(TInstant currentTime, const TString& shardingDirPath) noexcept
    {
        auto Logger = ExecNodeLogger()
            .WithTag("ShardingDirPath: %v", shardingDirPath);

        auto guard = TAsyncSemaphoreGuard();
        if (DynamicConfig_.Acquire()->DirectoryTraversalConcurrency.value_or(Config_->DirectoryTraversalConcurrency) != 0) {
            YT_LOG_INFO("Waiting semaphore to traverse job directory");
            guard = WaitForUnique(AsyncSemaphore_->AsyncAcquire()).ValueOrThrow();
        }

        auto logsStoragePeriod = DynamicConfig_.Acquire()->LogsStoragePeriod.value_or(Config_->LogsStoragePeriod);

        YT_LOG_INFO(
            "Start traversing job directory (LogsStoragePeriod: %v)",
            logsStoragePeriod);

        for (const auto& jobDirName : NFS::EnumerateDirectories(shardingDirPath)) {
            auto jobDirPath = NFS::CombinePaths(shardingDirPath, jobDirName);
            auto jobLogsDirModificationTime = TInstant::Seconds(TFileStat(jobDirPath).MTime);
            auto removeJobDirectory = BIND(&TJobProxyLogManager::RemoveJobDirectory, MakeStrong(this), Passed(std::move(jobDirPath)));
            if (jobLogsDirModificationTime + logsStoragePeriod <= currentTime) {
                Bootstrap_->GetStorageHeavyInvoker()->Invoke(std::move(removeJobDirectory));
            } else {
                YT_LOG_DEBUG(
                    "Job proxy log removal scheduled (DirectoryName: %v, Time: %v)",
                    jobDirName,
                    jobLogsDirModificationTime + logsStoragePeriod);

                TDelayedExecutor::Submit(
                    std::move(removeJobDirectory),
                    jobLogsDirModificationTime + logsStoragePeriod,
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

        auto dynamicConfig = DynamicConfig_.Acquire();

        auto dumpJobProxyLogBufferSize = dynamicConfig->LogDump && dynamicConfig->LogDump->BufferSize
            ? *dynamicConfig->LogDump->BufferSize
            : Config_->LogDump->BufferSize;

        auto logFileName = LogFileName_.Load();

        YT_LOG_DEBUG(
            "Dumping job proxy log file (JobId: %v, LogFileName: %v)",
            jobId,
            logFileName);

        auto logFile = [&] {
            try {
                return TFile(AdjustLogPath(jobId, logFileName), OpenExisting | RdOnly);
            } catch (const std::exception& ex) {
                THROW_ERROR_EXCEPTION("Failed to open log file")
                    << TErrorAttribute("log_file_name", logFileName)
                    << ex;
            }
        }();

        auto buffer = TSharedMutableRef::Allocate(dumpJobProxyLogBufferSize);

        NApi::TFileWriterOptions options;
        options.TransactionId = transactionId;
        auto writer = Bootstrap_->GetClient()->CreateFileWriter(path, options);

        WaitFor(writer->Open())
            .ThrowOnError();

        // TODO(pogorelov): Support compressed logs.
        while (true) {
            auto bytesRead = logFile.Read((void*)buffer.Data(), dumpJobProxyLogBufferSize);

            if (bytesRead == 0) {
                break;
            }

            WaitFor(writer->Write(buffer.Slice(0, bytesRead)))
                .ThrowOnError();
        }

        WaitFor(writer->Close())
            .ThrowOnError();
    }

    TString GetNewLogFileNameToDump(TStringBuf newLogWriterName)
    {
        const auto& jobProxyConfigTemplate = Bootstrap_->GetJobProxyConfigTemplate();
        auto jobProxyLoggingConfig = jobProxyConfigTemplate->GetSingletonConfig<NLogging::TLogManagerConfig>();

        auto jobProxyLogWriterConfigIt = jobProxyLoggingConfig->Writers.find(newLogWriterName);
        if (jobProxyLogWriterConfigIt == jobProxyLoggingConfig->Writers.end()) {
            THROW_ERROR_EXCEPTION("Log writer %Qv configured for dump is missing", newLogWriterName);
        }

        const auto& logWriterConfigNode = jobProxyLogWriterConfigIt->second;

        auto typedWriterConfig = ConvertTo<TLogWriterConfigPtr>(logWriterConfigNode);

        if (typedWriterConfig->Type != TFileLogWriterConfig::WriterType) {
            THROW_ERROR_EXCEPTION("Log writer %Qv configured for dump must have “file” type", newLogWriterName)
                << TErrorAttribute("log_writer_type", typedWriterConfig->Type);
        }

        auto fileLogWriterConfig = ConvertTo<TFileLogWriterConfigPtr>(logWriterConfigNode);
        return fileLogWriterConfig->FileName;
    }

    TString GetShardingKey(TJobId jobId)
    {
        auto entropy = NScheduler::EntropyFromAllocationId(
            NScheduler::AllocationIdFromJobId(jobId));
        auto entropyHex = Format("%016lx", entropy);
        return entropyHex.substr(0, ShardingKeyLength_);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TMockJobProxyLogManager
    : public IJobProxyLogManager
{
public:
    void Initialize() override
    { }

    void Start() override
    { }

    TString AdjustLogPath(TJobId /*jobId*/, const TString& /*logFilePath*/) override
    {
        THROW_ERROR_EXCEPTION("Dumping job proxy logs is not supported in simple logging mode");
    }

    void OnDynamicConfigChanged(
        const TJobProxyLogManagerDynamicConfigPtr& /*oldConfig*/,
        const TJobProxyLogManagerDynamicConfigPtr& /*newConfig*/) override
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
