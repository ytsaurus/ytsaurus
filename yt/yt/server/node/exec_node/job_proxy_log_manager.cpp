#include "job_proxy_log_manager.h"

#include "bootstrap.h"
#include "private.h"

#include <yt/yt/server/node/cluster_node/config.h>

#include <yt/yt/server/node/exec_node/job_controller.h>

#include <yt/yt/server/lib/exec_node/config.h>

#include <yt/yt/server/lib/scheduler/helpers.h>

#include <yt/yt/ytlib/api/native/client.h>

#include <yt/yt/ytlib/cypress_client/cypress_ypath_proxy.h>

#include <yt/yt/ytlib/file_client/file_chunk_output.h>

#include <yt/yt/ytlib/job_proxy/job_spec_helper.h>

#include <yt/yt/ytlib/object_client/object_service_proxy.h>

#include <yt/yt/client/api/file_writer.h>

#include <yt/yt/core/logging/config.h>

#include <yt/yt/server/lib/misc/disk_health_checker.h>

#include <yt/yt/core/concurrency/periodic_executor.h>

#include <yt/yt/core/misc/fs.h>

#include <util/datetime/base.h>

#include <util/generic/ymath.h>

#include <util/system/fstat.h>

namespace NYT::NExecNode {

using namespace NConcurrency;
using namespace NLogging;
using namespace NServer;

////////////////////////////////////////////////////////////////////////////////

constinit const auto Logger = ExecNodeLogger;

////////////////////////////////////////////////////////////////////////////////

class TJobProxyLogManager
    : public IJobProxyLogManager
{
public:
    explicit TJobProxyLogManager(IBootstrap* bootstrap)
        : Bootstrap_(bootstrap)
        , Config_(Bootstrap_->GetConfig()->ExecNode->JobProxyLogManager)
        , DynamicConfig_(New<TJobProxyLogManagerDynamicConfig>())
        , ShardingKeyLength_(Config_->ShardingKeyLength)
        , AsyncSemaphore_(New<TAsyncSemaphore>(Config_->DirectoryTraversalConcurrency))
    {
        LocationHealthCheckers_.reserve(Config_->Locations.size());
        AliveLocationIndices_.resize(Config_->Locations.size());
        for (int idx = 0; idx < std::ssize(Config_->Locations); ++idx) {
            AliveLocationIndices_[idx] = true;
            LocationHealthCheckers_.push_back(New<TDiskHealthChecker>(
                New<TDiskHealthCheckerConfig>(),
                TString(Config_->Locations[idx]->Path),
                Bootstrap_->GetStorageHeavyInvoker(),
                Logger()));
        }
        LocationAlerts_.resize(Config_->Locations.size());
    }

    void Initialize() final
    {
        const auto& logWriterName = Config_->LogDump->LogWriterName;

        LogFileName_.Store(GetNewLogFileNameToDump(logWriterName));

        Bootstrap_->GetJobController()->SubscribeJobCompletelyRemoved(
            BIND_NO_PROPAGATE(&TJobProxyLogManager::OnJobCompletelyRemoved, MakeStrong(this)));

        Bootstrap_->SubscribePopulateAlerts(
            BIND_NO_PROPAGATE(&TJobProxyLogManager::PopulateAlerts, MakeWeak(this)));
    }

    void Start() final
    {
        CreateShardingDirectories();

        CheckAllLocations();

        LocationCheckExecutor_ = New<TPeriodicExecutor>(
            Bootstrap_->GetStorageHeavyInvoker(),
            BIND_NO_PROPAGATE(&TJobProxyLogManager::CheckAllLocations, MakeWeak(this)),
            DynamicConfig_.Acquire()->LocationCheckPeriod.value_or(Config_->LocationCheckPeriod));
        LocationCheckExecutor_->Start();

        Bootstrap_->GetStorageHeavyInvoker()->Invoke(BIND(
            [this, this_ = MakeStrong(this)] {
                TraverseJobDirectoriesAndScheduleRemovals();
            }));
    }

    void OnJobCompletelyRemoved(TJobId jobId)
    {
        auto dynamicConfig = DynamicConfig_.Acquire();
        auto logsStoragePeriod = dynamicConfig->LogsStoragePeriod.value_or(Config_->LogsStoragePeriod);

        Bootstrap_->GetStorageHeavyInvoker()->Invoke(
            BIND_NO_PROPAGATE(&TJobProxyLogManager::ScheduleJobDirectoryRemoval, MakeStrong(this), jobId, logsStoragePeriod));
    }

    void ScheduleJobDirectoryRemoval(TJobId jobId, TDuration logsStoragePeriod)
    {
        TString jobLogsPath;
        try {
            jobLogsPath = FindExistingJobLogsPath(jobId);
        } catch (const std::exception& ex) {
            YT_LOG_DEBUG(ex,
                "Job log directory is not found, skipping removal (JobId: %v)",
                jobId);
            return;
        }

        YT_LOG_DEBUG(
            "Job proxy log removal scheduled (JobId: %v, Delay: %v)",
            jobId,
            logsStoragePeriod);

        TDelayedExecutor::Submit(
            BIND_NO_PROPAGATE(
                &TJobProxyLogManager::RemoveJobDirectory,
                MakeStrong(this),
                std::move(jobLogsPath),
                ToString(jobId)),
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

        const auto locationCheckPeriod = newConfig->LocationCheckPeriod.value_or(Config_->LocationCheckPeriod);
        if (LocationCheckExecutor_) {
            LocationCheckExecutor_->SetPeriod(locationCheckPeriod);
            YT_LOG_INFO(
                "Updated location check period (Prev: %v, Current: %v)",
                Config_->LocationCheckPeriod,
                locationCheckPeriod);
        }

        DynamicConfig_.Store(std::move(newConfig));
    }

    TString AdjustLogPath(TJobId jobId, const TString& logFilePath) final
    {
        return NFS::CombinePaths(GetJobLogDirectoryPath(jobId), NFS::GetFileName(logFilePath));
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

    bool IsEnabled() const final
    {
        auto guard = Guard(AliveLocationsLock_);
        return NoAliveLocationsAlert_.IsOK();
    }

private:
    IBootstrap* const Bootstrap_;

    const TJobProxyLogManagerConfigPtr Config_;

    TAtomicIntrusivePtr<TJobProxyLogManagerDynamicConfig> DynamicConfig_;

    std::vector<TDiskHealthCheckerPtr> LocationHealthCheckers_;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, AliveLocationsLock_);
    std::vector<bool> AliveLocationIndices_;

    std::vector<TError> LocationAlerts_;
    TError NoAliveLocationsAlert_;

    int ShardingKeyLength_;

    NThreading::TAtomicObject<TString> LogFileName_;

    TAsyncSemaphorePtr AsyncSemaphore_;

    NConcurrency::TPeriodicExecutorPtr LocationCheckExecutor_;

    void CreateShardingDirectories() noexcept
    {
        auto createShardingDir = [&](const std::string& prefixPath) {
            auto formatString = Format("%%0%dx", ShardingKeyLength_);

            for (int i = 0; i < Power(16, ShardingKeyLength_); ++i) {
                auto dirName = Format(TRuntimeFormat{formatString}, i);

                auto dirPath = NFS::CombinePaths(prefixPath, dirName);

                YT_LOG_INFO("Creating job proxy logs sharding key directory (DirPath: %v)", dirPath);

                NFS::MakeDirRecursive(dirPath);
            }
        };

        try {
            YT_LOG_INFO("Started creating job proxy sharding key directories");

            for (const auto& location : Config_->Locations) {
                createShardingDir(location->Path);
            }

            createShardingDir(Config_->JobProxyLogSymlinksPath);

            YT_LOG_INFO("Finished creating job proxy sharding key directories");
        } catch (const std::exception& ex) {
            YT_LOG_ERROR(ex, "Failed to create sharding key directories");
        }
    }

    void TraverseJobDirectoriesAndScheduleRemovals() noexcept
    {
        auto currentTime = Now();
        for (const auto& location : Config_->Locations) {
            for (const auto& shardingDirName : NFS::EnumerateDirectories(location->Path)) {
                const auto targetDirForTraverse = NFS::CombinePaths(location->Path, shardingDirName);
                Bootstrap_->GetStorageHeavyInvoker()->Invoke(BIND(
                    &TJobProxyLogManager::TraverseShardingDirectoryAndScheduleRemovals,
                    MakeStrong(this),
                    currentTime,
                    std::move(targetDirForTraverse)));
            }
        }
    }

    void TraverseShardingDirectoryAndScheduleRemovals(TInstant currentTime, const std::string& shardingDirPath) noexcept
    {
        auto Logger = ExecNodeLogger()
            .WithTag("ShardingDirPath: %v", shardingDirPath);

        auto guard = TAsyncSemaphoreGuard();
        if (DynamicConfig_.Acquire()->DirectoryTraversalConcurrency.value_or(Config_->DirectoryTraversalConcurrency) != 0) {
            YT_LOG_INFO("Waiting semaphore to traverse job directory");
            guard = WaitFor(AsyncSemaphore_->AsyncAcquire().AsUnique()).ValueOrThrow();
        }

        auto logsStoragePeriod = DynamicConfig_.Acquire()->LogsStoragePeriod.value_or(Config_->LogsStoragePeriod);

        YT_LOG_INFO(
            "Start traversing job directory (LogsStoragePeriod: %v)",
            logsStoragePeriod);

        for (const auto& jobDirName : NFS::EnumerateDirectories(shardingDirPath)) {
            auto jobDirPath = NFS::CombinePaths(shardingDirPath, jobDirName);
            auto jobLogsDirModificationTime = TInstant::Seconds(TFileStat(TString(jobDirPath)).MTime);
            auto removeJobDirectory = BIND(
                &TJobProxyLogManager::RemoveJobDirectory,
                MakeStrong(this),
                Passed(std::move(jobDirPath)),
                TString(jobDirName));
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

    void RemoveJobDirectory(const TString& targetDirectory, const TString& jobDirectoryName) noexcept
    {
        try {
            NFS::RemoveRecursive(targetDirectory);
            YT_LOG_INFO("Job directory removed (Path: %v)", targetDirectory);
        } catch (const std::exception& ex) {
            YT_LOG_ERROR(
                ex,
                "Failed to remove job directory (Path: %v)",
                targetDirectory);
            return;
        }

        TGuid guid;
        if (!TGuid::FromString(jobDirectoryName, &guid)) {
            YT_LOG_ERROR(
                "Failed to parse job directory name as job ID, skipping symlink removal (DirectoryName: %v)",
                jobDirectoryName);
            return;
        }

        const auto symlinkPath = NFS::CombinePaths({
            Config_->JobProxyLogSymlinksPath,
            GetShardingKey(TJobId(guid)),
            jobDirectoryName
        });

        try {
            NFS::Remove(symlinkPath);
            YT_LOG_INFO("Symlink for job removed (Path: %v)", symlinkPath);
        } catch (const std::exception& ex) {
            YT_LOG_ERROR(
                ex,
                "Failed to remove symlink for job directory (Path: %v)",
                symlinkPath);
        }
    }

    int PickLogStorageIndexFromLocationList(TJobId jobId, const std::vector<bool>& locationIndices) const
    {
        YT_VERIFY(!locationIndices.empty());

        if (std::find(locationIndices.begin(), locationIndices.end(), true) == locationIndices.end()) {
            THROW_ERROR_EXCEPTION("No healthy job proxy log locations available");
        }

        const auto jobHash = THash<TJobId>()(jobId);
        auto bestIndex = -1;
        for (auto it = locationIndices.begin(); it != locationIndices.end(); ++it) {
            ++bestIndex;
            if (*it) {
                break;
            }
        }
        YT_VERIFY(bestIndex >= 0);

        auto bestScore = jobHash;
        HashCombine(bestScore, THash<std::string>()(Config_->Locations[bestIndex]->Path));

        for (int idx = bestIndex; idx < std::ssize(locationIndices); ++idx) {
            if (!locationIndices[idx]) {
                continue;
            }

            auto score = jobHash;
            HashCombine(score, THash<std::string>()(Config_->Locations[idx]->Path));
            if (score > bestScore) {
                bestScore = score;
                bestIndex = idx;
            }
        }

        return bestIndex;
    }

    TString GetJobLogDirectoryPath(TJobId jobId)
    {
        auto shardingKey = GetShardingKey(jobId);

        std::vector<bool> alive;
        {
            auto guard = Guard(AliveLocationsLock_);
            alive = AliveLocationIndices_;
        }

        int index = PickLogStorageIndexFromLocationList(jobId, alive);

        const TString jobLogPath = NFS::CombinePaths({Config_->Locations[index]->Path, shardingKey, ToString(jobId)});

        NFS::MakeDirRecursive(jobLogPath);

        return jobLogPath;
    }

    void BindJobLogDirectoryWithSymlink(TJobId jobId) final
    {
        auto shardingKey = GetShardingKey(jobId);

        const auto targetJobLogPath = GetJobLogDirectoryPath(jobId);
        const auto symlinkPath = NFS::CombinePaths({Config_->JobProxyLogSymlinksPath, shardingKey, ToString(jobId)});
        if (!NFS::Exists(symlinkPath)) {
            NFS::MakeSymbolicLink(targetJobLogPath, symlinkPath);
            YT_LOG_INFO(
                "Created symlink for job directory (SymlinkPath: %v, Target: %v)",
                symlinkPath,
                targetJobLogPath);
        }
    }

    TString FindExistingJobLogsPath(TJobId jobId) const
    {
        auto shardingKey = GetShardingKey(jobId);

        std::vector<bool> aliveLocationsIndices;
        {
            auto guard = Guard(AliveLocationsLock_);
            aliveLocationsIndices = AliveLocationIndices_;
        }

        std::vector<bool> pretendedAliveIndices(aliveLocationsIndices.size(), true);
        int index = PickLogStorageIndexFromLocationList(jobId, pretendedAliveIndices);

        auto path = NFS::CombinePaths({Config_->Locations[index]->Path, shardingKey, ToString(jobId)});
        if (NFS::Exists(path)) {
            if (!aliveLocationsIndices[index]) {
                YT_LOG_WARNING(
                    "Job's log directory is located in a damaged location (JobId: %v, Location: %v)",
                    jobId,
                    Config_->Locations[index]->Path);
            }
            return path;
        }

        for (int location = 0; location < std::ssize(Config_->Locations); ++location) {
            auto path = NFS::CombinePaths({Config_->Locations[location]->Path, shardingKey, ToString(jobId)});
            if (NFS::Exists(path)) {
                if (!aliveLocationsIndices[location]) {
                    YT_LOG_WARNING(
                        "Job's log directory is located in a damaged location (JobId: %v, Location: %v)",
                        jobId,
                        Config_->Locations[location]->Path);
                }
                return path;
            }
        }

        THROW_ERROR_EXCEPTION("Job directory is not found")
            << TErrorAttribute("job_id", jobId);
    }

    void DoDumpJobProxyLog(
        TJobId jobId,
        const NYPath::TYPath& path,
        NObjectClient::TTransactionId transactionId)
    {
        auto logsPath = FindExistingJobLogsPath(jobId);

        auto dynamicConfig = DynamicConfig_.Acquire();

        auto dumpJobProxyLogBufferSize = dynamicConfig->LogDump && dynamicConfig->LogDump->BufferSize
            ? *dynamicConfig->LogDump->BufferSize
            : Config_->LogDump->BufferSize;

        auto logFileName = LogFileName_.Load();

        YT_LOG_DEBUG(
            "Dumping job proxy log file (JobId: %v, LogFileName: %v)",
            jobId,
            logFileName);

        auto logFilePath = NFS::CombinePaths(logsPath, NFS::GetFileName(logFileName));
        auto logFile = [&] {
            try {
                return TFile(logFilePath, OpenExisting | RdOnly);
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
        auto jobProxyConfigTemplate = Bootstrap_->GetJobProxyConfigTemplate();
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

    TString GetShardingKey(TJobId jobId) const
    {
        auto entropy = NScheduler::EntropyFromAllocationId(
            NScheduler::AllocationIdFromJobId(jobId));
        auto entropyHex = Format("%016lx", entropy);
        return entropyHex.substr(0, ShardingKeyLength_);
    }

    void CheckAllLocations() noexcept
    {
        YT_LOG_DEBUG("Location liveness check started");

        std::vector<TError> results(LocationHealthCheckers_.size());
        for (int idx = 0; idx < std::ssize(LocationHealthCheckers_); ++idx) {
            try {
                LocationHealthCheckers_[idx]->RunCheck();
            } catch (const std::exception& ex) {
                results[idx] = TError(ex);
            }
        }

        std::vector<bool> oldAlive;
        {
            auto guard = Guard(AliveLocationsLock_);
            oldAlive = AliveLocationIndices_;
            for (int idx = 0; idx < std::ssize(results); ++idx) {
                AliveLocationIndices_[idx] = results[idx].IsOK();
                if (results[idx].IsOK()) {
                    LocationAlerts_[idx] = TError();
                } else {
                    LocationAlerts_[idx] = TError("Location disabled") << results[idx];
                }
            }

            const bool anyAlive = std::any_of(
                AliveLocationIndices_.begin(),
                AliveLocationIndices_.end(),
                [] (bool v) { return v; });

            NoAliveLocationsAlert_ = anyAlive
                ? TError()
                : TError("All job proxy log locations are disabled");
        }

        for (int idx = 0; idx < std::ssize(results); ++idx) {
            if (oldAlive[idx] && !results[idx].IsOK()) {
                YT_LOG_ERROR(
                    results[idx],
                    "Job proxy log location disabled (Path: %v)",
                    Config_->Locations[idx]->Path);
            } else if (!oldAlive[idx] && results[idx].IsOK()) {
                YT_LOG_INFO(
                    "Job proxy log location recovered (Path: %v)",
                    Config_->Locations[idx]->Path);
            }
        }

        YT_LOG_DEBUG("Location liveness check finished");
    }

    void PopulateAlerts(std::vector<TError>* alerts)
    {
        auto guard = Guard(AliveLocationsLock_);
        for (const auto& alert : LocationAlerts_) {
            if (!alert.IsOK()) {
                alerts->push_back(alert);
            }
        }
        if (!NoAliveLocationsAlert_.IsOK()) {
            alerts->push_back(NoAliveLocationsAlert_);
        }
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
        THROW_ERROR_EXCEPTION("Method is not implemented in simple logging mode");
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

    bool IsEnabled() const override
    {
        THROW_ERROR_EXCEPTION("Method is not implemented in simple logging mode");
    }

    void BindJobLogDirectoryWithSymlink(TJobId /*jobId*/) override
    {
        THROW_ERROR_EXCEPTION("Method is not implemented in simple logging mode");
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
