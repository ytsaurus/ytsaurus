#include "job_proxy_log_manager.h"

#include "bootstrap.h"
#include "private.h"

#include <yt/yt/server/lib/exec_node/config.h>
#include <yt/yt/server/lib/scheduler/helpers.h>
#include <yt/yt/server/node/cluster_node/config.h>

#include <yt/yt/core/misc/fs.h>

#include <util/datetime/base.h>
#include <util/generic/ymath.h>
#include <util/system/fstat.h>

namespace NYT::NExecNode {

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = ExecNodeLogger;

////////////////////////////////////////////////////////////////////////////////

class TJobProxyLogManager
    : public IJobProxyLogManager
{
public:
    TJobProxyLogManager(IBootstrap* bootstrap)
        : Bootstrap_(bootstrap)
        , Config_(Bootstrap_->GetConfig()->ExecNode->JobProxyLogManager)
        , Directory_(Config_->Directory)
        , ShardingKeyLength_(Config_->ShardingKeyLength)
        , LogsStoragePeriod_(Config_->LogsStoragePeriod)
        , DirectoryTraversalConcurrency_(Config_->DirectoryTraversalConcurrency)
        , AsyncSemaphore_(New<NConcurrency::TAsyncSemaphore>(Config_->DirectoryTraversalConcurrency.value_or(0)))
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
        NConcurrency::TDelayedExecutor::Submit(
            BIND(&TJobProxyLogManager::RemoveJobLog, MakeStrong(this), jobId),
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

private:
    IBootstrap* const Bootstrap_;

    TJobProxyLogManagerConfigPtr Config_;

    TString Directory_;
    int ShardingKeyLength_;
    TDuration LogsStoragePeriod_;

    std::optional<int> DirectoryTraversalConcurrency_;
    NConcurrency::TAsyncSemaphorePtr AsyncSemaphore_;

    void CreateShardingDirectories()
    {
        YT_LOG_INFO("Start creating job proxy sharding key directories");

        TRuntimeFormat formatString{Format("%%0%dx", ShardingKeyLength_)};
        for (int i = 0; i < Power(16, ShardingKeyLength_); i++) {
            auto dirName = Format(formatString, i);

            auto dirPath = NFS::CombinePaths(Directory_, dirName);
            NFS::MakeDirRecursive(dirPath);
        }

        YT_LOG_INFO("Finish creating job proxy sharding key directories");
    }

    void TraverseJobDirectoriesAndScheduleRemovals()
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

    void TraverseShardingDirectoryAndScheduleRemovals(TInstant currentTime, TString shardingDirPath)
    {
        auto Logger = ExecNodeLogger()
            .WithTag("ShardingDirPath: %v", shardingDirPath);

        auto guard = NConcurrency::TAsyncSemaphoreGuard();
        if (DirectoryTraversalConcurrency_.has_value()) {
            YT_LOG_INFO("Waiting semaphore to traverse job directory");
            guard = NConcurrency::WaitForUnique(AsyncSemaphore_->AsyncAcquire()).ValueOrThrow();
        }

        YT_LOG_INFO("Start traversing job directory");

        for (const auto& jobDirName : NFS::EnumerateDirectories(shardingDirPath)) {
            auto jobDirPath = NFS::CombinePaths(shardingDirPath, jobDirName);
            auto jobLogsDirModificationTime = TInstant::Seconds(TFileStat(jobDirPath).MTime);
            auto removeLogsAction = BIND([path = std::move(jobDirPath)] {
                NFS::RemoveRecursive(path);
            });
            if (jobLogsDirModificationTime + LogsStoragePeriod_ <= currentTime) {
                Bootstrap_->GetStorageHeavyInvoker()->Invoke(removeLogsAction);
            } else {
                NConcurrency::TDelayedExecutor::Submit(
                    removeLogsAction,
                    jobLogsDirModificationTime + LogsStoragePeriod_,
                    Bootstrap_->GetStorageHeavyInvoker());
            }
        }

        YT_LOG_INFO("Finish traversing job directory");
    }

    void RemoveJobLog(TJobId jobId)
    {
        auto shardingKey = GetShardingKey(jobId);
        auto logsPath = NFS::CombinePaths({Directory_, shardingKey, ToString(jobId)});
        YT_LOG_INFO("Removing job directory (Path: %v)", logsPath);
        NFS::RemoveRecursive(logsPath);
    }
};

DEFINE_REFCOUNTED_TYPE(TJobProxyLogManager);

////////////////////////////////////////////////////////////////////////////////

class TMockJobProxyLogManager
    : public IJobProxyLogManager
{
public:
    void Start() override
    {}

    void OnJobUnregistered(TJobId /*jobId*/) override
    {}

    TString GetShardingKey(TJobId /*jobId*/) override
    {
        THROW_ERROR_EXCEPTION("Method GetShardingKey is not supported in simple JobProxy logging mode");
    }

    void OnDynamicConfigChanged(
        TJobProxyLogManagerDynamicConfigPtr /*oldConfig*/,
        TJobProxyLogManagerDynamicConfigPtr /*newConfig*/) override
    {}
};

DEFINE_REFCOUNTED_TYPE(TMockJobProxyLogManager);

////////////////////////////////////////////////////////////////////////////////

IJobProxyLogManagerPtr CreateJobProxyLogManager(IBootstrap* bootstrap)
{
    if (bootstrap->GetConfig()->ExecNode->JobProxy->JobProxyLogging->Mode == EJobProxyLoggingMode::Simple) {
        return New<TMockJobProxyLogManager>();
    }
    return New<TJobProxyLogManager>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NExecNode::NYT
