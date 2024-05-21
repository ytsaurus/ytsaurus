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

static auto& Logger = ExecNodeLogger;

////////////////////////////////////////////////////////////////////////////////

TJobProxyLogManager::TJobProxyLogManager(IBootstrap* bootstrap)
    : Bootstrap_(bootstrap)
    , Config_(Bootstrap_->GetConfig()->ExecNode->JobProxyLogManager)
    , Enabled_(Bootstrap_->GetConfig()->ExecNode->JobProxy->JobProxyLogging->Mode == EJobProxyLoggingMode::PerJobDirectory)
    , Directory_(Config_->Directory)
    , ShardingKeyLength_(Config_->ShardingKeyLength)
    , LogsStoragePeriod_(Config_->LogsStoragePeriod)
    , DirectoryTraversalConcurrency_(Config_->DirectoryTraversalConcurrency)
    , AsyncSemaphore_(New<NConcurrency::TAsyncSemaphore>(Config_->DirectoryTraversalConcurrency.value_or(0)))
{}

void TJobProxyLogManager::Start()
{
    if (!Enabled_) {
        return;
    }

    Bootstrap_->GetStorageHeavyInvoker()->Invoke(BIND(
        [this, this_ = MakeStrong(this)] {
            YT_LOG_INFO("Starting JobProxyLogManager");
            CreateShardingDirectories();
            TraverseJobDirectoriesAndScheduleRemovals();
        }));
}

void TJobProxyLogManager::OnJobUnregistered(TJobId jobId)
{
    if (!Enabled_) {
        return;
    }

    YT_LOG_INFO("Job unregistered %v, log storage period: %v", jobId, LogsStoragePeriod_);
    NConcurrency::TDelayedExecutor::Submit(
        BIND(&TJobProxyLogManager::RemoveJobLog, MakeStrong(this), jobId),
        LogsStoragePeriod_,
        Bootstrap_->GetStorageHeavyInvoker()
    );
}

TString TJobProxyLogManager::GetShardingKey(TJobId jobId)
{
    auto randomPart = RandomPartFromAllocationId(NScheduler::AllocationIdFromJobId(jobId));
    auto randomPartHex = Format("%016lx", randomPart);
    return randomPartHex.substr(0, ShardingKeyLength_);
}

void TJobProxyLogManager::OnDynamicConfigChanged(
    TJobProxyLogManagerDynamicConfigPtr /*oldConfig*/,
    TJobProxyLogManagerDynamicConfigPtr newConfig)
{
    LogsStoragePeriod_ = newConfig->LogsStoragePeriod;
    DirectoryTraversalConcurrency_ = newConfig->DirectoryTraversalConcurrency;
    AsyncSemaphore_->SetTotal(DirectoryTraversalConcurrency_.value_or(0));
}

void TJobProxyLogManager::CreateShardingDirectories()
{
    auto formatString = Format("%%0%dx", ShardingKeyLength_);
    for (int i = 0; i < Power(16, ShardingKeyLength_); i++) {
        auto dirName = Format(formatString, i);

        auto dirPath = NFS::CombinePaths(Directory_, dirName);
        YT_LOG_INFO("Creating Directory %v", dirPath);
        NFS::MakeDirRecursive(dirPath);
    }
}

void TJobProxyLogManager::TraverseJobDirectoriesAndScheduleRemovals()
{
    auto currentTime = Now();

    for (const auto& shardingDirName : NFS::EnumerateDirectories(Directory_)) {
        auto shardingDirPath = NFS::CombinePaths(Directory_, shardingDirName);
        Bootstrap_->GetStorageHeavyInvoker()->Invoke(BIND(
            &TJobProxyLogManager::TraverseShardingDirectoryAndScheduleRemovals,
            MakeStrong(this),
            currentTime,
            std::move(shardingDirPath)
        ));
    }
}

void TJobProxyLogManager::TraverseShardingDirectoryAndScheduleRemovals(TInstant currentTime, TString shardingDirPath)
{
    auto guard = NConcurrency::TAsyncSemaphoreGuard();
    if (DirectoryTraversalConcurrency_.has_value()) {
        guard = NConcurrency::WaitForUnique(AsyncSemaphore_->AsyncAcquire()).ValueOrThrow();
    }
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
                Bootstrap_->GetStorageHeavyInvoker()
            );
        }
    }
}

void TJobProxyLogManager::RemoveJobLog(TJobId jobId)
{
    auto shardingKey = GetShardingKey(jobId);
    auto logsPath = NFS::CombinePaths({Directory_, shardingKey, ToString(jobId)});
    YT_LOG_INFO("Removing JobProxy log, jobId: %v, path: %v", jobId, logsPath);
    NFS::RemoveRecursive(logsPath);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NExecNode::NYT
