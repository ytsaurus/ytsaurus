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

TJobProxyLogManager::TJobProxyLogManager(IBootstrap* bootstrap)
    : Bootstrap_(bootstrap)
    , Config_(Bootstrap_->GetConfig()->ExecNode->JobProxyLogManager)
    , Directory_(Config_->Directory)
    , ShardingKeyLength_(Config_->ShardingKeyLength)
    , LogsStoragePeriod_(Config_->LogsStoragePeriod)
    , DirectoryTraversalConcurrency_(Config_->DirectoryTraversalConcurrency)
    , AsyncSemaphore_(New<NConcurrency::TAsyncSemaphore>(Config_->DirectoryTraversalConcurrency.value_or(0)))
{}

void TJobProxyLogManager::Start()
{
    Bootstrap_->GetStorageHeavyInvoker()->Invoke(BIND(&TJobProxyLogManager::CreateShardingDirectories, MakeStrong(this)));
    Bootstrap_->GetStorageHeavyInvoker()->Invoke(BIND(&TJobProxyLogManager::TraverseJobDirectoriesAndScheduleRemovals, MakeStrong(this)));
}

void TJobProxyLogManager::OnJobUnregistered(TJobId jobId)
{
    NConcurrency::TDelayedExecutor::Submit(
        BIND([this, jobId] {
            RemoveJobLog(jobId);
        }),
        LogsStoragePeriod_,
        Bootstrap_->GetStorageHeavyInvoker()
    );
}

void TJobProxyLogManager::OnDynamicConfigChanged(
    TJobProxyLogManagerDynamicConfigPtr /*oldConfig*/,
    TJobProxyLogManagerDynamicConfigPtr newConfig)
{
    LogsStoragePeriod_ = newConfig->LogsStoragePeriod;
    DirectoryTraversalConcurrency_ = newConfig->DirectoryTraversalConcurrency;
}

void TJobProxyLogManager::CreateShardingDirectories()
{
    auto formatString = Format("%%0%dx", ShardingKeyLength_);
    for (int i = 0; i < Power(16, ShardingKeyLength_); i++) {
        auto dirName = Format(formatString, i);

        auto dirPath = NFS::CombinePaths(Directory_, dirName);
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

TString TJobProxyLogManager::GetShardingKey(TJobId jobId)
{
    auto randomPart = RandomPartFromAllocationId(NScheduler::AllocationIdFromJobId(jobId));
    auto randomPartHex = Format("%016lx", randomPart);
    return randomPartHex.substr(0, ShardingKeyLength_);
}

void TJobProxyLogManager::RemoveJobLog(TJobId jobId)
{
    auto shardingKey = GetShardingKey(jobId);
    auto logsPath = NFS::CombinePaths({Directory_, shardingKey, ToString(jobId)});
    NFS::RemoveRecursive(logsPath);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NExecNode::NYT
