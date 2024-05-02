#include "job_proxy_log_manager.h"

#include "bootstrap.h"
#include "private.h"

#include <util/datetime/base.h>
#include <util/generic/ymath.h>
#include <util/system/fstat.h>

#include <yt/yt/core/misc/fs.h>
#include <yt/yt/server/lib/exec_node/config.h>
#include <yt/yt/server/lib/scheduler/helpers.h>
#include <yt/yt/server/node/cluster_node/config.h>

namespace NYT::NExecNode {

////////////////////////////////////////////////////////////////////////////////

// static const auto& Logger = ExecNodeLogger;

////////////////////////////////////////////////////////////////////////////////

void TJobProxyLogManagerState::Register(TRegistrar registrar)
{
    registrar.Parameter("job_id_to_modification_time", &TThis::JobIdToModificationTime)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

TJobProxyLogManager::TJobProxyLogManager(IBootstrap* bootstrap)
    : Bootstrap_(bootstrap)
    , Directory_(bootstrap->GetConfig()->ExecNode->JobProxyLogManager->Directory)
    , ShardingKeyLength_(bootstrap->GetConfig()->ExecNode->JobProxyLogManager->ShardingKeyLength)
    , LogsDeadline_(bootstrap->GetConfig()->ExecNode->JobProxyLogManager->LogsDeadline)
    , AsyncSemaphore_(New<NConcurrency::TAsyncSemaphore>(bootstrap->GetConfig()->ExecNode->JobProxyLogManager->MaxParallelism.value_or(0)))
{}

void TJobProxyLogManager::Start()
{
    CreateShardingDirectories();
    RemoveOutdatedLogs();
}

void TJobProxyLogManager::OnJobFinished(TJobId jobId)
{
    NConcurrency::TDelayedExecutor::Submit(
        BIND([this, jobId] {
            RemoveJobLog(jobId);
        }),
        LogsDeadline_,
        Bootstrap_->GetStorageHeavyInvoker()
    );
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

void TJobProxyLogManager::RemoveOutdatedLogs()
{
    auto invoker = Bootstrap_->GetStorageHeavyInvoker();

    auto currentTime = Now();

    for (const auto& shardingKeyDirName : NFS::EnumerateDirectories(Directory_)) {
        auto shardingKeyDirPath = NFS::CombinePaths(Directory_, shardingKeyDirName);
        invoker->Invoke(BIND([this, currentTime, shardingKeyDirPath, invoker] {
            if (Bootstrap_->GetConfig()->ExecNode->JobProxyLogManager->MaxParallelism.has_value()) {
                this->AsyncSemaphore_->Acquire();
            }
            for (const auto& jobDirName : NFS::EnumerateDirectories(shardingKeyDirPath)) {
                auto jobDirPath = NFS::CombinePaths(shardingKeyDirPath, jobDirName);
                // Check directory mtime
                auto jobLogsDirModificationTime = TInstant::Seconds(TFileStat(jobDirPath).MTime);
                auto diff = currentTime - jobLogsDirModificationTime;
                auto removeLogsAction = BIND([path = std::move(jobDirPath)] {
                    NFS::RemoveRecursive(path);
                });
                if (diff >= LogsDeadline_) {
                    invoker->Invoke(removeLogsAction);
                } else {
                    NConcurrency::TDelayedExecutor::Submit(
                        removeLogsAction,
                        LogsDeadline_ - diff,
                        invoker
                    );
                }
            }
            if (Bootstrap_->GetConfig()->ExecNode->JobProxyLogManager->MaxParallelism.has_value()) {
                this->AsyncSemaphore_.Release();
            }
        }));
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
