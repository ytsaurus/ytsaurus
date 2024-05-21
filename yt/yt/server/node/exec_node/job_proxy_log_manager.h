#pragma once

#include "private.h"

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NExecNode {

////////////////////////////////////////////////////////////////////////////////

class TJobProxyLogManager
    : public TRefCounted
{
public:
    explicit TJobProxyLogManager(IBootstrap* bootstrap);
    void Start();
    void OnJobUnregistered(TJobId jobId);

    TString GetShardingKey(TJobId jobId);

    void OnDynamicConfigChanged(
        TJobProxyLogManagerDynamicConfigPtr oldConfig,
        TJobProxyLogManagerDynamicConfigPtr newConfig);

private:
    IBootstrap* const Bootstrap_;

    TJobProxyLogManagerConfigPtr Config_;

    bool Enabled_;

    TString Directory_;
    int ShardingKeyLength_;
    TDuration LogsStoragePeriod_;

    std::optional<int> DirectoryTraversalConcurrency_;
    NConcurrency::TAsyncSemaphorePtr AsyncSemaphore_;

    void CreateShardingDirectories();
    void TraverseJobDirectoriesAndScheduleRemovals();
    void TraverseShardingDirectoryAndScheduleRemovals(TInstant currentTime, TString shardingDirPath);
    void RemoveJobLog(TJobId jobId);
};

DEFINE_REFCOUNTED_TYPE(TJobProxyLogManager);

////////////////////////////////////////////////////////////////////////////////

} // namespace NExecNode::NYT
