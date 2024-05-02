#pragma once

#include "private.h"

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NExecNode {

////////////////////////////////////////////////////////////////////////////////

class TJobProxyLogManagerState
    : public NYTree::TYsonStruct
{
public:
    THashMap<TJobId, TInstant> JobIdToModificationTime;

    REGISTER_YSON_STRUCT(TJobProxyLogManagerState);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TJobProxyLogManagerState);

////////////////////////////////////////////////////////////////////////////////

class TJobProxyLogManager
    : public TRefCounted
{
public:
    explicit TJobProxyLogManager(IBootstrap* bootstrap);
    void Start();
    void OnJobFinished(TJobId jobId);

    TString GetShardingKey(TJobId jobId);

private:
    IBootstrap* const Bootstrap_;

    TString Directory_;
    int ShardingKeyLength_;
    TDuration LogsDeadline_;

    NConcurrency::TAsyncSemaphorePtr AsyncSemaphore_;

    void CreateShardingDirectories();
    void RemoveOutdatedLogs();
    void RemoveJobLog(TJobId jobId);
};

DEFINE_REFCOUNTED_TYPE(TJobProxyLogManager);

////////////////////////////////////////////////////////////////////////////////

} // namespace NExecNode::NYT
