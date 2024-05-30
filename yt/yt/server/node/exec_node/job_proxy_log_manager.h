#pragma once

#include "private.h"

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NExecNode {

////////////////////////////////////////////////////////////////////////////////

struct IJobProxyLogManager
    : public TRefCounted
{
    virtual void Start() = 0;

    virtual void OnJobUnregistered(TJobId jobId) = 0;

    virtual TString GetShardingKey(TJobId jobId) = 0;

    virtual void OnDynamicConfigChanged(
        TJobProxyLogManagerDynamicConfigPtr oldConfig,
        TJobProxyLogManagerDynamicConfigPtr newConfig) = 0;
};

DEFINE_REFCOUNTED_TYPE(IJobProxyLogManager);

////////////////////////////////////////////////////////////////////////////////

IJobProxyLogManagerPtr CreateJobProxyLogManager(IBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NExecNode::NYT
