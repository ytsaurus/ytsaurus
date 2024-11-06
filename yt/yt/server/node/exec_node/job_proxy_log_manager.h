#pragma once

#include "private.h"

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NExecNode {

////////////////////////////////////////////////////////////////////////////////

struct IJobProxyLogManager
    : public TRefCounted
{
    virtual void Initialize() = 0;
    virtual void Start() = 0;

    virtual TString AdjustLogPath(TJobId jobId, const TString& logFilePath) = 0;

    virtual void OnDynamicConfigChanged(
        const TJobProxyLogManagerDynamicConfigPtr& oldConfig,
        const TJobProxyLogManagerDynamicConfigPtr& newConfig) = 0;

    virtual TFuture<void> DumpJobProxyLog(
        TJobId jobId,
        const NYPath::TYPath& path,
        NObjectClient::TTransactionId transactionId) = 0;
};

DEFINE_REFCOUNTED_TYPE(IJobProxyLogManager);

////////////////////////////////////////////////////////////////////////////////

IJobProxyLogManagerPtr CreateJobProxyLogManager(IBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
