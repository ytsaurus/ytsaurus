#pragma once

#include "public.h"

#include <yt/yt/flow/library/cpp/common/flow_view.h>
#include <yt/yt/flow/library/cpp/common/stream_inflight_limits.h>

#include <yt/yt/core/actions/invoker.h>

namespace NYT::NFlow::NWorker {

////////////////////////////////////////////////////////////////////////////////

struct TJobStreamLimitUsageStates
{
    TStreamLimitUsageStateMap Input;
    TStreamLimitUsageStateMap Output;
};

struct IBufferStateManager
    : public TRefCounted
{
    virtual TJobStreamLimitUsageStates RegisterJob(TJobId jobId, const TJobSpecPtr& jobSpec) = 0;
    virtual void RemoveJob(TJobId jobId) = 0;
    virtual void Reconfigure(TDynamicBufferStateManagerSpecPtr dynamicSpec) = 0;
    virtual void ManageBuffers() = 0;
    virtual void UpdateMessageTransferingInfo(TMessageTransferingInfoPtr messageTransferingInfo) = 0;
};

DEFINE_REFCOUNTED_TYPE(IBufferStateManager);

////////////////////////////////////////////////////////////////////////////////

IBufferStateManagerPtr CreateBufferStateManager(
    IInvokerPtr invoker,
    IJobDirectoryPtr jobDirectory,
    TDynamicBufferStateManagerSpecPtr dynamicSpec,
    std::function<TInstant()> timeProvider = [] {
        return TInstant::Now();
    });

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NWorker
