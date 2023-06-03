#pragma once

#include "public.h"
#include "chunk_cache.h"
#include "job.h"
#include "slot.h"

#include <yt/yt/server/lib/job_agent/public.h>

#include <yt/yt/library/containers/public.h>

#include <yt/yt/core/actions/public.h>
#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/logging/log.h>

#include <yt/yt/core/yson/string.h>

#include <yt/yt/core/misc/public.h>

#include <yt/yt/ytlib/scheduler/helpers.h>

namespace NYT::NExecNode
{

////////////////////////////////////////////////////////////////////////////////

struct TJobGpuCheckerSettings
{
    IUserSlotPtr Slot;
    TJobPtr Job;
    NContainers::TRootFS RootFs;
    TString CommandUser;

    TString GpuCheckBinaryPath;
    std::vector<TString> GpuCheckBinaryArgs;
    EGpuCheckType GpuCheckType;
    int CurrentStartIndex;
    bool TestExtraGpuCheckCommandFailure;
    std::vector<NContainers::TDevice> GpuDevices;
};

////////////////////////////////////////////////////////////////////////////////

class TJobGpuChecker
    : public TRefCounted
{
public:
    DEFINE_SIGNAL(void(), RunCheck);
    DEFINE_SIGNAL(void(), FinishCheck);

    DEFINE_BYVAL_RO_PROPERTY(std::optional<TInstant>, GpuCheckStartTime);
    DEFINE_BYVAL_RO_PROPERTY(std::optional<TInstant>, GpuCheckFinishTime);

public:
    TJobGpuChecker(const TJobGpuCheckerSettings& settings);

    TFuture<void> RunGpuCheck();

private:
    DECLARE_THREAD_AFFINITY_SLOT(JobThread);

    const TJobGpuCheckerSettings Settings_;

};

DEFINE_REFCOUNTED_TYPE(TJobGpuChecker)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
