#pragma once

#include "private.h"
#include "job.h"

#include <yt/yt/library/containers/public.h>

#include <yt/yt/core/actions/public.h>
#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/logging/log.h>

#include <yt/yt/core/yson/string.h>

#include <yt/yt/core/misc/public.h>

#include <yt/yt/ytlib/scheduler/helpers.h>

namespace NYT::NExecNode {

////////////////////////////////////////////////////////////////////////////////

struct TJobGpuCheckerContext
{
    IUserSlotPtr Slot;
    TJobPtr Job;
    NContainers::TRootFS RootFS;
    TString CommandUser;

    std::vector<TShellCommandConfigPtr> SetupCommands;

    TString GpuCheckBinaryPath;
    std::vector<TString> GpuCheckBinaryArgs;
    THashMap<TString, TString> GpuCheckEnvironment;
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
    TJobGpuChecker(TJobGpuCheckerContext context, NLogging::TLogger logger);

    TFuture<void> RunGpuCheck();

    ~TJobGpuChecker();

private:
    DECLARE_THREAD_AFFINITY_SLOT(JobThread);

    const TJobGpuCheckerContext Context_;

    NLogging::TLogger Logger;

    static void OnGpuCheckFinished(TJobGpuCheckerPtr checker, TErrorOr<std::vector<TShellCommandOutput>>&& result);
};

DEFINE_REFCOUNTED_TYPE(TJobGpuChecker)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
