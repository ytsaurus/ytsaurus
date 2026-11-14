#pragma once

#include "public.h"

#include <yt/yt/server/lib/scheduler/public.h>

#include <yt/yt/core/misc/error.h>

namespace NYT::NExecNode {

////////////////////////////////////////////////////////////////////////////////

std::string GetRootFSUserDirectory();

const std::string& GetSandboxRelPath(ESandboxKind sandboxKind);

////////////////////////////////////////////////////////////////////////////////

TErrorCode GetDisableErrorCode(
    const TError& error,
    TErrorCode fallbackErrorCode);

////////////////////////////////////////////////////////////////////////////////

NScheduler::EAllocationState JobStateToAllocationState(EJobState jobState);

////////////////////////////////////////////////////////////////////////////////

//! Returns no reason if the error should fail the job rather than abort it.
std::optional<NScheduler::EAbortReason> DeduceJobAbortReason(
    const TError& resultError,
    const TJobCommonConfig& config,
    bool abortJobIfAccountLimitExceeded,
    bool hasFailedChunks);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
