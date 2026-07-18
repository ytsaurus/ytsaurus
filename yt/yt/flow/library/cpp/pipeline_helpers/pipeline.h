#pragma once

#include "public.h"

#include <yt/yt/flow/library/cpp/common/spec.h>
#include <yt/yt/flow/library/cpp/pipeline_helpers/flow_execute/flow_execute.h>

#include <yt/yt/client/api/public.h>

#include <util/datetime/base.h>
#include <util/generic/string.h>

#include <optional>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

//! Returns the YT_FLOW_GRACEFUL_UPDATE env var as bool (defaults to true).
//! Used by the runner to decide between StopPipeline (graceful) and PausePipeline.
bool IsGracefulUpdateFromEnv();

////////////////////////////////////////////////////////////////////////////////

void WaitPipelineState(
    const std::string& clusterUrl,
    const std::optional<std::string>& proxyRole,
    const NYPath::TYPath& root,
    EPipelineState state,
    TDuration waitTimeout = DefaultWaitPipelineTimeout);

void WaitPipelineState(
    NApi::IClientPtr client,
    const NYPath::TYPath& root,
    EPipelineState state,
    TDuration waitTimeout = DefaultWaitPipelineTimeout);

void RunPipeline(
    const std::string& clusterUrl,
    const std::optional<std::string>& proxyRole,
    const NYPath::TYPath& root,
    const TPipelineSpecPtr& spec,
    const TDynamicPipelineSpecPtr& dynamicSpec,
    bool setFlowCoreTarget = true,
    std::optional<bool> graceful = {},
    TDuration waitTimeout = DefaultWaitPipelineTimeout,
    bool enablePipelineCreation = true,
    bool enablePipelineStopOrPause = true);

void WaitPipeline(
    const std::string& clusterUrl,
    const std::optional<std::string>& proxyRole,
    const NYPath::TYPath& root);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
