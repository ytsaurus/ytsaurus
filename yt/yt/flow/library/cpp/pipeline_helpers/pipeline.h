#pragma once

#include "public.h"

#include <yt/yt/flow/library/cpp/common/spec.h>
#include <yt/yt/flow/library/cpp/pipeline_helpers/flow_execute/flow_execute.h>

#include <yt/yt/client/api/public.h>

#include <yt/yt/client/scheduler/public.h>

#include <yt/yt/client/ypath/rich.h>

#include <util/datetime/base.h>
#include <util/generic/string.h>

#include <optional>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

//! Returns the YT_FLOW_GRACEFUL_UPDATE env var as bool (defaults to true).
//! Used by the runner to decide between StopPipeline (graceful) and PausePipeline.
bool IsGracefulUpdateFromEnv();

////////////////////////////////////////////////////////////////////////////////

//! The vanilla operation a pipeline runs in. The runner checks it whenever the controller
//! is unreachable: a terminal operation ends the wait with an error right away.
struct TVanillaOperationHandle
{
    NApi::IClientPtr Client;
    NScheduler::TOperationId OperationId;

    //! Throws when the operation is completed, failed or aborted; a failed lookup is logged and ignored.
    void ThrowIfTerminal() const;
};

////////////////////////////////////////////////////////////////////////////////

//! Polls until the target state is reached or the overall wait budget expires.
//! The timeout passed to each GetPipelineState request is the lesser of requestTimeout
//! and the remaining wait budget.
void WaitPipelineState(
    const std::string& clusterUrl,
    const std::optional<std::string>& proxyRole,
    const NYPath::TYPath& root,
    EPipelineState state,
    TDuration waitTimeout = DefaultWaitPipelineTimeout,
    TDuration requestTimeout = DefaultWaitPipelineStateRequestTimeout);

//! Same as above, using an existing client.
void WaitPipelineState(
    NApi::IClientPtr client,
    const NYPath::TYPath& root,
    EPipelineState state,
    TDuration waitTimeout = DefaultWaitPipelineTimeout,
    TDuration requestTimeout = DefaultWaitPipelineStateRequestTimeout);

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

void RunPipeline(
    NApi::IClientPtr client,
    const NYPath::TYPath& root,
    const TPipelineSpecPtr& spec,
    const TDynamicPipelineSpecPtr& dynamicSpec,
    bool setFlowCoreTarget = true,
    std::optional<bool> graceful = {},
    TDuration waitTimeout = DefaultWaitPipelineTimeout,
    bool enablePipelineCreation = true,
    bool enablePipelineStopOrPause = true,
    const std::optional<TVanillaOperationHandle>& vanillaOperation = {});

void WaitPipeline(
    const std::string& clusterUrl,
    const std::optional<std::string>& proxyRole,
    const NYPath::TYPath& root);

//! Tails the controller log until the pipeline completes or the controller stays
//! unreachable for |controllerUnavailableTimeout|.
void WaitPipeline(
    NApi::IClientPtr client,
    const NYPath::TRichYPath& pipelinePath,
    TDuration controllerUnavailableTimeout = DefaultWaitPipelineTimeout,
    const std::optional<TVanillaOperationHandle>& vanillaOperation = {});

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
