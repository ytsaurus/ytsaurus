#pragma once

#include "public.h"

#include "config.h"

#include <yt/yt/flow/library/cpp/client/flow_execute/flow_execute.h>

#include <yt/yt/core/http/public.h>

namespace NYT::NFlow::NController {

////////////////////////////////////////////////////////////////////////////////

//! Handler of flow_execute command of controller service.
struct IFlowExecutor
    : public TRefCounted
{
    virtual NYson::TYsonString Execute(const std::string& command, const NYson::TYsonString& argument, const std::string& user) = 0;

    virtual TGetFlowViewResult GetFlowView(const TGetFlowViewArg& argument) = 0;

    virtual TGetPipelineDynamicSpecResult GetPipelineDynamicSpec(const TGetPipelineDynamicSpecArg& argument) = 0;
    virtual TSetPipelineDynamicSpecResult SetPipelineDynamicSpec(const TSetPipelineDynamicSpecArg& argument) = 0;

    virtual TGetPipelineSpecResult GetPipelineSpec(const TGetPipelineSpecArg& argument) = 0;
    virtual TSetPipelineSpecResult SetPipelineSpec(const TSetPipelineSpecArg& argument) = 0;

    virtual TSetPipelineSpecsResult SetPipelineSpecs(const TSetPipelineSpecsArg& argument) = 0;

    virtual TGetPipelineStateResult GetPipelineState(const TGetPipelineStateArg& argument) = 0;
    virtual TSetTargetPipelineStateResult SetTargetPipelineState(const TSetTargetPipelineStateArg& argument) = 0;

    virtual TGetFlowCoreTargetResult GetFlowCoreTarget(const TGetFlowCoreTargetArg& argument) = 0;
    virtual TSetFlowCoreTargetResult SetFlowCoreTarget(const TSetFlowCoreTargetArg& argument) = 0;
};

IFlowExecutorPtr CreateFlowExecutor(
    IControllerPtr controller,
    IPersistedStateManagerPtr persistedStateManager,
    IYTConnectorPtr ytConnector,
    TControllerServiceConfigPtr config,
    NYTree::IMapNodePtr orchidRoot,
    IStatusProfilerPtr rootStatusProfiler,
    IInvokerPtr poolInvoker,
    NHttp::IClientPtr httpClient = {},
    NRpc::IChannelFactoryPtr channelFactory = {},
    IPipelineAuthenticatorPtr authenticator = {});

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NController
