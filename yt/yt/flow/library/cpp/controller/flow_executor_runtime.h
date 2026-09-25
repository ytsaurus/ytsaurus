#pragma once

#include "flow_executor.h"

#include "describe/describe_pipeline.h"

#include <functional>

namespace NYT::NFlow::NController {

////////////////////////////////////////////////////////////////////////////////

//! Supplies environment-dependent data and operations to the shared flow command executor.
struct IFlowExecutorRuntime
    : public TRefCounted
{
    using TCommandHandler = std::function<NYson::TYsonString()>;

    //! Runs |handler| synchronously inside runtime-specific command guards and does not retain it.
    virtual NYson::TYsonString RunCommand(
        const std::string& command,
        const NYson::TYsonString& argument,
        const std::string& user,
        const TCommandHandler& handler) = 0;

    virtual void AuthorizeCommand(NYTree::EPermission permission, const std::string& user) = 0;

    virtual TGetFlowViewResult GetFlowView(const TGetFlowViewArg& argument) = 0;
    virtual TGetFlowViewV2Result GetFlowViewV2(const TGetFlowViewV2Arg& argument) = 0;
    virtual TVersionedPipelineSpecPtr GetPipelineSpec() = 0;
    virtual TVersionedDynamicPipelineSpecPtr GetPipelineDynamicSpec() = 0;
    virtual TVersionedFlowCoreTargetPtr GetFlowCoreTarget() = 0;

    virtual TFlowViewPtr GetDescribeFlowView() = 0;
    virtual THashMap<std::string, TError> GetDescribeControllerErrors() = 0;
    virtual NDescribe::TDescribePipelineArguments MakeDescribePipelineArguments(bool statusOnly) = 0;
    virtual TErrorOr<NYson::TYsonString> GetDescribeJobOrchid(
        const TPartitionId& partitionId,
        TDuration timeout) = 0;
    virtual std::string GetDescribeDeployStageUrl() = 0;

    virtual TSetPipelineDynamicSpecResult SetPipelineDynamicSpec(const TSetPipelineDynamicSpecArg& argument) = 0;
    virtual TSetPipelineSpecResult SetPipelineSpec(const TSetPipelineSpecArg& argument) = 0;
    virtual TSetPipelineSpecsResult SetPipelineSpecs(const TSetPipelineSpecsArg& argument) = 0;
    virtual TSetTargetPipelineStateResult SetTargetPipelineState(const TSetTargetPipelineStateArg& argument) = 0;
    virtual TSetFlowCoreTargetResult SetFlowCoreTarget(const TSetFlowCoreTargetArg& argument) = 0;

    virtual NYson::TYsonString GetWorkerOrchid(const NYson::TYsonString& argument) = 0;
    virtual NYson::TYsonString GetControllerOrchid(const NYson::TYsonString& argument) = 0;
    virtual NYson::TYsonString KillWorker(const NYson::TYsonString& argument) = 0;
    virtual NYson::TYsonString UpdateWorker(const NYson::TYsonString& argument) = 0;
    virtual NYson::TYsonString GetWorkerBacktraces(const NYson::TYsonString& argument) = 0;
    virtual NYson::TYsonString ReadStates(const NYson::TYsonString& argument) = 0;
    virtual NYson::TYsonString DeleteStates(const NYson::TYsonString& argument) = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NController
