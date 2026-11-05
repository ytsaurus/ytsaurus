#pragma once

#include "flow_executor.h"
#include "persisted_state_manager.h"

#include "describe/describe_pipeline.h"

#include <functional>

namespace NYT::NFlow::NController {

////////////////////////////////////////////////////////////////////////////////

//! Throws unless a spec change is allowed in |observed|; the state rule applies only to static changes.
void ValidatePipelineSafeForSpecUpdate(const TObservedPipelineState& observed, bool staticSpecChanged, bool force);

//! Throws unless a FlowCoreTarget change is allowed in |observed|.
void ValidatePipelineSafeForFlowCoreTargetUpdate(const TObservedPipelineState& observed, bool allowOnPause);

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
    //! Return the persisted versioned specs as raw nodes, so that unparsable ones can still be viewed;
    //! null when absent.
    virtual NYTree::INodePtr GetPipelineSpecNode() = 0;
    virtual NYTree::INodePtr GetPipelineDynamicSpecNode() = 0;
    virtual TVersionedFlowCoreTargetPtr GetFlowCoreTarget() = 0;
    //! Returns the pipeline state, or Unknown when a FlowView exists and its applied specs differ from
    //! the canonical ones.
    virtual EPipelineState GetPipelineState() = 0;

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
