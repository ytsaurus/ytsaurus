#include "flow_execute.h"

#include <yt/yt/client/api/client.h>

#include <yt/yt/core/compression/codec.h>

#include <yt/yt/core/concurrency/scheduler_api.h>

#include <yt/yt/core/ytree/convert.h>

#include <util/generic/algorithm.h>

namespace NYT::NFlow {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

void TGetFlowViewArg::Register(TRegistrar registrar)
{
    registrar.Parameter("path", &TThis::Path)
        .Default("");
    registrar.Parameter("cache", &TThis::Cache)
        .Default(false);
    registrar.UnrecognizedStrategy(EUnrecognizedStrategy::Throw);
}

void TGetFlowViewV2Arg::Register(TRegistrar /*registrar*/)
{
    // Inherits path/cache (and the unrecognized-field strategy) from TGetFlowViewArg::Register.
}

void TGetFlowViewV2Result::Register(TRegistrar registrar)
{
    registrar.Parameter("codec", &TThis::Codec);
    registrar.Parameter("data", &TThis::Data);
}

TGetFlowViewResult DecompressFlowView(const TGetFlowViewV2Result& compressed)
{
    auto decompressed = NCompression::GetCodec(compressed.Codec)->Decompress(TSharedRef::FromString(compressed.Data));
    return NYson::TYsonString(decompressed);
}

TGetFlowViewResult GetFlowView(
    const NApi::IClientPtr& client,
    const NYPath::TYPath& pipelinePath,
    const TGetFlowViewArg& arg)
{
    auto flowExecute = [&] (const std::string& command, const NYson::TYsonString& argument) {
        return NConcurrency::WaitFor(client->FlowExecute(pipelinePath, command, argument))
            .ValueOrThrow()
            .Result;
    };

    // The "list" command advertises the controller's supported commands; prefer the compressed v2 when present.
    auto commands = ConvertTo<std::vector<std::string>>(flowExecute("list", NYson::TYsonString(TStringBuf("#"))));
    auto argument = NYson::ConvertToYsonString(arg);
    if (IsIn(commands, "get-flow-view-v2")) {
        return DecompressFlowView(ConvertTo<TGetFlowViewV2Result>(flowExecute("get-flow-view-v2", argument)));
    }
    return flowExecute("get-flow-view", argument);
}

////////////////////////////////////////////////////////////////////////////////

void TGetPipelineDynamicSpecArg::Register(TRegistrar registrar)
{
    registrar.Parameter("path", &TThis::Path)
        .Default("");
    registrar.UnrecognizedStrategy(EUnrecognizedStrategy::Throw);
}

void TGetPipelineDynamicSpecResult::Register(TRegistrar registrar)
{
    registrar.Parameter("spec", &TThis::Spec);
    registrar.Parameter("version", &TThis::Version);
}

////////////////////////////////////////////////////////////////////////////////

void TSetPipelineDynamicSpecArg::Register(TRegistrar registrar)
{
    registrar.Parameter("spec", &TThis::Spec);
    registrar.Parameter("path", &TThis::Path)
        .Default("");
    registrar.Parameter("expected_version", &TThis::ExpectedVersion)
        .Default();
    registrar.UnrecognizedStrategy(EUnrecognizedStrategy::Throw);
}

void TSetPipelineDynamicSpecResult::Register(TRegistrar registrar)
{
    registrar.Parameter("version", &TThis::Version);
}

////////////////////////////////////////////////////////////////////////////////

void TGetPipelineSpecArg::Register(TRegistrar registrar)
{
    registrar.Parameter("path", &TThis::Path)
        .Default("");
    registrar.UnrecognizedStrategy(EUnrecognizedStrategy::Throw);
}

void TGetPipelineSpecResult::Register(TRegistrar registrar)
{
    registrar.Parameter("spec", &TThis::Spec);
    registrar.Parameter("version", &TThis::Version);
}

////////////////////////////////////////////////////////////////////////////////

void TSetPipelineSpecArg::Register(TRegistrar registrar)
{
    registrar.Parameter("spec", &TThis::Spec);
    registrar.Parameter("expected_version", &TThis::ExpectedVersion)
        .Default();
    registrar.Parameter("force", &TThis::Force)
        .Default(false);
    registrar.UnrecognizedStrategy(EUnrecognizedStrategy::Throw);
}

void TSetPipelineSpecResult::Register(TRegistrar registrar)
{
    registrar.Parameter("version", &TThis::Version);
}

////////////////////////////////////////////////////////////////////////////////

void TSetPipelineSpecsArg::Register(TRegistrar registrar)
{
    registrar.Parameter("spec", &TThis::Spec)
        .Default();
    registrar.Parameter("dynamic_spec", &TThis::DynamicSpec)
        .Default();
    registrar.Parameter("expected_spec_version", &TThis::ExpectedSpecVersion)
        .Default();
    registrar.Parameter("expected_dynamic_spec_version", &TThis::ExpectedDynamicSpecVersion)
        .Default();
    registrar.Parameter("allow_spec_update_on_pause", &TThis::AllowSpecUpdateOnPause)
        .Default(false);
    registrar.Parameter("validate_strict", &TThis::ValidateStrict)
        .Default(false);
    registrar.Parameter("force", &TThis::Force)
        .Default(false);
    registrar.Postprocessor([] (TThis* arg) {
        if (arg->Force) {
            arg->AllowSpecUpdateOnPause = true;
            arg->ValidateStrict = false;
        }
    });
    registrar.UnrecognizedStrategy(EUnrecognizedStrategy::Throw);
}

void TSetPipelineSpecsResult::Register(TRegistrar registrar)
{
    registrar.Parameter("spec_version", &TThis::SpecVersion);
    registrar.Parameter("dynamic_spec_version", &TThis::DynamicSpecVersion);
}

////////////////////////////////////////////////////////////////////////////////

void TGetPipelineStateArg::Register(TRegistrar registrar)
{
    registrar.UnrecognizedStrategy(EUnrecognizedStrategy::Throw);
}

void TGetPipelineStateResult::Register(TRegistrar registrar)
{
    registrar.Parameter("pipeline_state", &TThis::PipelineState);
}

////////////////////////////////////////////////////////////////////////////////

void TSetTargetPipelineStateArg::Register(TRegistrar registrar)
{
    registrar.Parameter("target_pipeline_state", &TThis::TargetPipelineState);
    registrar.UnrecognizedStrategy(EUnrecognizedStrategy::Throw);
}

void TSetTargetPipelineStateResult::Register(TRegistrar /*registrar*/)
{ }

////////////////////////////////////////////////////////////////////////////////

void TGetControllerOrchidArg::Register(TRegistrar registrar)
{
    registrar.Parameter("path", &TThis::Path)
        .Default("");
    registrar.UnrecognizedStrategy(EUnrecognizedStrategy::Throw);
}

void TGetControllerOrchidResult::Register(TRegistrar registrar)
{
    registrar.Parameter("value", &TThis::Value)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

void TGetFlowCoreTargetArg::Register(TRegistrar registrar)
{
    registrar.UnrecognizedStrategy(EUnrecognizedStrategy::Throw);
}

void TGetFlowCoreTargetResult::Register(TRegistrar registrar)
{
    registrar.Parameter("flow_core_target", &TThis::FlowCoreTarget);
    registrar.Parameter("version", &TThis::Version);
}

////////////////////////////////////////////////////////////////////////////////

void TSetFlowCoreTargetArg::Register(TRegistrar registrar)
{
    registrar.Parameter("flow_core_target", &TThis::FlowCoreTarget);
    registrar.Parameter("allow_update_on_pause", &TThis::AllowUpdateOnPause)
        .Default(false);
    registrar.Parameter("expected_version", &TThis::ExpectedVersion)
        .Default();
    registrar.UnrecognizedStrategy(EUnrecognizedStrategy::Throw);
}

void TSetFlowCoreTargetResult::Register(TRegistrar registrar)
{
    registrar.Parameter("version", &TThis::Version);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
