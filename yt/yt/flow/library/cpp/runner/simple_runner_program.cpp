#include "simple_runner_program.h"

#include "debug_build_warning.h"
#include "init.h"
#include "public.h"

#include <yt/yt/flow/library/cpp/client/pipeline.h>
#include <yt/yt/flow/library/cpp/common/internal_urls.h>
#include <yt/yt/flow/library/cpp/common/registry.h>
#include <yt/yt/flow/library/cpp/common/spec.h>
#include <yt/yt/flow/library/cpp/common/yson_message.h>

#include <yt/yt/core/logging/log_manager.h>
#include <yt/yt/core/net/address.h>

#include <yt/yt/library/program/program.h>
#include <yt/yt/library/program/program_config_mixin.h>

#include <library/cpp/colorizer/colors.h>
#include <library/cpp/iterator/concatenate.h>

#include <util/system/env.h>

namespace NYT::NFlow {

using namespace NYTree;
using namespace NLogging;

////////////////////////////////////////////////////////////////////////////////

namespace {

// Prints the pipeline graph URL in the YT UI as a colored, framed banner so the user can quickly
// open the running pipeline. Skipped when the cluster name cannot be inferred (e.g. a local
// cluster) or the UI prefix is not configured (open-source build).
void PrintPipelineUiUrl(const std::string& clusterUrl, const NYPath::TYPath& path)
{
    const std::string_view ytUrlPrefix = NInternalUrls::YtUrlPrefix;
    auto clusterName = NNet::InferYTClusterFromClusterUrl(clusterUrl);
    if (ytUrlPrefix.empty() || !clusterName || clusterName->empty()) {
        return;
    }

    auto url = Format("%v%v/flows/graph?path=%v", ytUrlPrefix, *clusterName, path);

    const auto& colors = NColorizer::StdErr();
    static constexpr TStringBuf frame = "========================================================================";
    Cerr << '\n'
         << frame << '\n'
         << colors.LightGreenColor() << "  Pipeline UI: " << url << colors.OldColor() << '\n'
         << frame << Endl;
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

void TSimpleRunnerConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("cluster_url", &TThis::ClusterUrl);
    registrar.Parameter("proxy_role", &TThis::ProxyRole);
    registrar.Parameter("path", &TThis::Path);
    registrar.Parameter("spec", &TThis::Spec);
    registrar.Parameter("dynamic_spec", &TThis::DynamicSpec)
        .DefaultNew();
    registrar.Parameter("abort_on_unrecognized_options", &TThis::AbortOnUnrecognizedOptions)
        .Default(false);
    registrar.Parameter("abort_on_specs_parseability_error", &TThis::AbortOnSpecsParseabilityErrors)
        .Default(false);
    registrar.Parameter("vanilla", &TThis::Vanilla)
        .Default();
    registrar.Parameter("set_flow_core_target", &TThis::SetFlowCoreTarget)
        .Default(true);
}

////////////////////////////////////////////////////////////////////////////////

TSimpleRunnerProgram::TSimpleRunnerProgram()
    : TSimpleRunnerProgram(TPrepareSpecCallback{})
{ }

TSimpleRunnerProgram::TSimpleRunnerProgram(TSimpleRunnerProgram::TPrepareSpecCallback prepareSpecCallback)
    : TProgramConfigMixin(Opts_)
    , Logger(TLogger("SimpleRunner"))
    , PrepareSpecCallback_(std::move(prepareSpecCallback))
{
    Opts_.AddLongOption("skip-set-flow-core-target",
        "One-shot override: do not set FlowCoreTarget on this invocation, "
        "overrides value of set_flow_core_target config field.")
        .NoArgument()
        .StoreValue(&SkipSetFlowCoreTarget_, true);
    Opts_.AddLongOption("validate-only",
        "Validate the pipeline spec and exit.")
        .NoArgument()
        .StoreValue(&ValidateOnly_, true);
}

void TSimpleRunnerProgram::DoRun()
{
    THROW_ERROR_EXCEPTION_IF(
        std::getenv(FlowModeEnvVarName.data()),
        "TSimpleRunnerProgram must not run with %v set; this is the Runner role",
        FlowModeEnvVarName);

    RunMixinCallbacks();
    ConfigureCrashHandler();

    TLogManager::Get()->ConfigureFromEnv();

    auto config = GetConfig();
    ConfigureSingletons(config);

    MaybeLogSlowBuild(Logger);

    if (config->AbortOnUnrecognizedOptions) {
        AbortOnUnrecognizedOptions(Logger(), config);
    } else {
        WarnForUnrecognizedOptions(Logger(), config);
    }

    {
        auto specErrors = TRegistry::Get()->ValidatePipelineSpecParseability(ConvertTo<IMapNodePtr>(config->Spec));
        auto dynamicSpecErrors = TRegistry::Get()->ValidateDynamicPipelineSpecParseability(config->Spec, ConvertTo<IMapNodePtr>(config->DynamicSpec));
        for (const auto& error : Concatenate(specErrors, dynamicSpecErrors)) {
            YT_TLOG_ERROR("Found specs parseability error")
                .With(error);
        }
        if (config->AbortOnSpecsParseabilityErrors && specErrors.size() + dynamicSpecErrors.size() > 0) {
            YT_ABORT(Format("Found specs parseability errors (ErrorCount: %v)", specErrors.size() + dynamicSpecErrors.size()));
        }
    }

    if (PrepareSpecCallback_) {
        PrepareSpecCallback_(config->Spec, config->DynamicSpec);
    }

    // Validate the full spec before any side effects (vanilla operation submission, prior-op
    // shutdown, pipeline spec update) so an invalid spec fails fast with nothing changed.
    ValidatePipelineSpec(config->Spec);
    ValidateDynamicPipelineSpec(config->DynamicSpec);

    if (ValidateOnly_) {
        YT_TLOG_INFO("Pipeline spec is valid; exiting without side effects because run with --validate-only");
        return;
    }

    if (config->Vanilla && config->Vanilla->Enable) {
        NYPath::TRichYPath pipelinePath(config->Path);
        pipelinePath.SetCluster(config->ClusterUrl);
        LaunchInVanillaJob(pipelinePath, config->ProxyRole, config->Vanilla);
    }

    bool setFlowCoreTarget = true;
    if (!config->SetFlowCoreTarget) {
        YT_TLOG_WARNING(
            "FlowCoreTarget will not be updated because set_flow_core_target=%false in runner config; "
            "the pipeline is left without zombie-process protection");
        setFlowCoreTarget = false;
    } else if (SkipSetFlowCoreTarget_) {
        YT_TLOG_WARNING(
            "FlowCoreTarget will not be updated because --skip-set-flow-core-target was passed; "
            "the pipeline is left without zombie-process protection");
        setFlowCoreTarget = false;
    }

    RunPipeline(
        config->ClusterUrl,
        config->ProxyRole,
        config->Path,
        config->Spec,
        config->DynamicSpec,
        setFlowCoreTarget);

    PrintPipelineUiUrl(config->ClusterUrl, config->Path);

    if (FromString<bool>(GetEnv("YT_FLOW_WAIT", "1"))) {
        WaitPipeline(config->ClusterUrl, config->ProxyRole, config->Path);
    }
}

////////////////////////////////////////////////////////////////////////////////

void TSimpleSpecBuilder::operator()(const TPipelineSpecPtr& spec, const TDynamicPipelineSpecPtr& /*dynamicSpec*/)
{
    for (const auto& [streamId, streamSpec] : Streams_) {
        if (spec->Streams.contains(streamId)) {
            THROW_ERROR_EXCEPTION("Spec already contains spec for stream %Qv", streamId);
        }
        spec->Streams[streamId] = streamSpec;
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
