#pragma once

#include "vanilla_launcher.h"

#include <yt/yt/flow/library/cpp/common/public.h>
#include <yt/yt/flow/library/cpp/common/spec.h>
#include <yt/yt/flow/library/cpp/common/yson_message.h>

#include <yt/yt/library/program/config.h>
#include <yt/yt/library/program/helpers.h>
#include <yt/yt/library/program/program.h>
#include <yt/yt/library/program/program_config_mixin.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

struct TSimpleRunnerConfig
    : public virtual TSingletonsConfig
{
    std::string ClusterUrl;
    std::optional<std::string> ProxyRole;
    NYPath::TYPath Path;

    TPipelineSpecPtr Spec;
    TDynamicPipelineSpecPtr DynamicSpec;

    bool AbortOnUnrecognizedOptions{};
    bool AbortOnSpecsParseabilityErrors{};
    bool SetFlowCoreTarget{};

    //! Optional block that, when present and `enable=%true`, makes the runner submit
    //! a YT vanilla operation hosting the controller/worker federation for this pipeline.
    TVanillaConfigPtr Vanilla;

    REGISTER_YSON_STRUCT(TSimpleRunnerConfig);

    static void Register(TRegistrar registrar);
};

using TSimpleRunnerConfigPtr = TIntrusivePtr<TSimpleRunnerConfig>;

////////////////////////////////////////////////////////////////////////////////

//! Implements the *Runner* role of `flow_server`: one-shot CLI that submits
//  the pipeline spec to YT and optionally launches the vanilla operation.
class TSimpleRunnerProgram
    : public virtual TProgram
    , public TProgramConfigMixin<TSimpleRunnerConfig>
{
public:
    using TPrepareSpecCallback = std::function<void(const TPipelineSpecPtr& spec, const TDynamicPipelineSpecPtr& dynamicSpec)>;

    TSimpleRunnerProgram();
    explicit TSimpleRunnerProgram(TPrepareSpecCallback prepareSpecCallback);

protected:
    NLogging::TLogger Logger;

    void DoRun() override;

private:
    TPrepareSpecCallback PrepareSpecCallback_;

    //! One-shot CLI override that skips the `set-flow-core-target` RPC for
    //! this invocation only.
    bool SkipSetFlowCoreTarget_ = false;

    //! CLI flag: validate the spec and exit.
    bool ValidateOnly_ = false;
};

////////////////////////////////////////////////////////////////////////////////

class TSimpleSpecBuilder
{
public:
    TSimpleSpecBuilder() = default;

    template <CYsonMessage T>
    void RegisterStream(const TStreamId& streamId, const std::optional<std::string>& migrationFunction = std::nullopt)
    {
        auto spec = New<TStreamSpec>();
        spec->ClassName = TypeName<T>();
        if (migrationFunction) {
            spec->MigrationFunction = *migrationFunction;
        }
        spec->Schema = GetYsonMessagePayloadSchema<T>();

        EmplaceOrCrash(
            Streams_,
            streamId,
            spec);
    }

    void operator()(const TPipelineSpecPtr& spec, const TDynamicPipelineSpecPtr& dynamicSpec);

private:
    THashMap<TStreamId, TStreamSpecPtr> Streams_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
