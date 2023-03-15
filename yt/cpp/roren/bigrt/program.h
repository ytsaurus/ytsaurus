#pragma once

#include "config_builder.h"

#include <yt/cpp/roren/interface/roren.h>
#include <yt/cpp/roren/library/program/program.h>

#include <yt/yt/core/http/public.h>

namespace NRoren {

class TPipeline;

////////////////////////////////////////////////////////////////////////////////

template <class TConfig>
concept TStandardProgramConfig = requires(TConfig config) {
    {config.GetRorenConfig()} -> std::same_as<const TBigRtProgramConfig&>;
};
template <class TConfig>
concept TNonStandardProgramConfig = !TStandardProgramConfig<TConfig>;

class TBigRtProgramBase
    : public TProgram
    , public TBigRtConfigBuilderOps
{
public:

    explicit TBigRtProgramBase(NYT::TCancelableContextPtr cancelableContext, std::function<TBigRtProgramConfig&()> configGetter);

    NYT::NHttp::IRequestPathMatcherPtr GetHttpHandlers();

    void Run(std::function<void(TPipeline&)> pipelineConstructor);
    void Run(std::function<void(TPipeline&, const TBigRtProgramConfig&)> pipelineConstructor);

    TBigRtProgramBase& SetProfiler(NYT::NProfiling::TProfiler profiler);
    TBigRtProgramBase& SetProfilerTags(NYT::NProfiling::TTagSet tagSet);

    virtual const google::protobuf::Message& GetUserConfig() const = 0;

protected:
    struct TInternal;
    template <class TPipelineConstructorFunc, class... TUserConfigType>
    void InternalInvokePipelineConstructor(TPipelineConstructorFunc&& pipelineConstructor, TPipeline& pipeline, const TUserConfigType&... userConfig);
    void Run(const TPipeline& pipeline);

    std::shared_ptr<TInternal> Internal_;
}; // class TBigRtProgramBase

////////////////////////////////////////////////////////////////////////////////

template <class TPipelineConstructorFunc, class... TUserConfigType>
void TBigRtProgramBase::InternalInvokePipelineConstructor(TPipelineConstructorFunc&& pipelineConstructor, TPipeline& pipeline, const TUserConfigType&... userConfig)
{
    if constexpr (std::is_invocable_v<TPipelineConstructorFunc, TPipeline&, const TBigRtProgramConfig&>) {
        pipelineConstructor(pipeline, GetNativeConfig());
    } else if constexpr (std::is_invocable_v<TPipelineConstructorFunc, TPipeline&, const TUserConfigType&...>) {
        pipelineConstructor(pipeline, userConfig...);
    } else if constexpr (std::is_invocable_v<TPipelineConstructorFunc, TPipeline&, const TBigRtProgramConfig&, const TUserConfigType&...>) {
        pipelineConstructor(pipeline, GetNativeConfig(), userConfig...);
    } else {
        pipelineConstructor(pipeline);
    }
}

////////////////////////////////////////////////////////////////////////////////

template <class TConfig>
class TBigRtProgram;

template <TStandardProgramConfig TConfig>
class TBigRtProgram<TConfig>
    : public TBigRtProgramBase
{
public:
    using TBigRtProgramBase::Run;

    static TBigRtProgram<TConfig> Create(int argc, const char* argv[], NYT::TCancelableContextPtr cancelableContext = {})
    {
        auto config = NRoren::TProgram::LoadConfig<TConfig>(argc, argv);
        if (!config.HasRorenConfig()) {
            throw std::runtime_error("RorenConfig is required and must be set in config file.");
        }
        return CreateWithConfig(std::move(config), std::move(cancelableContext));
    }

    static TBigRtProgram<TConfig> CreateWithConfig(TConfig config, NYT::TCancelableContextPtr cancelableContext = {})
    {
        auto program = CreateWithDefaultConfig(std::move(cancelableContext));
        program.SetConfig(std::move(config));
        return program;
    }

    static TBigRtProgram<TConfig> CreateWithDefaultConfig(NYT::TCancelableContextPtr cancelableContext = {})
    {
        return TBigRtProgram<TConfig>(std::move(cancelableContext));
    }

    const TConfig& GetConfig() const
    {
        return Config_;
    }

    void SetConfig(TConfig config)
    {
        Config_ = std::move(config);
    }

    const google::protobuf::Message& GetUserConfig() const override {
        return GetConfig();
    }

    void Run(std::function<void(TPipeline&, const TConfig&)> pipelineConstructor)
    {
        Run(
            [this, pipelineConstructor=std::move(pipelineConstructor)] (TPipeline& pipeline) -> void {
                InternalInvokePipelineConstructor(pipelineConstructor, pipeline, GetConfig());
            }
        );
    }

protected:

    TBigRtProgram(NYT::TCancelableContextPtr cancelableContext = {})
        : TBigRtProgramBase(cancelableContext, [this]() -> TBigRtProgramConfig& { return *(this->Config_.MutableRorenConfig()); })
    {
    }

    TConfig Config_;
}; // class TBigRtProgram<TStandardProgramConfig>

////////////////////////////////////////////////////////////////////////////////

template <TNonStandardProgramConfig TConfig>
class TBigRtProgram<TConfig>
    : public TBigRtProgramBase
{
public:
    using TBigRtProgramBase::Run;

    static TBigRtProgram<TConfig> CreateWithDefaultConfig(NYT::TCancelableContextPtr cancelableContext = {})
    {
        return TBigRtProgram<TConfig>(std::move(cancelableContext));
    }

    static TBigRtProgram<TConfig> CreateWithCustomConfig(TConfig config, NYT::TCancelableContextPtr cancelableContext = {})
    {
        auto program = CreateWithDefaultConfig(std::move(cancelableContext));
        program.SetConfig(std::move(config));
        return program;
    }

    static TBigRtProgram<TConfig> CreateLoadCustomConfig(int argc, const char* argv[], NYT::TCancelableContextPtr cancelableContext = {})
    {
        auto config = NRoren::TProgram::LoadConfig<TConfig>(argc, argv);
        return CreateWithCustomConfig(std::move(config), std::move(cancelableContext));
    }

    const TConfig& GetConfig() const
    {
        return UserConfig_;
    }

    void SetConfig(TConfig config)
    {
        UserConfig_ = std::move(config);
    }

    const google::protobuf::Message& GetUserConfig() const override {
        return GetConfig();
    }

    void Run(std::function<void(TPipeline&, const TConfig&)> pipelineConstructor)
    {
        Run(
            [this, pipelineConstructor=std::move(pipelineConstructor)] (TPipeline& pipeline) -> void {
                InternalInvokePipelineConstructor(pipelineConstructor, pipeline, GetConfig());
            }
        );
    }

    void Run(std::function<void(TPipeline&, const TBigRtProgramConfig&, const TConfig&)> pipelineConstructor)
    {
        Run(
            [this, pipelineConstructor=std::move(pipelineConstructor)] (TPipeline& pipeline) -> void {
                InternalInvokePipelineConstructor(pipelineConstructor, pipeline, GetConfig());
            }
        );
    }

protected:

    TBigRtProgram(NYT::TCancelableContextPtr cancelableContext = {})
        : TBigRtProgramBase(cancelableContext, [this]() -> TBigRtProgramConfig& { return this->NativeConfig_; })
    {
    }

    TBigRtProgramConfig NativeConfig_;
    TConfig UserConfig_;
}; // class TBigRtProgram<TNonStandardProgramConfig>

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren
