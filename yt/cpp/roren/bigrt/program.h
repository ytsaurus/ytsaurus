#pragma once

#include "config_builder.h"
#include "fwd.h"

#include <yt/cpp/roren/interface/roren.h>
#include <yt/cpp/roren/library/program/program.h>

#include <yt/yt/core/http/public.h>
#include <yt/yt/core/concurrency/public.h>

#include <library/cpp/yt/string/format_extensions/all.h>

namespace NRoren {

class TPipeline;

////////////////////////////////////////////////////////////////////////////////

template <class TConfig>
concept TStandardYsonProgramConfig = std::derived_from<TConfig, TBigRtProgramConfig>;
template <class TConfig>
concept TStandardProgramConfig = requires(TConfig config) {
    {config.GetRorenConfig()} -> std::same_as<const TBigRtProgramProtoConfig&>;
};
template <class TConfig>
concept TNonStandardProgramConfig = !TStandardYsonProgramConfig<TConfig> && !TStandardProgramConfig<TConfig>;

////////////////////////////////////////////////////////////////////////////////

namespace NPrivate {

class TBigRtProgramBaseInternal
    : public TProgram
    , public TBigRtConfigBuilderOps
{
public:

    explicit TBigRtProgramBaseInternal(NYT::TCancelableContextPtr cancelableContext, std::function<TBigRtProgramProtoConfig&()> configGetter);
    TBigRtProgramBaseInternal(TBigRtProgramBaseInternal&&) = default;

    virtual ~TBigRtProgramBaseInternal() = default;

    NYT::NHttp::IRequestPathMatcherPtr GetHttpHandlers();

    virtual void Prepare() {}
    virtual void Start() {}
    virtual void Finish() {}

    void Run(std::function<void(TPipeline&)> pipelineConstructor);
    void Run(std::function<void(TPipeline&, const TBigRtProgramProtoConfig&)> pipelineConstructor);

    TBigRtProgramBaseInternal& SetProfiler(NYT::NProfiling::TProfiler profiler);
    TBigRtProgramBaseInternal& SetProfilerTags(NYT::NProfiling::TTagSet tagSet);
    TBigRtProgramBaseInternal& SetConsumingSystemTagFactory(std::function<std::pair<TString, TString>(const TString&)> factory);

    virtual const google::protobuf::Message& GetUserConfig() const = 0;
    virtual const google::protobuf::Message& GetOriginalConfig() const = 0;

    const NYT::NConcurrency::ITwoLevelFairShareThreadPoolPtr& GetThreadPool();

    static NYT::NConcurrency::ITwoLevelFairShareThreadPoolPtr CreateThreadPool(const TBigRtProgramProtoConfig& config);

    static void NormalizeInputs(TBigRtProgramProtoConfig& config);
    static void NormalizeMainYtParams(TBigRtProgramProtoConfig& config);
    static void NormalizeTvmConfig(TBigRtProgramProtoConfig& config);
    static void NormalizeDestinations(TBigRtProgramProtoConfig& config, NYT::TCancelableContextPtr cancelableContext);
    static void NormalizeConfigBase(TBigRtProgramProtoConfig& config, NYT::TCancelableContextPtr cancelableContext);

protected:
    struct TInternal;
    template <class TPipelineConstructorFunc, class... TUserConfigType>
    void InternalInvokePipelineConstructor(TPipelineConstructorFunc&& pipelineConstructor, TPipeline& pipeline, const TUserConfigType&... userConfig);

    virtual void NormalizeConfig(TBigRtProgramProtoConfig& config);

    std::shared_ptr<TInternal> Internal_;
}; // class TBigRtProgramBaseInternal

////////////////////////////////////////////////////////////////////////////////

template <class TPipelineConstructorFunc, class... TUserConfigType>
void TBigRtProgramBaseInternal::InternalInvokePipelineConstructor(TPipelineConstructorFunc&& pipelineConstructor, TPipeline& pipeline, const TUserConfigType&... userConfig)
{
    if constexpr (std::invocable<TPipelineConstructorFunc, TPipeline&, const TBigRtProgramProtoConfig&>) {
        pipelineConstructor(pipeline, GetNativeConfig());
    } else if constexpr (std::invocable<TPipelineConstructorFunc, TPipeline&, const TUserConfigType&...>) {
        pipelineConstructor(pipeline, userConfig...);
    } else if constexpr (std::invocable<TPipelineConstructorFunc, TPipeline&, const TBigRtProgramProtoConfig&, const TUserConfigType&...>) {
        pipelineConstructor(pipeline, GetNativeConfig(), userConfig...);
    } else {
        pipelineConstructor(pipeline);
    }
}

////////////////////////////////////////////////////////////////////////////////

template <class TConfig>
class TBigRtProgramWithConfig;

template <TStandardProgramConfig TConfig>
class TBigRtProgramWithConfig<TConfig>
    : public TBigRtProgramBaseInternal
{
public:
    using TBigRtProgramBaseInternal::Run;

    TBigRtProgramWithConfig(TBigRtProgramWithConfig&&) = default;

    const TConfig& GetConfig() const
    {
        return Config_;
    }

    const TConfig& GetOriginalConfig() const override
    {
        return OriginalConfig_;
    }

    void SetConfig(TConfig config)
    {
        OriginalConfig_ = config;
        NormalizeConfig(*config.MutableRorenConfig());
        Config_ = std::move(config);
    }

    const google::protobuf::Message& GetUserConfig() const override
    {
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

    TBigRtProgramWithConfig(NYT::TCancelableContextPtr cancelableContext = {})
        : TBigRtProgramBaseInternal(cancelableContext, [this]() -> TBigRtProgramProtoConfig& { return *(this->Config_.MutableRorenConfig()); })
    {
    }

    TConfig OriginalConfig_;
    TConfig Config_;
}; // class TBigRtProgramWithConfig<TStandardProgramConfig>

////////////////////////////////////////////////////////////////////////////////

template <TNonStandardProgramConfig TConfig>
class TBigRtProgramWithConfig<TConfig>
    : public TBigRtProgramBaseInternal
{
public:
    using TBigRtProgramBaseInternal::Run;

    TBigRtProgramWithConfig(TBigRtProgramWithConfig&&) = default;

    const TConfig& GetConfig() const
    {
        return UserConfig_;
    }

    void SetConfig(TConfig config)
    {
        UserConfig_ = std::move(config);
    }

    const TConfig& GetOriginalConfig() const override
    {
        return GetConfig();
    }

    const google::protobuf::Message& GetUserConfig() const override
    {
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

    void Run(std::function<void(TPipeline&, const TBigRtProgramProtoConfig&, const TConfig&)> pipelineConstructor)
    {
        Run(
            [this, pipelineConstructor=std::move(pipelineConstructor)] (TPipeline& pipeline) -> void {
                InternalInvokePipelineConstructor(pipelineConstructor, pipeline, GetConfig());
            }
        );
    }

protected:

    TBigRtProgramWithConfig(NYT::TCancelableContextPtr cancelableContext = {})
        : TBigRtProgramBaseInternal(cancelableContext, [this]() -> TBigRtProgramProtoConfig& { return this->NativeConfig_; })
    {
    }

    TBigRtProgramProtoConfig NativeConfig_;
    TConfig UserConfig_;
}; // class TBigRtProgramWithConfig<TNonStandardProgramConfig>


} // namespace NPrivate

////////////////////////////////////////////////////////////////////////////////

template <typename TConfig, typename TDerived>
class TBigRtProgramBase;

template <TStandardProgramConfig TConfig, typename TDerived>
class TBigRtProgramBase<TConfig, TDerived> : public NPrivate::TBigRtProgramWithConfig<TConfig>
{
public:
    TBigRtProgramBase(TBigRtProgramBase&&) = default;

    static TDerived Create(int argc, const char* argv[], NYT::TCancelableContextPtr cancelableContext = {})
    {
        auto config = NRoren::TProgram::LoadConfig<TConfig>(argc, argv);
        if (!config.HasRorenConfig()) {
            throw std::runtime_error("RorenConfig is required and must be set in config file.");
        }
        return CreateWithConfig(std::move(config), std::move(cancelableContext));
    }

    static TDerived CreateWithConfig(TConfig config, NYT::TCancelableContextPtr cancelableContext = {})
    {
        auto program = CreateWithDefaultConfig(std::move(cancelableContext));
        program.SetConfig(std::move(config));
        return program;
    }

    static TDerived CreateWithDefaultConfig(NYT::TCancelableContextPtr cancelableContext = {})
    {
        return TDerived(std::move(cancelableContext));
    }

protected:
    using NPrivate::TBigRtProgramWithConfig<TConfig>::TBigRtProgramWithConfig;

}; // class TBigRtProgramBase<TStandardProgramConfig>

////////////////////////////////////////////////////////////////////////////////

template <TNonStandardProgramConfig TConfig, typename TDerived>
class TBigRtProgramBase<TConfig, TDerived> : public NPrivate::TBigRtProgramWithConfig<TConfig>
{
public:
    TBigRtProgramBase(TBigRtProgramBase&&) = default;

    static TDerived CreateWithDefaultConfig(NYT::TCancelableContextPtr cancelableContext = {})
    {
        return TDerived(std::move(cancelableContext));
    }

    static TDerived CreateWithCustomConfig(TConfig config, NYT::TCancelableContextPtr cancelableContext = {})
    {
        auto program = CreateWithDefaultConfig(std::move(cancelableContext));
        program.SetConfig(std::move(config));
        return program;
    }

    static TDerived CreateLoadCustomConfig(int argc, const char* argv[], NYT::TCancelableContextPtr cancelableContext = {})
    {
        auto config = NRoren::TProgram::LoadConfig<TConfig>(argc, argv);
        return CreateWithCustomConfig(std::move(config), std::move(cancelableContext));
    }

protected:
    using NPrivate::TBigRtProgramWithConfig<TConfig>::TBigRtProgramWithConfig;

}; // class TBigRtProgramBase<TNonStandardProgramConfig>

////////////////////////////////////////////////////////////////////////////////

template <typename TConfig>
class TBigRtProgram : public TBigRtProgramBase<TConfig, TBigRtProgram<TConfig>>
{
public:
    friend class TBigRtProgramBase<TConfig, TBigRtProgram<TConfig>>;

    TBigRtProgram(TBigRtProgram&&) = default;

protected:
    using TBigRtProgramBase<TConfig, TBigRtProgram<TConfig>>::TBigRtProgramBase;

}; // class TBigRtProgram

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren
