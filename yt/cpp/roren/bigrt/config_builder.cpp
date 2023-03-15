#include "config_builder.h"

#include <library/cpp/yt/memory/new.h>

namespace NRoren {

////////////////////////////////////////////////////////////////////////////////

    TBigRtConfigBuilderOps::TBigRtConfigBuilderOps(std::function<TBigRtProgramConfig&()> getter)
        : GetConfig_(std::move(getter))
    {
    }

    const TBigRtProgramConfig& TBigRtConfigBuilderOps::GetNativeConfig() const {
        return GetConfig_();
    }

    TBigRtConfigBuilderOps& TBigRtConfigBuilderOps::SetNativeConfig(TBigRtProgramConfig config)
    {
        GetConfig_() = std::move(config);
        return *this;
    }

    TBigRtConfigBuilderOps& TBigRtConfigBuilderOps::AddConsumingSystem(
        NBigRT::TStatelessShardProcessorConfig shardProcessorConfig,
        NBigRT::TConsumingSystemConfig consumingSystemConfig,
        TString inputTag)
    {
        TConsumerConfig config;
        *config.MutableStatelessShardProcessorConfig() = std::move(shardProcessorConfig);
        *config.MutableConsumingSystemConfig() = std::move(consumingSystemConfig);
        *config.MutableInputTag() = std::move(inputTag);
        Y_ENSURE(!IsSingleConsumer_, "SetConsumingSystem is allowed only in case of a single consumer");
        GetConfig_().MutableConsumers()->Add(std::move(config));
        return *this;
    }

    TBigRtConfigBuilderOps& TBigRtConfigBuilderOps::SetConsumingSystem(
        NBigRT::TStatelessShardProcessorConfig shardProcessorConfig,
        NBigRT::TConsumingSystemConfig consumingSystemConfig,
        TString inputTag)
    {
        TConsumerConfig config;
        *config.MutableStatelessShardProcessorConfig() = std::move(shardProcessorConfig);
        *config.MutableConsumingSystemConfig() = std::move(consumingSystemConfig);
        *config.MutableInputTag() = std::move(inputTag);
        Y_ENSURE(GetConfig_().GetConsumers().empty(), "SetConsumingSystem is allowed only in case of a single consumer");
        IsSingleConsumer_ = true;
        GetConfig_().MutableConsumers()->Add(std::move(config));
        return *this;
    }

    TBigRtConfigBuilderOps& TBigRtConfigBuilderOps::AddInput(NBigRT::TSupplierConfig supplierConfig)
    {
        NRoren::TRorenInputConfig inputConfig;
        inputConfig.SetInputTag(supplierConfig.GetAlias());
        inputConfig.MutableSuppliers()->Add(std::move(supplierConfig));
        GetConfig_().MutableInputs()->Add(std::move(inputConfig));
        return *this;
    }

    TBigRtConfigBuilderOps& TBigRtConfigBuilderOps::AddInput(
        const TVector<NBigRT::TSupplierConfig>& supplierConfigs,
        TString inputTag)
    {
        NRoren::TRorenInputConfig inputConfig;
        inputConfig.SetInputTag(std::move(inputTag));
        inputConfig.MutableSuppliers()->Add(supplierConfigs.begin(), supplierConfigs.end());
        GetConfig_().MutableInputs()->Add(std::move(inputConfig));
        return *this;
    }

    TBigRtConfigBuilderOps& TBigRtConfigBuilderOps::SetSolomonExporterConfig(NBSYeti::NProfiling::TExporterConfig config)
    {
        *GetConfig_().MutableSolomonExporterConfig() = std::move(config);
        return *this;
    }

    TBigRtConfigBuilderOps& TBigRtConfigBuilderOps::SetHttpServerConfig(NYTEx::NHttp::TServerConfig config)
    {
        *GetConfig_().MutableHttpServerConfig() = std::move(config);
        return *this;
    }

    TBigRtConfigBuilderOps& TBigRtConfigBuilderOps::SetTvmConfig(NBSYeti::TTvmGlobalConfig config)
    {
        *GetConfig_().MutableTvmConfig() = std::move(config);
        return *this;
    }

    TBigRtConfigBuilderOps& TBigRtConfigBuilderOps::SetYtClientConfig(NUserSessions::NRT::TYtClientConfig config)
    {
        *GetConfig_().MutableYtClientConfig() = std::move(config);
        return *this;
    }

    TBigRtConfigBuilderOps& TBigRtConfigBuilderOps::SetMaxInFlightBytes(ui64 maxInFlightBytes)
    {
        GetConfig_().SetMaxInFlightBytes(maxInFlightBytes);
        return *this;
    }

    TBigRtConfigBuilderOps& TBigRtConfigBuilderOps::SetLocationSolomonTag(TString locationSolomonTag)
    {
        GetConfig_().SetLocationSolomonTag(locationSolomonTag);
        return *this;
    }

    TBigRtConfigBuilderOps& TBigRtConfigBuilderOps::SetUserThreadCount(ui64 userThreadCount)
    {
        GetConfig_().SetUserThreadCount(userThreadCount);
        return *this;
    }

    TBigRtConfigBuilderOps& TBigRtConfigBuilderOps::SetEnableV2GraphParsing(bool enable)
    {
        GetConfig_().SetEnableV2GraphParsing(enable);
        return *this;
    }

////////////////////////////////////////////////////////////////////////////////

TBigRtConfigBuilder::TBigRtConfigBuilder()
    : TBigRtConfigBuilderOps([this]() -> TBigRtProgramConfig& { return this->Config_; })
{
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren
