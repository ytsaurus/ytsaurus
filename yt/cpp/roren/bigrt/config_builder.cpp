#include "config_builder.h"

#include <library/cpp/yt/memory/new.h>
#include <library/cpp/yt/string/format.h>

namespace NRoren {

////////////////////////////////////////////////////////////////////////////////

    TBigRtConfigBuilderOps::TBigRtConfigBuilderOps(std::function<TBigRtProgramProtoConfig&()> getter)
        : GetConfig_(std::move(getter))
    {
    }

    const TBigRtProgramProtoConfig& TBigRtConfigBuilderOps::GetNativeConfig() const {
        return GetConfig_();
    }

    TBigRtConfigBuilderOps& TBigRtConfigBuilderOps::SetNativeConfig(TBigRtProgramProtoConfig config)
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
        Y_ENSURE(!IsSingleConsumer_, "SetConsumingSystem is not allowed in case of a single consumer");
        Y_ENSURE(!GetConfig_().MutableConsumers()->contains(inputTag), NYT::Format("Conflicting consuming systems with tag %v", inputTag));
        (*GetConfig_().MutableConsumers())[inputTag] = std::move(config);
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
        Y_ENSURE(GetConfig_().GetConsumers().empty(), "SetConsumingSystem is allowed only in case of a single consumer");
        IsSingleConsumer_ = true;
        (*GetConfig_().MutableConsumers())[inputTag] = std::move(config);
        return *this;
    }

    TBigRtConfigBuilderOps& TBigRtConfigBuilderOps::AddInput(NBigRT::TSupplierConfig supplierConfig)
    {
        NRoren::TRorenInputConfig inputConfig;
        auto inputTag = supplierConfig.GetAlias();
        inputConfig.MutableSuppliers()->Add(std::move(supplierConfig));
        Y_ENSURE(!GetConfig_().MutableInputs()->contains(inputTag), NYT::Format("Conflicting inputs with tag %v", inputTag));
        (*GetConfig_().MutableInputs())[inputTag] = std::move(inputConfig);
        return *this;
    }

    TBigRtConfigBuilderOps& TBigRtConfigBuilderOps::AddInput(
        const TVector<NBigRT::TSupplierConfig>& supplierConfigs,
        TString inputTag)
    {
        NRoren::TRorenInputConfig inputConfig;
        inputConfig.MutableSuppliers()->Add(supplierConfigs.begin(), supplierConfigs.end());
        Y_ENSURE(!GetConfig_().MutableInputs()->contains(inputTag), NYT::Format("Conflicting inputs with tag %v", inputTag));
        (*GetConfig_().MutableInputs())[inputTag] = std::move(inputConfig);
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

    TBigRtConfigBuilderOps& TBigRtConfigBuilderOps::SetUserThreadCount(ui64 userThreadCount)
    {
        GetConfig_().SetUserThreadCount(userThreadCount);
        return *this;
    }

    TBigRtConfigBuilderOps& TBigRtConfigBuilderOps::SetEnableDirectSolomonExporterHandler(bool value)
    {
        GetConfig_().SetEnableDirectSolomonExporterHandler(value);
        return *this;
    }

////////////////////////////////////////////////////////////////////////////////

TBigRtConfigBuilder::TBigRtConfigBuilder()
    : TBigRtConfigBuilderOps([this]() -> TBigRtProgramProtoConfig& { return this->Config_; })
{
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren
