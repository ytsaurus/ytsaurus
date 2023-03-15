#pragma once

#include <yt/cpp/roren/bigrt/proto/config.pb.h>
#include <library/cpp/yt/memory/ref_counted.h>
#include <util/generic/vector.h>

namespace NRoren {

////////////////////////////////////////////////////////////////////////////////

class TBigRtConfigBuilderOps {
public:
    TBigRtConfigBuilderOps(std::function<TBigRtProgramConfig&()> getter);

    const TBigRtProgramConfig& GetNativeConfig() const;

    /// Set all configuration options at once.
    TBigRtConfigBuilderOps& SetNativeConfig(TBigRtProgramConfig config);

    /// Add ConsumingSystem to the config.
    TBigRtConfigBuilderOps& AddConsumingSystem(
        NBigRT::TStatelessShardProcessorConfig shardProcessorConfig,
        NBigRT::TConsumingSystemConfig consumingSystemConfig,
        TString inputTag);

    /// Set config of a single consumer.
    TBigRtConfigBuilderOps& SetConsumingSystem(
        NBigRT::TStatelessShardProcessorConfig shardProcessorConfig,
        NBigRT::TConsumingSystemConfig consumingSystemConfig,
        TString inputTag = "input");

    /// Add Input to the config, inputTag = supplierConfig.GetAlias()
    TBigRtConfigBuilderOps& AddInput(NBigRT::TSupplierConfig supplierConfig);

    /// Add Input to the config
    TBigRtConfigBuilderOps& AddInput(
        const TVector<NBigRT::TSupplierConfig>& supplierConfigs,
        TString inputTag);

    /// Set `SolomonExporterConfig` part of the config.
    TBigRtConfigBuilderOps& SetSolomonExporterConfig(NBSYeti::NProfiling::TExporterConfig config);
    /// Set `HttpServerConfig` part of the config.
    TBigRtConfigBuilderOps& SetHttpServerConfig(NYTEx::NHttp::TServerConfig config);
    /// Set `TvmConfig` part of the config.
    TBigRtConfigBuilderOps& SetTvmConfig(NBSYeti::TTvmGlobalConfig config);
    /// Set `YtClientConfig` part of the config.
    TBigRtConfigBuilderOps& SetYtClientConfig(NUserSessions::NRT::TYtClientConfig config);
    /// Set `MaxInFlightBytes` value of config.
    TBigRtConfigBuilderOps& SetMaxInFlightBytes(ui64 maxInFlightBytes);
    /// Set `Location` value of config.
    TBigRtConfigBuilderOps& SetLocationSolomonTag(TString locationSolomonTag);
    /// Set `UserThread` value of config.
    TBigRtConfigBuilderOps& SetUserThreadCount(ui64 userThreadCount);

    /// Enable V2 version of graph parsing (temporary option).
    ///
    /// V2 version supports stateful processing and switching concurrency.
    /// In future default parser will be switched to V2, and this version removed.
    TBigRtConfigBuilderOps& SetEnableV2GraphParsing(bool enable);

protected:
    bool IsSingleConsumer_ = false;
    std::function<TBigRtProgramConfig&()> GetConfig_;
};  // class TBigRtConfigBuilderOps

////////////////////////////////////////////////////////////////////////////////

class TBigRtConfigBuilder
    : public TBigRtConfigBuilderOps
{
public:
    TBigRtConfigBuilder();

protected:
    TBigRtProgramConfig Config_;
};  // class TBigRtConfigBuilder

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren
