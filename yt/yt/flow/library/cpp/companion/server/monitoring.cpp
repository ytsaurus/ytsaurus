#include "monitoring.h"

#include "private.h"

#include <yt/yt/build/build.h>

#include <yt/yt/core/http/config.h>
#include <yt/yt/core/http/server.h>

#include <yt/yt/library/profiling/solomon/config.h>
#include <yt/yt/library/profiling/solomon/exporter.h>
#include <yt/yt/library/profiling/solomon/registry.h>

#ifdef _linux_
    #include <yt/yt/library/ytprof/build_info.h>

    #include <yt/yt/library/ytprof/http/handler.h>
#endif

namespace NYT::NFlow::NCompanionServer {

constinit const auto Logger = CompanionServerLogger;

////////////////////////////////////////////////////////////////////////////////

namespace {

//! The path the worker's Solomon proxy pulls from the companion.
constexpr TStringBuf MetricsPath = "/metrics";
//! The prefix the exporter's own routes live under, mirroring the flow node.
constexpr TStringBuf SolomonPrefix = "/solomon";

NProfiling::TSolomonExporterConfigPtr BuildSolomonExporterConfig(
    const NCompanion::TCompanionExecutionConfigPtr& config)
{
    // Preserve the node exporter behavior; only companion-owned tags differ.
    NProfiling::TSolomonExporterConfigPtr exporterConfig = CloneYsonStruct(config->Monitoring);
    exporterConfig->InstanceTags[std::string(CompanionProcessTag)] = std::string(CompanionProcessTagValue);
    exporterConfig->InstanceTags["pipeline_path"] = config->PipelinePath;
    exporterConfig->InstanceTags["pipeline_cluster"] = config->ClusterUrl;
    return exporterConfig;
}

NHttp::TServerConfigPtr BuildHttpServerConfig(int port)
{
    auto config = New<NHttp::TServerConfig>();
    config->Port = port;
    config->ServerName = "CompanionMon";
    return config;
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

TCompanionMonitoring::TCompanionMonitoring(
    NCompanion::TCompanionExecutionConfigPtr config,
    NProfiling::TSolomonRegistryPtr registry)
    : Config_(std::move(config))
    , Registry_(registry ? std::move(registry) : NProfiling::TSolomonRegistry::Get())
    , SolomonExporter_(Config_->MonitoringPort == 0 || !Config_->Monitoring->Enable
            ? nullptr
            : New<NProfiling::TSolomonExporter>(
                BuildSolomonExporterConfig(Config_),
                Registry_))
{ }

void TCompanionMonitoring::Start()
{
    if (!SolomonExporter_) {
        // No exporter drains registrations; disable the registry to avoid retaining them.
        Registry_->Disable();
    }

    if (Config_->MonitoringPort == 0) {
        YT_TLOG_INFO("Companion monitoring is disabled")
            .With("MonitoringPort", Config_->MonitoringPort)
            .With("ExporterEnabled", Config_->Monitoring->Enable);
        return;
    }

    try {
        // Keep a poller thread available for metrics while a single profile is symbolized.
        HttpServer_ = NHttp::CreateServer(BuildHttpServerConfig(Config_->MonitoringPort), /*pollerThreadCount*/ 2);

        if (SolomonExporter_) {
            SolomonExporter_->Register(SolomonPrefix, HttpServer_);

            // Reuse the exporter handler at the proxy's fixed |/metrics| path.
            auto shardHandler = HttpServer_->GetPathMatcher()->Match(Format("%v/all", SolomonPrefix));
            YT_VERIFY(shardHandler);
            HttpServer_->AddHandler(std::string(MetricsPath), shardHandler);

            SolomonExporter_->Start();
        }

#ifdef _linux_
        // Profiles do not depend on the exporter, so they are served even when it is disabled.
        auto buildInfo = NYTProf::TBuildInfo::GetDefault();
        buildInfo.BinaryVersion = GetVersion();
        // The worker's profiling proxy forwards to this prefix, mirroring the flow node.
        NYTProf::Register(HttpServer_, "/ytprof", buildInfo);
#endif

        HttpServer_->Start();
    } catch (const std::exception& ex) {
        YT_TLOG_ERROR("Failed to start companion monitoring; continuing without it")
            .With(ex);
        Stop();
        SolomonExporter_.Reset();
        Registry_->Disable();
        return;
    }

    YT_TLOG_INFO("Companion monitoring server started")
        .With("Port", Config_->MonitoringPort)
        .With("ExporterEnabled", static_cast<bool>(SolomonExporter_));
}

void TCompanionMonitoring::Stop()
{
    if (HttpServer_) {
        YT_TLOG_INFO("Stopping companion monitoring server");
        HttpServer_->Stop();
        HttpServer_.Reset();
    }
    if (SolomonExporter_) {
        SolomonExporter_->Stop();
    }
}

const NProfiling::TSolomonExporterPtr& TCompanionMonitoring::GetSolomonExporter() const
{
    return SolomonExporter_;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NCompanionServer
