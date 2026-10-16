#pragma once

#include "public.h"

#include <yt/yt/flow/library/cpp/companion/client/config.h>

#include <yt/yt/core/http/public.h>

#include <yt/yt/library/profiling/solomon/public.h>

namespace NYT::NFlow::NCompanionServer {

////////////////////////////////////////////////////////////////////////////////

//! Reserved origin tag for companion sensors in merged exports.
constexpr TStringBuf CompanionProcessTag = "flow_process";
constexpr TStringBuf CompanionProcessTagValue = "companion";

////////////////////////////////////////////////////////////////////////////////

//! Owns the companion's |/metrics| endpoint and Solomon exporter.
class TCompanionMonitoring
    : public TRefCounted
{
public:
    //! |registry| defaults to the process-wide sensor registry.
    explicit TCompanionMonitoring(
        NCompanion::TCompanionExecutionConfigPtr config,
        NProfiling::TSolomonRegistryPtr registry = nullptr);

    void Start();
    void Stop();

    //! The exporter serving |/metrics|; null when monitoring is disabled.
    const NProfiling::TSolomonExporterPtr& GetSolomonExporter() const;

private:
    const NCompanion::TCompanionExecutionConfigPtr Config_;
    const NProfiling::TSolomonRegistryPtr Registry_;
    NProfiling::TSolomonExporterPtr SolomonExporter_;

    NHttp::IServerPtr HttpServer_;
};

DEFINE_REFCOUNTED_TYPE(TCompanionMonitoring);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NCompanionServer
