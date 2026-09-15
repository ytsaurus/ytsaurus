#pragma once

#include "public.h"

#include <yt/yt/core/ypath/public.h>

#include <yt/yt/library/profiling/solomon/config.h>

namespace NYT::NFlow::NCompanion {

////////////////////////////////////////////////////////////////////////////////

//! Node exporter configuration passed to the companion.
struct TCompanionMonitoringConfig
    : public NProfiling::TSolomonExporterConfig
{
    REGISTER_YSON_STRUCT(TCompanionMonitoringConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TCompanionMonitoringConfig);

////////////////////////////////////////////////////////////////////////////////

//! User-facing companion configuration (TFlowNodeConfig::Companion).
//! Contains only fields a user can set in YSON config.
struct TCompanionConfig
    : public NYTree::TYsonStruct
{
    int Port{};
    int MonitoringPort{};
    int CompanionProcessCount{};

    REGISTER_YSON_STRUCT(TCompanionConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TCompanionConfig);

////////////////////////////////////////////////////////////////////////////////

//! Wire-format config: serialized into YT_FLOW_COMPANION_CONFIG
//! and stored in the companion singleton state.
struct TCompanionExecutionConfig
    : public TCompanionConfig
{
    std::string ClusterUrl;
    NYPath::TYPath PipelinePath;
    TCompanionMonitoringConfigPtr Monitoring;

    REGISTER_YSON_STRUCT(TCompanionExecutionConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TCompanionExecutionConfig);

////////////////////////////////////////////////////////////////////////////////

//! Builds a validated run config; |solomonExporterConfig| may be null.
TCompanionExecutionConfigPtr BuildCompanionExecutionConfig(
    const TCompanionConfigPtr& userConfig,
    const std::string& clusterUrl,
    const NYPath::TYPath& pipelinePath,
    const NProfiling::TSolomonExporterConfigPtr& solomonExporterConfig = nullptr);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NCompanion
