#pragma once

#include "public.h"

#include <yt/yt/core/ypath/public.h>

namespace NYT::NFlow::NCompanion {

////////////////////////////////////////////////////////////////////////////////

//! User-facing companion configuration (TFlowNodeConfig::Companion).
//! Contains only fields a user can set in YSON config.
struct TCompanionConfig
    : public NYTree::TYsonStruct
{
    int Port{};
    int MonitoringPort{};
    // Per-job TTL in seconds. Stored as a plain integer (not TDuration)
    // for matching the Java/Python sides.
    i64 JobTtlSeconds{};
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

    REGISTER_YSON_STRUCT(TCompanionExecutionConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TCompanionExecutionConfig);

////////////////////////////////////////////////////////////////////////////////

//! Builds a fully-validated run config from a user config and runtime values.
TCompanionExecutionConfigPtr BuildCompanionExecutionConfig(
    const TCompanionConfigPtr& userConfig,
    const std::string& clusterUrl,
    const NYPath::TYPath& pipelinePath);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NCompanion
