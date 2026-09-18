#pragma once

#include <yt/yt/core/ytree/public.h>
#include <yt/yt/core/ytree/yson_struct.h>
#include <yt/yt/flow/library/cpp/common/spec.h>
#include <yt/yt/flow/library/cpp/runner/config.h>

#include <optional>
#include <string>
#include <vector>


namespace NYql::NYtflow {

DECLARE_REFCOUNTED_STRUCT(TLocalFileConfig);

struct TLocalFileConfig
    : public virtual NYT::NYTree::TYsonStruct
{
    std::string Name;
    std::string Path;

    REGISTER_YSON_STRUCT(TLocalFileConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TLocalFileConfig);

DECLARE_REFCOUNTED_STRUCT(TWorkerConfig);

struct TWorkerConfig
    : public virtual NYT::NFlow::TFlowNodeConfig
{
    NYT::NFlow::TPipelineSpecPtr PipelineSpec;
    NYT::NFlow::TDynamicPipelineSpecPtr DynamicPipelineSpec;

    bool GracefulUpdate;
    bool RunVanilla;
    bool SetFlowCoreTarget;

    uint64_t ControllerCount;
    double ControllerCpuLimit;
    uint64_t ControllerMemoryLimit;
    uint64_t ControllerRpcPort;
    uint64_t ControllerMonitoringPort;
    NYT::NLogging::TLogManagerConfigPtr ControllerLogManagerConfig;

    uint64_t WorkerCount;
    double WorkerCpuLimit;
    uint64_t WorkerMemoryLimit;
    uint64_t WorkerRpcPort;
    uint64_t WorkerMonitoringPort;
    NYT::NLogging::TLogManagerConfigPtr WorkerLogManagerConfig;

    std::vector<TLocalFileConfigPtr> LocalFiles;

    std::optional<std::string> Pool;
    std::optional<std::string> RuntimeCluster;

    std::optional<std::string> NetworkProject;
    std::optional<std::string> SolomonResolverTag;

    NYT::NYTree::IMapNodePtr Description;
    std::optional<std::string> Title;

    REGISTER_YSON_STRUCT(TWorkerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TWorkerConfig);

} // namespace NYql::NYtflow
