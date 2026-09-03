#pragma once

#include <yt/yt/flow/library/cpp/runner/public.h>

#include <yt/yt/client/cache/public.h>

#include <yt/yt/client/ypath/rich.h>

#include <yt/yt/core/ytree/size.h>
#include <yt/yt/core/ytree/yson_struct.h>

#include <util/datetime/base.h>
#include <util/generic/hash.h>

#include <optional>
#include <string>
#include <vector>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

//! The scheduler cap. Its own default of ten is too few: a job's stderr is the only artifact that
//! survives the operation, so it is worth retaining for every job.
constexpr int DefaultMaxStderrCount = 150;

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TVanillaTaskConfig)

//! Per-task (controller or worker) sizing in a Flow vanilla launch.
struct TVanillaTaskConfig
    : public NYTree::TYsonStruct
{
    int Count{};
    //! Byte size; accepts human-readable forms like "12g" (NYTree::TSize).
    std::optional<NYTree::TSize> MemoryLimit;
    std::optional<int> CpuLimit;

    //! When positive, the task requests this many YT-allocated ports (exposed as YT_PORT_<i>),
    //! overriding the fixed ports from the node config. Needed on a shared-network host
    //! (e.g. local tests) where fixed ports of co-located jobs would collide, so when the
    //! launch has no network project it defaults to 2 (controller) / 3 (worker); an explicit
    //! 0 keeps the fixed ports.
    std::optional<int> PortCount;

    //! Extra files delivered into the task sandbox: sandbox file name -> path.
    //! `LocalFiles` are uploaded from the launching host; `CypressFiles` are referenced by their
    //! Cypress path. A language launcher (e.g. python) fills `LocalFiles` to ship its companion.
    THashMap<std::string, std::string> LocalFiles;
    THashMap<std::string, std::string> CypressFiles;

    //! Cypress paths of porto layers mounted into the task's root filesystem.
    std::vector<std::string> Layers;

    //! Per-task base OS layer; overrides the default system layer when set.
    std::optional<std::string> SystemLayerPath;

    //! Root filesystem image for the task, on clusters whose job environment pulls images rather
    //! than mounting porto layers. Mutually exclusive with `Layers`.
    std::optional<std::string> DockerImage;

    REGISTER_YSON_STRUCT(TVanillaTaskConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TVanillaTaskConfig)

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TVanillaConfig)

//! Declarative config for launching a Flow federation inside a YT vanilla
//! operation. Embedded into the runner config as the "vanilla" block.
//!
//! Required: Worker.Count, Pool.
//! Everything else has sensible defaults.
struct TVanillaConfig
    : public NYTree::TYsonStruct
{
    bool Enable{};

    std::string Pool;

    TVanillaTaskConfigPtr Controller;
    TVanillaTaskConfigPtr Worker;

    std::optional<std::string> RuntimeCluster;
    //! RPC proxy role for the runtime cluster (the pipeline cluster's role may not exist there).
    std::optional<std::string> RuntimeProxyRole;

    //! Content-addressed cache the job files are uploaded to (shared across all flow operations on a
    //! cluster). The durable per-pipeline copy under the pipeline node is a cheap CopyNode from here.
    NYPath::TYPath CachePath;

    ui64 MaxFailedJobCount{};
    int MaxStderrCount{};
    TDuration WaitTimeout;
    std::string SolomonResolverTag;

    std::optional<std::string> Alias;
    std::optional<std::string> Title;
    //! Defaults to `yt_flow_common` in internal builds. Set to an entity (`#`) to disable it.
    std::optional<std::string> NetworkProject;

    THashMap<std::string, std::string> ProxyUrlAliasingRules;

    //! Names of environment variables to forward from the launcher's environment into the
    //! operation's secure vault. Values are read from the env at launch and never stored; inside
    //! the job they are re-exported as plain env vars (e.g. TVM_SECRET for Logbroker/TVM access).
    //! YT_TOKEN need not be listed here — it is always delivered by a separate built-in mechanism.
    std::vector<std::string> SecretEnv;

    //! Optional patch merged on top of the auto-built TFlowNodeConfig.
    //! Useful for tests and edge cases where the default config needs tweaks.
    NYTree::INodePtr NodeConfigPatch;

    REGISTER_YSON_STRUCT(TVanillaConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TVanillaConfig)

////////////////////////////////////////////////////////////////////////////////

//! Builds a TFlowNodeConfig with defaults appropriate for ephemeral
//! vanilla jobs: stderr logging, OS-picked ports, lenient option parsing.
//! |workerPortCount| is the worker task's `port_count`.
TFlowNodeConfigPtr BuildDefaultVanillaNodeConfig(
    const NYPath::TRichYPath& pipelinePath,
    std::optional<std::string> proxyRole,
    std::optional<int> workerPortCount);

////////////////////////////////////////////////////////////////////////////////

//! Submits a YT vanilla operation that runs a Flow federation for the given pipeline.
//! |pipelinePath| must carry the cluster annotation (`<cluster=...>/path/to/pipeline`).
//! |clientsCache| supplies the clients for the pipeline, runtime and prior-operation clusters.
//! Called by TSimpleRunnerProgram when the runner config contains a "vanilla" block.
void LaunchInVanillaJob(
    const NYPath::TRichYPath& pipelinePath,
    const std::optional<std::string>& proxyRole,
    const TVanillaConfigPtr& vanilla,
    const NClient::NCache::IClientsCachePtr& clientsCache);

////////////////////////////////////////////////////////////////////////////////

//! Per-task limits, files and environment for an externally launched flow vanilla operation.
struct TFlowVanillaTask
{
    int JobCount = 0;
    i64 MemoryLimit = 0;
    double CpuLimit = 0;
    //! When > 0, request that many YT-allocated ports (exposed as YT_PORT_<i>).
    int PortCount = 0;
    //! Extra environment beyond YT_FLOW_MODE (e.g. a YT_FLOW_CONFIG override).
    THashMap<std::string, std::string> Environment;
    //! Files uploaded from the launching host: in-job name -> local path.
    THashMap<std::string, std::string> LocalFiles;
    //! Files referenced by Cypress path: in-job name -> Cypress path.
    THashMap<std::string, std::string> CypressFiles;
    std::vector<std::string> Layers;
    std::optional<std::string> SystemLayerPath;
    std::optional<std::string> DockerImage;
};

//! Options for launching a Flow federation as a vanilla operation directly, without the pipeline
//! manifest/reanimate lifecycle — for external launchers (roren, ytflow_worker).
struct TFlowVanillaOptions
{
    TFlowNodeConfigPtr NodeConfig;
    TFlowVanillaTask Controller;
    TFlowVanillaTask Worker;
    //! Secret values delivered to the jobs (YT_TOKEN added if absent).
    THashMap<std::string, std::string> SecureVault;
    std::optional<std::string> Pool;
    std::optional<std::string> Alias;
    std::optional<std::string> Title;
    std::optional<std::string> NetworkProject;
    std::string SolomonResolverTag;
    int MaxFailedJobCount = 10000;
    //! Number of jobs whose stderr the scheduler retains after they finish.
    int MaxStderrCount = DefaultMaxStderrCount;
    //! Optional operation `description` annotation.
    NYTree::IMapNodePtr Description;
    //! Cluster to run the operation on; defaults to the node config's cluster.
    std::optional<std::string> RuntimeCluster;
    //! RPC proxy role for the runtime cluster; defaults to the node config's role.
    std::optional<std::string> ProxyRole;
    //! Content-addressed cache the job files are uploaded to; defaults to the shared wrapper cache.
    NYPath::TYPath CachePath;
    //! Proxy URL aliasing rules propagated into the job environment; when empty, the launcher's own
    //! YT_PROXY_URL_ALIASING_CONFIG environment variable is propagated instead.
    THashMap<std::string, std::string> ProxyUrlAliasingRules;
};

//! Builds and starts a Flow vanilla operation (controller + worker) via StartOperation, uploading the
//! job binary and files through the runtime cluster's file cache. Returns the new operation id.
std::string StartFlowVanillaOperation(const TFlowVanillaOptions& options);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
