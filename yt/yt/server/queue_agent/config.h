#pragma once

#include "private.h"

#include <yt/yt/server/lib/cypress_election/config.h>

#include <yt/yt/server/lib/misc/config.h>

#include <yt/yt/client/ypath/public.h>
#include <yt/yt/client/ypath/rich.h>

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/ytlib/discovery_client/public.h>

#include <yt/yt/ytlib/queue_client/public.h>

#include <yt/yt/library/dynamic_config/config.h>

#include <yt/yt/library/server_program/config.h>

#include <yt/yt/library/program/config.h>

#include <yt/yt/core/concurrency/config.h>

#include <yt/yt/core/misc/config.h>

namespace NYT::NQueueAgent {

////////////////////////////////////////////////////////////////////////////////

struct TCypressSynchronizerConfig
    : public NYTree::TYsonStruct
{
    REGISTER_YSON_STRUCT(TCypressSynchronizerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TCypressSynchronizerConfig)

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(ECypressSynchronizerPolicy,
    (Polling)
    (Watching)
);

struct TCypressSynchronizerDynamicConfig
    : public NYTree::TYsonStruct
{
    //! Cypress poll period.
    TDuration PassPeriod;

    //! Flag for disabling Cypress synchronizer entirely; used primarily for tests.
    bool Enable;

    //! Policy used for following updates to queues/consumers.
    ECypressSynchronizerPolicy Policy;

    //! Clusters polled by the watching version of the synchronizer.
    std::vector<TString> Clusters;

    //! If true, the synchronizer will add objects from the registration table to the list of objects to watch.
    //! NB: This flag is only supported with the `watching` policy.
    bool PollReplicatedObjects;
    //! If true, the synchronizer will update the configured replicated table mapping table with the corresponding meta.
    //! NB: This flag is only supported with the `watching` policy, as well as enabled polling of replicated objects.
    bool WriteReplicatedTableMapping;
    //! COMPAT(achulkov2): Remove this once the queue_agent_stage attribute is supported for chaos replicated tables.
    //! Currently chaos replicated tables do not have a builtin queue_agent_stage attribute, thus we set a default
    //! stage for crt-objects in the dynamic config.
    std::string ChaosReplicatedTableQueueAgentStage;

    REGISTER_YSON_STRUCT(TCypressSynchronizerDynamicConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TCypressSynchronizerDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

struct TQueueAgentConfig
    : public NYTree::TYsonStruct
{
    //! Used to create channels to other queue agents.
    NBus::TBusConfigPtr BusClient;

    //! Identifies a family of queue agents.
    //! Each queue agent only handles queues and consumers with the corresponding attribute set to its own stage.
    TString Stage;

    REGISTER_YSON_STRUCT(TQueueAgentConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TQueueAgentConfig)

////////////////////////////////////////////////////////////////////////////////

// NB(apachee): Separate config for exports for future refactoring. See YT-23208.

struct TQueueExporterDynamicConfig
    : public NYTree::TYsonStructLite
{
    static const TExponentialBackoffOptions DefaultRetryBackoff;

    bool Enable;

    //! Queue exporter pass period. Defines the minimum duration between 2 consecutive export iterations.
    TDuration PassPeriod;
    //! Maximum number of static tables exported per single export iteration.
    int MaxExportedTableCountPerTask;

    //! Retry backoff options in case export task fails.
    /*!
     * \note #InvocationCount must be int max as exports are retried indefinitely.
    */
    TExponentialBackoffOptions RetryBackoff;

    NConcurrency::TPeriodicExecutorOptions GetPeriodicExecutorOptions() const;

    bool operator==(const TQueueExporterDynamicConfig&) const = default;

    REGISTER_YSON_STRUCT_LITE(TQueueExporterDynamicConfig);

    static void Register(TRegistrar registrar);
};

////////////////////////////////////////////////////////////////////////////////

struct TQueueControllerDynamicConfig
    : public NYTree::TYsonStruct
{
    //! Controller pass period. Defines the period of monitoring information exporting, automatic
    //! trimming and the maximum age of cached Orchid state.
    TDuration PassPeriod;

    //! Flag for disabling automatic trimming entirely.
    bool EnableAutomaticTrimming;
    //! If set, trimming requests to individual partitions are performed with roughly this period.
    //! Pass period is used by default.
    //! NB: Internally this is implemented via a multiplier for trimming frequency. A trimming iteration will occur
    //! once in `ceil(TrimmingPeriod / PassPeriod)` queue controller passes.
    std::optional<TDuration> TrimmingPeriod;

    //! COMPAT(apachee): This flag is used to disable taking exports progress
    //! into account for CRT queues, since at this moment this can potentially
    //! lead to crash in tabnodes (see YT-22882).
    //! Default is false to reflect previous behavior that is known to work.
    bool EnableCrtTrimByExports;

    //! List of objects, for which controllers must be delayed every pass.
    //!
    //! Passes of such controllers take additional #ControllerDelayDuration seconds
    //! to complete. This should be used for debug only.
    std::vector<NYPath::TRichYPath> DelayedObjects;
    //! Delay duration for #DelayedObjects.
    TDuration ControllerDelayDuration;

    TQueueExporterDynamicConfig QueueExporter;

    NAlertManager::TAlertManagerDynamicConfigPtr AlertManager;

    REGISTER_YSON_STRUCT(TQueueControllerDynamicConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TQueueControllerDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

struct TQueueExportManagerDynamicConfig
    : public NYTree::TYsonStruct
{
    //! Maximum number of export starting per second.
    std::optional<double> ExportRateLimit;

    REGISTER_YSON_STRUCT(TQueueExportManagerDynamicConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TQueueExportManagerDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

struct TQueueAgentDynamicConfig
    : public NYTree::TYsonStruct
{
    //! State table poll period.
    TDuration PassPeriod;

    //! Controller thread pool thread count.
    int ControllerThreadCount;

    //! Configuration of queue controllers.
    TQueueControllerDynamicConfigPtr Controller;

    //! Configuration of queue export manager.
    TQueueExportManagerDynamicConfigPtr QueueExportManager;

    //! The limit on how much objects are shown in each list of "inactive_objects" in "controller_info".
    i64 InactiveObjectDisplayLimit;

    //! Controls whether replicated objects are handled by this queue agent instance.
    //! NB: Even when set to true, mutating requests are only performed for objects with the corresponding stage.
    bool HandleReplicatedObjects;

    REGISTER_YSON_STRUCT(TQueueAgentDynamicConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TQueueAgentDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

struct TQueueAgentShardingManagerDynamicConfig
    : public NYTree::TYsonStruct
{
    TDuration PassPeriod;
    TDuration SyncBannedInstancesPeriod;

    REGISTER_YSON_STRUCT(TQueueAgentShardingManagerDynamicConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TQueueAgentShardingManagerDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

struct TQueueAgentBootstrapConfig
    : public NServer::TNativeServerBootstrapConfig
{
    TQueueAgentConfigPtr QueueAgent;

    TCypressSynchronizerConfigPtr CypressSynchronizer;

    bool AbortOnUnrecognizedOptions;

    //! User for native clients in queue agent and Cypress synchronizer.
    std::string User;

    //! Paths to queue agent state.
    NQueueClient::TQueueAgentDynamicStateConfigPtr DynamicState;

    NYTree::IMapNodePtr CypressAnnotations;

    NCypressElection::TCypressElectionManagerConfigPtr ElectionManager;

    NDynamicConfig::TDynamicConfigManagerConfigPtr DynamicConfigManager;
    TString DynamicConfigPath;

    REGISTER_YSON_STRUCT(TQueueAgentBootstrapConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TQueueAgentBootstrapConfig)

////////////////////////////////////////////////////////////////////////////////

struct TQueueAgentProgramConfig
    : public TQueueAgentBootstrapConfig
    , public TServerProgramConfig
{
    REGISTER_YSON_STRUCT(TQueueAgentProgramConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TQueueAgentProgramConfig)

////////////////////////////////////////////////////////////////////////////////

struct TQueueAgentComponentDynamicConfig
    : public TSingletonsDynamicConfig
{
    NDiscoveryClient::TMemberClientConfigPtr MemberClient;
    NDiscoveryClient::TDiscoveryClientConfigPtr DiscoveryClient;

    NAlertManager::TAlertManagerDynamicConfigPtr AlertManager;
    TQueueAgentShardingManagerDynamicConfigPtr QueueAgentShardingManager;
    TQueueAgentDynamicConfigPtr QueueAgent;
    TCypressSynchronizerDynamicConfigPtr CypressSynchronizer;

    REGISTER_YSON_STRUCT(TQueueAgentComponentDynamicConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TQueueAgentComponentDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueueAgent
