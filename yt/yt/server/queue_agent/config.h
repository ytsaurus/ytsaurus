#pragma once

#include "private.h"

#include <yt/yt/server/lib/cypress_election/config.h>

#include <yt/yt/server/lib/misc/config.h>

#include <yt/yt/client/ypath/public.h>

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/ytlib/discovery_client/public.h>

#include <yt/yt/ytlib/queue_client/public.h>

#include <yt/yt/core/ytree/yson_struct.h>

#include <yt/yt/library/dynamic_config/config.h>

namespace NYT::NQueueAgent {

////////////////////////////////////////////////////////////////////////////////

class TAlertManagerDynamicConfig
    : public NYTree::TYsonStruct
{
public:
    TDuration AlertCollectionPeriod;

    REGISTER_YSON_STRUCT(TAlertManagerDynamicConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TAlertManagerDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

class TCypressSynchronizerConfig
    : public NYTree::TYsonStruct
{
public:
    REGISTER_YSON_STRUCT(TCypressSynchronizerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TCypressSynchronizerConfig)

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(ECypressSynchronizerPolicy,
    (Polling)
    (Watching)
);

class TCypressSynchronizerDynamicConfig
    : public NYTree::TYsonStruct
{
public:
    //! Cypress poll period.
    TDuration PassPeriod;

    //! Flag for disabling cypress synchronizer entirely; used primarily for tests.
    bool Enable;

    //! Policy used for following updates to queues/consumers.
    ECypressSynchronizerPolicy Policy;

    //! Clusters polled by the watching version of the synchronizer.
    std::vector<TString> Clusters;

    REGISTER_YSON_STRUCT(TCypressSynchronizerDynamicConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TCypressSynchronizerDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

class TQueueAgentConfig
    : public NYTree::TYsonStruct
{
public:
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

class TQueueControllerDynamicConfig
    : public NYTree::TYsonStruct
{
public:
    //! Controller pass period. Defines the period of monitoring information exporting, automatic
    //! trimming and the maximum age of cached Orchid state.
    TDuration PassPeriod;

    //! Flag for disabling automatic trimming entirely.
    bool EnableAutomaticTrimming;

    REGISTER_YSON_STRUCT(TQueueControllerDynamicConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TQueueControllerDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

class TQueueAgentDynamicConfig
    : public NYTree::TYsonStruct
{
public:
    //! State table poll period.
    TDuration PassPeriod;

    //! Controller thread pool thread count.
    int ControllerThreadCount;

    //! Configuration of queue controllers.
    TQueueControllerDynamicConfigPtr Controller;

    REGISTER_YSON_STRUCT(TQueueAgentDynamicConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TQueueAgentDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

class TQueueAgentShardingManagerDynamicConfig
    : public NYTree::TYsonStruct
{
public:
    TDuration PassPeriod;

    REGISTER_YSON_STRUCT(TQueueAgentShardingManagerDynamicConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TQueueAgentShardingManagerDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

class TQueueAgentServerConfig
    : public TNativeServerConfig
{
public:
    TQueueAgentConfigPtr QueueAgent;

    TCypressSynchronizerConfigPtr CypressSynchronizer;

    bool AbortOnUnrecognizedOptions;

    //! User for native clients in queue agent and cypress synchronizer.
    TString User;

    //! Paths to queue agent state.
    NQueueClient::TQueueAgentDynamicStateConfigPtr  DynamicState;

    NYTree::IMapNodePtr CypressAnnotations;

    NCypressElection::TCypressElectionManagerConfigPtr ElectionManager;

    NDynamicConfig::TDynamicConfigManagerConfigPtr DynamicConfigManager;
    TString DynamicConfigPath;

    REGISTER_YSON_STRUCT(TQueueAgentServerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TQueueAgentServerConfig)

////////////////////////////////////////////////////////////////////////////////

class TQueueAgentServerDynamicConfig
    : public TNativeSingletonsDynamicConfig
{
public:
    NDiscoveryClient::TMemberClientConfigPtr MemberClient;
    NDiscoveryClient::TDiscoveryClientConfigPtr DiscoveryClient;

    TAlertManagerDynamicConfigPtr AlertManager;
    TQueueAgentShardingManagerDynamicConfigPtr QueueAgentShardingManager;
    TQueueAgentDynamicConfigPtr QueueAgent;
    TCypressSynchronizerDynamicConfigPtr CypressSynchronizer;

    REGISTER_YSON_STRUCT(TQueueAgentServerDynamicConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TQueueAgentServerDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueueAgent
