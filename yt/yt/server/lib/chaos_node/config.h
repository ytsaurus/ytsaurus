#pragma once

#include "public.h"

#include <yt/yt/core/ytree/yson_struct.h>

#include <yt/yt/server/lib/cellar_agent/public.h>

#include <yt/yt/ytlib/chaos_client/public.h>

#include <yt/yt/library/dynamic_config/public.h>

namespace NYT::NChaosNode {

////////////////////////////////////////////////////////////////////////////////

struct TChaosCellSynchronizerConfig
    : public NYTree::TYsonStruct
{
    TDuration SyncPeriod;

    REGISTER_YSON_STRUCT(TChaosCellSynchronizerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TChaosCellSynchronizerConfig)

////////////////////////////////////////////////////////////////////////////////

struct TReplicationCardObserverConfig
    : public NYTree::TYsonStruct
{
    TDuration ObservationPeriod;
    i64 ReplicationCardCountPerRound;

    REGISTER_YSON_STRUCT(TReplicationCardObserverConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TReplicationCardObserverConfig)

////////////////////////////////////////////////////////////////////////////////

struct TMigratedReplicationCardRemoverConfig
    : public NYTree::TYsonStruct
{
    TDuration RemovePeriod;

    REGISTER_YSON_STRUCT(TMigratedReplicationCardRemoverConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TMigratedReplicationCardRemoverConfig)

////////////////////////////////////////////////////////////////////////////////

struct TForeignMigratedReplicationCardRemoverConfig
    : public NYTree::TYsonStruct
{
    TDuration RemovePeriod;
    TDuration ReplicationCardKeepAlivePeriod;

    REGISTER_YSON_STRUCT(TForeignMigratedReplicationCardRemoverConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TForeignMigratedReplicationCardRemoverConfig)

////////////////////////////////////////////////////////////////////////////////

struct TChaosManagerConfig
    : public NYTree::TYsonStruct
{
    TChaosCellSynchronizerConfigPtr ChaosCellSynchronizer;
    TReplicationCardObserverConfigPtr ReplicationCardObserver;
    TDuration EraCommencingPeriod;
    TMigratedReplicationCardRemoverConfigPtr MigratedReplicationCardRemover;
    TForeignMigratedReplicationCardRemoverConfigPtr ForeignMigratedReplicationCardRemover;
    TDuration LeftoverMigrationPeriod;

    REGISTER_YSON_STRUCT(TChaosManagerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TChaosManagerConfig)

////////////////////////////////////////////////////////////////////////////////

struct TCoordinatorManagerConfig
    : public NYTree::TYsonStruct
{
    REGISTER_YSON_STRUCT(TCoordinatorManagerConfig);

    static void Register(TRegistrar )
    { }
};

DEFINE_REFCOUNTED_TYPE(TCoordinatorManagerConfig)

////////////////////////////////////////////////////////////////////////////////

struct TTransactionManagerConfig
    : public NYTree::TYsonStruct
{
    TDuration MaxTransactionTimeout;
    int MaxAbortedTransactionPoolSize;

    REGISTER_YSON_STRUCT(TTransactionManagerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TTransactionManagerConfig)

////////////////////////////////////////////////////////////////////////////////

struct TChaosNodeConfig
    : public NYTree::TYsonStruct
{
    NCellarAgent::TCellarOccupantConfigPtr CellarOccupant;

    TTransactionManagerConfigPtr TransactionManager;

    TChaosManagerConfigPtr ChaosManager;
    NChaosClient::TReplicationCardsWatcherConfigPtr ReplicationCardsWatcher;

    TCoordinatorManagerConfigPtr CoordinatorManager;

    TDuration SlotScanPeriod;

    int SnapshotStoreReadPoolSize;

    NDynamicConfig::TDynamicConfigManagerConfigPtr ReplicatedTableTrackerConfigFetcher;

    REGISTER_YSON_STRUCT(TChaosNodeConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TChaosNodeConfig)

////////////////////////////////////////////////////////////////////////////////

struct TChaosSlotDynamicConfig
    : public NYTree::TYsonStruct
{
    bool EnableVerboseLogging;

    REGISTER_YSON_STRUCT(TChaosSlotDynamicConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TChaosSlotDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

struct TChaosNodeDynamicConfig
    : public NYTree::TYsonStruct
{
    THashMap<std::string, TChaosSlotDynamicConfigPtr> PerBundleConfigs;

    REGISTER_YSON_STRUCT(TChaosNodeDynamicConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TChaosNodeDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosNode
