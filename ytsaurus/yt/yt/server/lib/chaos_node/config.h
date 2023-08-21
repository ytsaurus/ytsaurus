#pragma once

#include "public.h"

#include <yt/yt/core/ytree/yson_struct.h>

#include <yt/yt/server/lib/cellar_agent/public.h>

#include <yt/yt/library/dynamic_config/public.h>

namespace NYT::NChaosNode {

////////////////////////////////////////////////////////////////////////////////

class TChaosCellSynchronizerConfig
    : public NYTree::TYsonStruct
{
public:
    TDuration SyncPeriod;

    REGISTER_YSON_STRUCT(TChaosCellSynchronizerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TChaosCellSynchronizerConfig)

////////////////////////////////////////////////////////////////////////////////

class TReplicationCardObserverConfig
    : public NYTree::TYsonStruct
{
public:
    TDuration ObservationPeriod;
    i64 ReplicationCardCountPerRound;

    REGISTER_YSON_STRUCT(TReplicationCardObserverConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TReplicationCardObserverConfig)

////////////////////////////////////////////////////////////////////////////////

class TMigratedReplicationCardRemoverConfig
    : public NYTree::TYsonStruct
{
public:
    TDuration RemovePeriod;

    REGISTER_YSON_STRUCT(TMigratedReplicationCardRemoverConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TMigratedReplicationCardRemoverConfig)

////////////////////////////////////////////////////////////////////////////////

class TForeignMigratedReplicationCardRemoverConfig
    : public NYTree::TYsonStruct
{
public:
    TDuration RemovePeriod;
    TDuration ReplicationCardKeepAlivePeriod;

    REGISTER_YSON_STRUCT(TForeignMigratedReplicationCardRemoverConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TForeignMigratedReplicationCardRemoverConfig)

////////////////////////////////////////////////////////////////////////////////

class TChaosManagerConfig
    : public NYTree::TYsonStruct
{
public:
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

class TCoordinatorManagerConfig
    : public NYTree::TYsonStruct
{
public:
    REGISTER_YSON_STRUCT(TCoordinatorManagerConfig);

    static void Register(TRegistrar )
    { }
};

DEFINE_REFCOUNTED_TYPE(TCoordinatorManagerConfig)

////////////////////////////////////////////////////////////////////////////////

class TTransactionManagerConfig
    : public NYTree::TYsonStruct
{
public:
    TDuration MaxTransactionTimeout;
    int MaxAbortedTransactionPoolSize;

    REGISTER_YSON_STRUCT(TTransactionManagerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TTransactionManagerConfig)

////////////////////////////////////////////////////////////////////////////////

class TChaosNodeConfig
    : public NYTree::TYsonStruct
{
public:
    NCellarAgent::TCellarOccupantConfigPtr CellarOccupant;

    TTransactionManagerConfigPtr TransactionManager;

    TChaosManagerConfigPtr ChaosManager;

    TCoordinatorManagerConfigPtr CoordinatorManager;

    TDuration SlotScanPeriod;

    int SnapshotStoreReadPoolSize;

    NDynamicConfig::TDynamicConfigManagerConfigPtr ReplicatedTableTrackerConfigFetcher;

    REGISTER_YSON_STRUCT(TChaosNodeConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TChaosNodeConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosNode
