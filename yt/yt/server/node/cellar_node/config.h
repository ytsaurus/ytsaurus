#pragma once

#include "public.h"

#include <yt/yt/server/node/cluster_node/public.h>

#include <yt/yt/server/lib/cellar_agent/config.h>

#include <yt/yt/server/lib/transaction_supervisor/config.h>

#include <yt/yt/core/ytree/yson_struct.h>

#include <yt/yt/core/concurrency/config.h>

namespace NYT::NCellarNode {

////////////////////////////////////////////////////////////////////////////////

using TMemoryLimitsEnumIndexedVector = TEnumIndexedArray<EMemoryCategory, NClusterNode::TMemoryLimitPtr>;

////////////////////////////////////////////////////////////////////////////////

class TMasterConnectorConfig
    : public NYTree::TYsonStruct
{
public:
    //! Period between consequent cellar node heartbeats.
    TDuration HeartbeatPeriod;

    //! Splay for cellar node heartbeats.
    TDuration HeartbeatPeriodSplay;

    NConcurrency::TRetryingPeriodicExecutorOptions HeartbeatExecutor;

    REGISTER_YSON_STRUCT(TMasterConnectorConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TMasterConnectorConfig)

////////////////////////////////////////////////////////////////////////////////

class TMasterConnectorDynamicConfig
    : public NYTree::TYsonStruct
{
public:
    std::optional<NConcurrency::TRetryingPeriodicExecutorOptions> HeartbeatExecutor;

    //! Timeout of the cellar node heartbeat RPC request.
    TDuration HeartbeatTimeout;

    REGISTER_YSON_STRUCT(TMasterConnectorDynamicConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TMasterConnectorDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

class TCellarNodeDynamicConfig
    : public NYTree::TYsonStruct
{
public:
    NCellarAgent::TCellarManagerDynamicConfigPtr CellarManager;

    TMasterConnectorDynamicConfigPtr MasterConnector;

    REGISTER_YSON_STRUCT(TCellarNodeDynamicConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TCellarNodeDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

class TCellarNodeConfig
    : public NYTree::TYsonStruct
{
public:
    NCellarAgent::TCellarManagerConfigPtr CellarManager;

    TMasterConnectorConfigPtr MasterConnector;

    NTransactionSupervisor::TTransactionLeaseTrackerConfigPtr TransactionLeaseTracker;

    REGISTER_YSON_STRUCT(TCellarNodeConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TCellarNodeConfig)

///////////////////////////////////////////////////////////////////////////////

struct TCpuLimits
    : public NYTree::TYsonStruct
{
    std::optional<int> WriteThreadPoolSize;
    std::optional<int> LookupThreadPoolSize;
    std::optional<int> QueryThreadPoolSize;

    REGISTER_YSON_STRUCT(TCpuLimits);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TCpuLimits)

////////////////////////////////////////////////////////////////////////////////

struct TMemoryLimits
    : public NYTree::TYsonStruct
{
    std::optional<i64> TabletStatic;
    std::optional<i64> TabletDynamic;
    std::optional<i64> CompressedBlockCache;
    std::optional<i64> UncompressedBlockCache;
    std::optional<i64> KeyFilterBlockCache;
    std::optional<i64> VersionedChunkMeta;
    std::optional<i64> LookupRowCache;

    TMemoryLimitsEnumIndexedVector AsEnumIndexedVector() const;

    REGISTER_YSON_STRUCT(TMemoryLimits);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TMemoryLimits)

///////////////////////////////////////////////////////////////////////////////

struct TMediumThroughputLimits
    : public NYTree::TYsonStruct
{
    i64 WriteByteRate;
    i64 ReadByteRate;

    REGISTER_YSON_STRUCT(TMediumThroughputLimits);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TMediumThroughputLimits)

///////////////////////////////////////////////////////////////////////////////

struct TBundleDynamicConfig
    : public NYTree::TYsonStruct
{
    TCpuLimitsPtr CpuLimits;
    TMemoryLimitsPtr MemoryLimits;
    THashMap<TString, TMediumThroughputLimitsPtr> MediumThroughputLimits;

    REGISTER_YSON_STRUCT(TBundleDynamicConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TBundleDynamicConfig)

///////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellarNode
