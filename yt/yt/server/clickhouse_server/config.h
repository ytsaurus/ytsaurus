#pragma once

#include "private.h"

#include <yt/server/lib/misc/config.h>

#include <yt/ytlib/api/native/config.h>

#include <yt/ytlib/chunk_client/config.h>

#include <yt/ytlib/table_client/config.h>

#include <yt/ytlib/object_client/config.h>

#include <yt/ytlib/security_client/config.h>

#include <yt/client/misc/config.h>

#include <yt/client/ypath/rich.h>

#include <yt/core/concurrency/config.h>

#include <yt/core/ytree/fluent.h>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

class THealthCheckerConfig
    : public NYTree::TYsonSerializable
{
public:
    TDuration Period;
    TDuration Timeout;
    std::vector<TString> Queries;

    THealthCheckerConfig();
};

DEFINE_REFCOUNTED_TYPE(THealthCheckerConfig)

////////////////////////////////////////////////////////////////////////////////

class TSubqueryConfig
    : public NYTree::TYsonSerializable
{
public:
    NChunkClient::TFetcherConfigPtr ChunkSliceFetcher;
    int MaxJobCountForPool;
    int MinDataWeightPerThread;

    // Two fields below are for the chunk spec fetcher.
    int MaxChunksPerFetch;
    int MaxChunksPerLocateRequest;

    ui64 MaxDataWeightPerSubquery;

    TSubqueryConfig();
};

DEFINE_REFCOUNTED_TYPE(TSubqueryConfig)

////////////////////////////////////////////////////////////////////////////////

class TMemoryWatchdogConfig
    : public NYTree::TYsonSerializable
{
public:
    //! Memory limit for the job.
    size_t MemoryLimit;

    //! If remaining memory becomes less than `CodicilWatermark`, process dumps its query registry
    //! to simplify the investigation of its inevitable^W possible death.
    size_t CodicilWatermark;

    //! Check period.
    TDuration Period;

    TMemoryWatchdogConfig();
};

DEFINE_REFCOUNTED_TYPE(TMemoryWatchdogConfig);

////////////////////////////////////////////////////////////////////////////////

class TSecurityManagerConfig
    : public NYTree::TYsonSerializable
{
public:
    bool Enable;

    TDuration OperationAclUpdatePeriod;

    TSecurityManagerConfig();
};

DEFINE_REFCOUNTED_TYPE(TSecurityManagerConfig);

////////////////////////////////////////////////////////////////////////////////

class TYtConfig
    : public NYTree::TYsonSerializable
{
public:
    //! Clique id = id of containing operation.
    TGuid CliqueId;
    //! Instance id = job id of containing job.
    TGuid InstanceId;

    TSlruCacheConfigPtr ClientCache;

    // COMPAT(max42): deprecate these.
    std::optional<bool> ValidateOperationAccess;
    std::optional<TDuration> OperationAclUpdatePeriod;

    TSecurityManagerConfigPtr SecurityManager;

    //! User for communication with YT.
    TString User;

    NTableClient::TTableWriterConfigPtr TableWriterConfig;

    TMemoryWatchdogConfigPtr MemoryWatchdog;

    //! Note that CliqueId will be added to Directory automatically.
    TDiscoveryConfigPtr Discovery;

    TDuration GossipPeriod;

    //! We will ignore ping from unknown instances if discovery is younger than this.
    TDuration UnknownInstanceAgeThreshold;

    //! How many times we will handle ping from an unknown instance before ignore it.
    int UnknownInstancePingLimit;

    //! Config for cache which is used for checking read permissions to tables.
    NSecurityClient::TPermissionCacheConfigPtr PermissionCache;

    //! Config for cache which is used for getting table's attributes, like id, schema, external_cell_tag, etc.
    NObjectClient::TObjectAttributeCacheConfigPtr TableAttributeCache;

    TDuration ProcessListSnapshotUpdatePeriod;

    int WorkerThreadCount;

    std::optional<int> CpuLimit;

    //! Subquery logic configuration.
    TSubqueryConfigPtr Subquery;

    NYTree::INodePtr CreateTableDefaultAttributes;

    //! Total amount of memory available for chunk readers.
    i64 TotalReaderMemoryLimit;

    //! Initial memory reservation for reader.
    i64 ReaderMemoryRequirement;

    TDuration ProfilingPeriod;

    THealthCheckerConfigPtr HealthChecker;

    TYtConfig();
};

DEFINE_REFCOUNTED_TYPE(TYtConfig);

////////////////////////////////////////////////////////////////////////////////

struct TPorts
{
    // YT ports.
    int Monitoring = 0;
    int Rpc = 0;
    // CH ports.
    int Http = 0;
    int Tcp = 0;
};

class TClickHouseServerBootstrapConfig
    : public TServerConfig
{
public:
    NApi::NNative::TConnectionConfigPtr ClusterConnection;

    TClickHouseConfigPtr ClickHouse;

    TYtConfigPtr Yt;

    //! Instance will not shutdown during this timeout after receiving signal even
    //! if there are not any running queries.
    //! To avoid receiving queries after shutdown, this value should be greater than GossipPeriod.
    TDuration InterruptionGracefulTimeout;

    TPorts GetPorts() const;

    TClickHouseServerBootstrapConfig();
};

DEFINE_REFCOUNTED_TYPE(TClickHouseServerBootstrapConfig);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
