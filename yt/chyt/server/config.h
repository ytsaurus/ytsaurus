#pragma once

#include "private.h"

#include <yt/yt/server/lib/misc/config.h>

#include <yt/yt/ytlib/api/native/config.h>

#include <yt/yt/ytlib/chunk_client/config.h>

#include <yt/yt/ytlib/table_client/config.h>

#include <yt/yt/ytlib/object_client/config.h>

#include <yt/yt/ytlib/security_client/config.h>

#include <yt/yt/client/ypath/rich.h>

#include <yt/yt/client/table_client/config.h>

#include <yt/yt/core/concurrency/config.h>

#include <yt/yt/core/ytree/fluent.h>

#include <yt/yt/core/yson/public.h>

#include <yt/yt/core/ytree/yson_struct.h>

#include <yt/yt/library/clickhouse_discovery/config.h>

#include <yt/yt/library/re2/public.h>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

//! Settings affecting how CHYT behaves around composite values and any columns.
class TCompositeSettings
    : public NYTree::TYsonStruct
{
public:
    EExtendedYsonFormat DefaultYsonFormat;

    REGISTER_YSON_STRUCT(TCompositeSettings);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TCompositeSettings);

////////////////////////////////////////////////////////////////////////////////

class TDynamicTableSettings
    : public NYTree::TYsonStruct
{
public:
    bool EnableDynamicStoreRead;

    int WriteRetryCount;

    TDuration WriteRetryBackoff;

    int MaxRowsPerWrite;

    NTransactionClient::EAtomicity TransactionAtomicity;

    bool FetchFromTablets;

    REGISTER_YSON_STRUCT(TDynamicTableSettings);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDynamicTableSettings);

////////////////////////////////////////////////////////////////////////////////

class TTestingSettings
    : public NYTree::TYsonStruct
{
public:
    bool EnableKeyConditionFiltering;
    bool MakeUpperBoundInclusive;

    bool ThrowExceptionInDistributor;
    bool ThrowExceptionInSubquery;
    i64 SubqueryAllocationSize;

    bool HangControlInvoker;

    //! If |value| > 0, clique nodes are replaced with |value| virtual local nodes.
    int LocalCliqueSize;

    REGISTER_YSON_STRUCT(TTestingSettings);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TTestingSettings);

////////////////////////////////////////////////////////////////////////////////

class TExecutionSettings
    : public NYTree::TYsonStruct
{
public:
    //! Hard limit for query depth. Query will be aborted after reaching this.
    //! If |value| <= 0,  the limit is disabled.
    i64 QueryDepthLimit;
    // TODO(dakovalkov): i64 TotalSecondaryQueryLimit;

    i64 MinDataWeightPerSecondaryQuery;

    //! Limit for number of nodes which can be used in distributed join.
    //! If |value| <= 0, the limit is disabled.
    i64 JoinNodeLimit;
    //! Limit for number of nodes which can be used in distributed select.
    //! If |value| <= 0, the limit is disabled.
    i64 SelectNodeLimit;

    EJoinPolicy JoinPolicy;
    ESelectPolicy SelectPolicy;

    //! Seed for choosing instances to distribute queries deterministically.
    size_t DistributionSeed;

    //! Number of input streams to read queries in parallel.
    //! Larger number of input streams can increase query performance,
    //! but it leads to higher memory usage.
    //! It makes sense to lower number of input streams if the clique is
    //! overloaded by many concurrent queries.
    //! if |value| <= 0, the max_threads is used.
    i64 InputStreamsPerSecondaryQuery;

    //! Allow query processing up to advanced stages (e.g. AfterAggregation) on workers.
    //! Can change distribution sort key if it helps to process query up to higher stage.
    //! Higher query stage on workers lowers amount of work on coordinator.
    //! Optimized query processing stage also allows to enable distributed insert
    //! automatically for simple queries.
    bool OptimizeQueryProcessingStage;
    //! Add bound conditions for second table expression if the main table is sorted by join key.
    //! Useless without AllowSwitchToSortedPool.
    //! Prefiltering right table lowers memory usage in distirbuted join and can improve performance.
    bool FilterJoinedSubqueryBySortKey;

    //! Allow StorageDistributor to use sorted pool to optimize aggregation and joins.
    bool AllowSwitchToSortedPool;
    //! Allow StorageDistributor to truncate sort key to optimize aggregation.
    bool AllowKeyTruncating;

    //! If |true|, distributed RIGHT and FULL JOINs work in a usual way. (default)
    //! If |false|, rows from right table expression with null values in join key
    //! are discarded. This is not a standard behavior, but it's a little bit more
    //! efficient, because it avoids 'or isNull(column)' expressions.
    bool KeepNullsInRightOrFullJoin;

    //! The minimum query stage to enable distributed insert.
    EDistributedInsertStage DistributedInsertStage;

    REGISTER_YSON_STRUCT(TExecutionSettings);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TExecutionSettings);

////////////////////////////////////////////////////////////////////////////////

class TConcatTablesSettings
    : public NYTree::TYsonStruct
{
public:
    //! What to do if a column is missing in some tables (Drop / Throw / ReadAsNull).
    EMissingColumnMode MissingColumnMode;
    //! What to do if types of the column in diffrent tables do not match (Drop / Throw / ReadAsAny).
    ETypeMismatchMode TypeMismatchMode;
    //! Disable user-friendly check when there are no columns present in every input table.
    bool AllowEmptySchemaIntersection;
    //! Limit for number of tables in concat. If exceeded, the error is thrown.
    int MaxTables;

    REGISTER_YSON_STRUCT(TConcatTablesSettings);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TConcatTablesSettings)

////////////////////////////////////////////////////////////////////////////////

class TCachingSettings
    : public NYTree::TYsonStruct
{
public:
    //! Specifies how to invalidate cached attributes after table is modified (write/drop).
    EInvalidateCacheMode TableAttributesInvalidateMode;
    //! Timeout for outgoing 'InvalidateCachedObjectAttributes' RPC requests to other instances.
    TDuration InvalidateRequestTimeout;

    //! TODO(dakovalkov): Support per query options to disable caches (CHYT-498).
    // bool UseTableAttributesCache;
    // bool UsePermissionCache;
    // bool UseBlockCache;
    // bool UseColumnarStatisticsCache;

    REGISTER_YSON_STRUCT(TCachingSettings);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TCachingSettings)

////////////////////////////////////////////////////////////////////////////////

class TListDirSettings
    : public NYTree::TYsonStruct
{
public:
    //! Maximum number of nodes in listed directory. If exceeded, the error is thrown.
    //! Zero means 'default yt limit'.
    int MaxSize;

    REGISTER_YSON_STRUCT(TListDirSettings);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TListDirSettings)

////////////////////////////////////////////////////////////////////////////////

//! This class will be accessible either via settings or via default_settings.
class TQuerySettings
    : public NYTree::TYsonStruct
{
public:
    bool EnableColumnarRead;

    bool EnableComputedColumnDeduction;

    bool UseBlockSampling;

    EDeducedStatementMode DeducedStatementMode;

    bool LogKeyConditionDetails;

    bool ConvertRowBatchesInWorkerThreadPool;

    bool InferDynamicTableRangesFromPivotKeys;

    TCompositeSettingsPtr Composite;

    TDynamicTableSettingsPtr DynamicTable;

    TTestingSettingsPtr Testing;

    TExecutionSettingsPtr Execution;

    TConcatTablesSettingsPtr ConcatTables;

    TListDirSettingsPtr ListDir;

    NTableClient::TTableReaderConfigPtr TableReader;
    NTableClient::TTableWriterConfigPtr TableWriter;

    bool EnableReaderTracing;

    TCachingSettingsPtr Caching;

    REGISTER_YSON_STRUCT(TQuerySettings);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TQuerySettings)

////////////////////////////////////////////////////////////////////////////////

class THealthCheckerConfig
    : public NYTree::TYsonStruct
{
public:
    TDuration Period;
    TDuration Timeout;
    std::vector<TString> Queries;

    REGISTER_YSON_STRUCT(THealthCheckerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(THealthCheckerConfig)

////////////////////////////////////////////////////////////////////////////////

class TShowTablesConfig
    : public NYTree::TYsonStruct
{
public:
    std::vector<TString> Roots;

    REGISTER_YSON_STRUCT(TShowTablesConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TShowTablesConfig)

////////////////////////////////////////////////////////////////////////////////

class TSubqueryConfig
    : public NYTree::TYsonStruct
{
public:
    NChunkClient::TFetcherConfigPtr ChunkSliceFetcher;
    int MaxJobCountForPool;
    int MinDataWeightPerThread;

    // Two fields below are for the chunk spec fetcher.
    int MaxChunksPerFetch;
    int MaxChunksPerLocateRequest;

    i64 MaxDataWeightPerSubquery;
    bool UseColumnarStatistics;

    i64 MinSliceDataWeight;

    REGISTER_YSON_STRUCT(TSubqueryConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TSubqueryConfig)

////////////////////////////////////////////////////////////////////////////////

class TMemoryWatchdogConfig
    : public NYTree::TYsonStruct
{
public:
    //! Memory limit for the job.
    size_t MemoryLimit;

    //! If remaining memory becomes less than `CodicilWatermark`, process dumps its query registry
    //! to simplify the investigation of its inevitable^W possible death.
    size_t CodicilWatermark;

    TDuration WindowWidth;
    //! If remaining memory is does not exceed #WindowCodicilWatermark for #WindowWitdth time,
    //! dump process query registry and die.
    size_t WindowCodicilWatermark;

    //! Check period.
    TDuration Period;

    REGISTER_YSON_STRUCT(TMemoryWatchdogConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TMemoryWatchdogConfig);

////////////////////////////////////////////////////////////////////////////////

class TSecurityManagerConfig
    : public NYTree::TYsonStruct
{
public:
    bool Enable;

    TDuration OperationAclUpdatePeriod;

    REGISTER_YSON_STRUCT(TSecurityManagerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TSecurityManagerConfig);

////////////////////////////////////////////////////////////////////////////////

class TQueryStatisticsReporterConfig
    : public TArchiveReporterConfig
{
public:
    TArchiveHandlerConfigPtr DistributedQueriesHandler;
    TArchiveHandlerConfigPtr SecondaryQueriesHandler;
    TArchiveHandlerConfigPtr AncestorQueryIdsHandler;

    TString User;

    REGISTER_YSON_STRUCT(TQueryStatisticsReporterConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TQueryStatisticsReporterConfig);

////////////////////////////////////////////////////////////////////////////////

class TGossipConfig
    : public NYTree::TYsonStruct
{
public:
    //! Period to run the gossip procedure.
    //! Note: TPeriodicExecutor counts down the period since the completion of previous invocation,
    //! so the actual period will be in [Period, Period + Timeout].
    TDuration Period;
    //! Timeout for the gossip request. If it is exceeded, the instance is assumed to be dead.
    TDuration Timeout;

    //! We will ignore ping from unknown instances if discovery is younger than this.
    TDuration UnknownInstanceAgeThreshold;
    //! How many times we will handle ping from an unknown instance before ignoring it.
    int UnknownInstancePingLimit;
    //! Try to ping banned instances. It can help to prevent ban expiration for dead instances and
    //! to find mistakenly banned instances.
    bool PingBanned;
    //! Allow to unban the instance after successful gossip request.
    //! It can help to restore discovery list faster if the instance was banned because of
    //! transient error (e.g. temporary network overload).
    bool AllowUnban;

    REGISTER_YSON_STRUCT(TGossipConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TGossipConfig);

////////////////////////////////////////////////////////////////////////////////

class TInvokerLivenessCheckerConfig
    : public NYTree::TYsonStruct
{
public:
    bool Enabled;
    bool CoreDump;
    TDuration Period;
    TDuration Timeout;

    REGISTER_YSON_STRUCT(TInvokerLivenessCheckerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TInvokerLivenessCheckerConfig);

////////////////////////////////////////////////////////////////////////////////

class TQueryRegistryConfig
    : public NYTree::TYsonStruct
{
public:
    TDuration ProcessListSnapshotUpdatePeriod;

    bool SaveRunningQueries;
    bool SaveUsers;

    REGISTER_YSON_STRUCT(TQueryRegistryConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TQueryRegistryConfig);

////////////////////////////////////////////////////////////////////////////////

class TQuerySamplingConfig
    : public NYTree::TYsonStruct
{
public:
    double QuerySamplingRate;
    NRe2::TRe2Ptr UserAgentRegExp;

    REGISTER_YSON_STRUCT(TQuerySamplingConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TQuerySamplingConfig);

////////////////////////////////////////////////////////////////////////////////

class TClickHouseTableConfig
    : public NYTree::TYsonStruct
{
public:
    TString Database;
    TString Name;
    TString Engine;

    static TClickHouseTableConfigPtr Create(TString database, TString name, TString engine);

    REGISTER_YSON_STRUCT(TClickHouseTableConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TClickHouseTableConfig);

////////////////////////////////////////////////////////////////////////////////

class TQueryLogConfig
    : public NYTree::TYsonStruct
{
public:
    //! AdditionalTables is a list of tables (except system.query_log itself)
    //! which should be created for proper query_log operation.
    std::vector<TClickHouseTableConfigPtr> AdditionalTables;

    REGISTER_YSON_STRUCT(TQueryLogConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TQueryLogConfig);

////////////////////////////////////////////////////////////////////////////////

class TYtConfig
    : public NYTree::TYsonStruct
{
public:
    //! Clique id = id of containing operation.
    TGuid CliqueId;
    //! Instance id = job id of containing job.
    TGuid InstanceId;
    TString CliqueAlias;
    i64 CliqueIncarnation;
    //! Address override when entering discovery group.
    std::optional<TString> Address;

    TSlruCacheConfigPtr ClientCache;

    THashSet<TString> UserAgentBlacklist;

    NRe2::TRe2Ptr UserNameBlacklist;
    NRe2::TRe2Ptr UserNameWhitelist;

    // COMPAT(max42): deprecate these.
    std::optional<bool> ValidateOperationAccess;
    std::optional<TDuration> OperationAclUpdatePeriod;

    TSecurityManagerConfigPtr SecurityManager;

    //! User for communication with YT.
    TString User;

    TMemoryWatchdogConfigPtr MemoryWatchdog;

    //! Note that CliqueId will be added to Directory automatically.
    TDiscoveryConfigPtr Discovery;

    TGossipConfigPtr Gossip;

    TInvokerLivenessCheckerConfigPtr ControlInvokerChecker;

    //! Config for cache which is used for checking read permissions to tables.
    NSecurityClient::TPermissionCacheConfigPtr PermissionCache;

    //! Config for cache which is used for getting table's attributes, like id, schema, external_cell_tag, etc.
    NObjectClient::TObjectAttributeCacheConfigPtr TableAttributeCache;

    //! Config for cache which is used for WHERE to PREWHERE optimizator.
    NTableClient::TTableColumnarStatisticsCacheConfigPtr TableColumnarStatisticsCache;

    int WorkerThreadCount;
    int FetcherThreadCount;

    std::optional<int> CpuLimit;

    //! Subquery logic configuration.
    TSubqueryConfigPtr Subquery;

    NYTree::INodePtr CreateTableDefaultAttributes;

    //! Total amount of memory available for chunk readers.
    i64 TotalReaderMemoryLimit;

    //! Initial memory reservation for reader.
    i64 ReaderMemoryRequirement;

    THealthCheckerConfigPtr HealthChecker;

    TShowTablesConfigPtr ShowTables;

    TDuration TotalMemoryTrackerUpdatePeriod;

    TQuerySettingsPtr QuerySettings;

    TQueryStatisticsReporterConfigPtr QueryStatisticsReporter;

    TQueryRegistryConfigPtr QueryRegistry;

    TQuerySamplingConfigPtr QuerySampling;

    TQueryLogConfigPtr QueryLog;

    REGISTER_YSON_STRUCT(TYtConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TYtConfig);

////////////////////////////////////////////////////////////////////////////////

class TLauncherConfig
    : public NYTree::TYsonStruct
{
public:
    int Version;

    REGISTER_YSON_STRUCT(TLauncherConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TLauncherConfig);

////////////////////////////////////////////////////////////////////////////////
//
// Values in braces are not defined explicitly, but rather taken in account when setting
// the rest of the values. Values starting with hash sign are defined explicitly.
//
// | <================================================= #MemoryLimit ============================================> |
// | <================== #MaxServerMemoryUsage ==================> | <========== (ClickHouseWatermark) ==========> |
// | #Reader | #UncompressedBlockCache + | (CH Memory + Footprint) |                       | #WatchdogOomWatermark |
// |         | #CompressedBlockCache +   |            | <============== #WatchdogOomWindowWatermark =============> |
// |         | #ClientBlockMetaCache     |
//                                                         ^              ^                     ^                  ^
// If min rss over 15 min window resides in this __________|              |                     |                  |
// range, instance performs graceful self-interruption.                   |                     |                  |
//                                                                        |                     |                  |
// If rss goes here, CH does not allow any new memory allocation. ________|                     |                  |
//                                                                                              |                  |
// If rss goes here, instance performs harakiri. _______________________________________________|                  |
//                                                                                                                 |
// If rss goes here, YT kills the instance. _______________________________________________________________________|
//
// Memory tracking is one hell of a job.

class TMemoryConfig
    : public NYTree::TYsonStruct
{
public:
    std::optional<i64> Reader;
    std::optional<i64> UncompressedBlockCache;
    std::optional<i64> CompressedBlockCache;
    std::optional<i64> ChunkMetaCache;
    std::optional<i64> MemoryLimit;
    std::optional<i64> MaxServerMemoryUsage;
    std::optional<i64> WatchdogOomWatermark;
    std::optional<i64> WatchdogOomWindowWatermark;

    REGISTER_YSON_STRUCT(TMemoryConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TMemoryConfig);

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
    : public TNativeServerConfig
{
public:
    TClickHouseConfigPtr ClickHouse;

    TYtConfigPtr Yt;

    //! Instance will not shutdown during this period of time after receiving signal even
    //! if there are not any running queries.
    //! To avoid receiving queries after shutdown, this value should be greater than gossip period.
    TDuration GracefulInterruptionDelay;

    //! Hard timeout for process termination after receiving the interruption signal.
    //! If the timeout is exceeded, the process will be forcefully terminated and the job will be marked as failed.
    TDuration InterruptionTimeout;

    TLauncherConfigPtr Launcher;

    // COMPAT(max42): deprecate in favor of yt/cpu_limit.
    std::optional<int> CpuLimit;

    TMemoryConfigPtr Memory;

    int RpcQueryServiceThreadCount;

    TPorts GetPorts() const;

    REGISTER_YSON_STRUCT(TClickHouseServerBootstrapConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TClickHouseServerBootstrapConfig);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
