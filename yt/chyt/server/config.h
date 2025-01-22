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

#include <yt/yt/core/misc/configurable_singleton_def.h>

#include <yt/yt/core/concurrency/config.h>

#include <yt/yt/core/ytree/fluent.h>

#include <yt/yt/core/yson/public.h>

#include <yt/yt/core/ytree/yson_struct.h>

#include <yt/yt/library/coredumper/config.h>

#include <yt/yt/library/profiling/solomon/config.h>

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

    bool ConvertUnsupportedTypesToString;

    static TCompositeSettingsPtr Create(bool convertUnsupportedTypesToString);

    REGISTER_YSON_STRUCT(TCompositeSettings);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TCompositeSettings)

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

DEFINE_REFCOUNTED_TYPE(TDynamicTableSettings)

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

    bool CheckChytBanned;

    std::optional<NYPath::TYPath> ChunkSpecFetcherBreakpoint;
    std::optional<NYPath::TYPath> InputStreamFactoryBreakpoint;
    std::optional<NYPath::TYPath> ConcatTableRangeBreakpoint;
    std::optional<NYPath::TYPath> ListDirsBreakpoint;

    REGISTER_YSON_STRUCT(TTestingSettings);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TTestingSettings)

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

    bool DistributeOnlyGlobalAndSortedJoin;

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
    //! Prefiltering right table lowers memory usage in distributed join and can improve performance.
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

    //! Mode defines how tables are locked for reading during the query execution.
    ETableReadLockMode TableReadLockMode;

    bool EnableMinMaxFiltering;

    //! The intent of this optimization is similar to the `optimize_read_in_order` setting in native CH.
    //! When enabled, some SELECT queries over sorted tables are processed in the order they are stored
    //! in YT, allowing to avoid reading all data in case of specified LIMIT.
    //! More specifically, it is enabled for SELECT queries that specify ORDER BY <prefix of storage primary key>
    //! and LIMIT <limit> when reading from one or multiple concatenated sorted tables.
    //! The optimization is not applied for queries with JOINs or aggregations.
    //! Consider disabling this option manually when running queries that specify ORDER BY, a large LIMIT and a
    //! WHERE condition that requires reading a huge amount of records before queried data is found.
    bool EnableOptimizeReadInOrder;

    //! NB: Using OptimizeReadInOrder with key columns that can have NULLs (non-required) or NANs (floating point columns) is complicated
    //! by the fact that NULLs and NANs are on the opposite ends of the sort order in YT, while in CH they are always grouped together
    //! in the beginning or end of the result.
    //! In YT, NULL values compare less than any other values, while NAN values are larger than all other floats.
    //! In ClickHouse their common placement is controlled by specifying NULLS FIRST or NULLS LAST for each element of the ORDER BY expression,
    //! with NULLS LAST being used by default.
    //!
    //! You have multiple options to make this optimization work in such cases, they are explained below in order of preference.
    //!
    //! First, you should prefer to use required key columns with CHYT. This should alleviate the problem with NULLs, leaving only NAN-related
    //! issues for floating-point key columns.
    //!
    //! Second, you can use the two options declared below to tell the CHYT optimizer that none of the key columns contain NULLs or NANs.
    //!
    //! Third, you can manually configure NULLS direction correctly in your query. The "correct" direction is the one that aligns with the sort
    //! order of the underlying storage. I.e. it NULLS FIRST for ASC sort with NULLs, NULLS LAST (default) for DESC sort with NULLS, NULLS LAST
    //! (default) for ASC sort with NANs and NULLS FIRST for DESC sort with NANs.
    //!
    //! Note that it is impossible to use optimize read in order with a floating point key column that can have both NULLs and NANs, you have
    //! to make sure that CHYT knows that at least one of those values will not occur either by marking the column as required, or using the
    //! options below.

    //! Allows CHYT to assume that none of the key columns contain any NULL values.
    //! Used for optimizing read in order if the corresponding option is enabled, see NB clause above.
    bool AssumeNoNullKeys;
    //! Allows CHYT to assume that none of the floating-point key columns contain any NAN values.
    //! Applicable to both required and non-required columns.
    //! Used for optimizing read in order if the corresponding option is enabled, see NB clause above.
    bool AssumeNoNanKeys;

    REGISTER_YSON_STRUCT(TExecutionSettings);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TExecutionSettings)

////////////////////////////////////////////////////////////////////////////////

class TConcatTablesSettings
    : public NYTree::TYsonStruct
{
public:
    //! What to do if a column is missing in some tables (Drop / Throw / ReadAsNull).
    EMissingColumnMode MissingColumnMode;
    //! What to do if types of the column in different tables do not match (Drop / Throw / ReadAsAny).
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

class TPrewhereSettings
    : public NYTree::TYsonStruct
{
public:
    //! If |true|, CHYT would use a separate stage to pre-filter data slices
    //! with a prewhere condition before reading all other columns.
    //! This could significantly reduce the amount of data read from YT if the
    //! total size of the prewhere columns is much smaller than the total size
    //! of all other columns, and the prewhere filters out most of the rows.
    //! However, it could also increase the amount of data read from YT by up
    //! to x2, due to the prewhere column being read twice.
    //! Use with caution.
    bool PrefilterDataSlices;
    //! If |true|, column sizes would be approximated based only on column types.
    //! It is less accurate than using data weight column statistics, but it does
    //! not require any calls to master/data nodes.
    bool UseHeuristicColumnSizes;

    REGISTER_YSON_STRUCT(TPrewhereSettings);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TPrewhereSettings)

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

    NApi::TSerializableMasterReadOptionsPtr CypressReadOptions;
    NApi::TSerializableMasterReadOptionsPtr FetchChunksReadOptions;

    TPrewhereSettingsPtr Prewhere;

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
    NChunkClient::TFetcherConfigPtr ColumnarStatisticsFetcher;
    NChunkClient::TChunkSliceFetcherConfigPtr ChunkSliceFetcher;
    int MaxJobCountForPool;
    i64 MinDataWeightPerThread;

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

DEFINE_REFCOUNTED_TYPE(TMemoryWatchdogConfig)

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

DEFINE_REFCOUNTED_TYPE(TSecurityManagerConfig)

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

DEFINE_REFCOUNTED_TYPE(TGossipConfig)

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

DEFINE_REFCOUNTED_TYPE(TInvokerLivenessCheckerConfig)

////////////////////////////////////////////////////////////////////////////////

class TQueryRegistryConfig
    : public NYTree::TYsonStruct
{
public:
    TDuration ProcessListSnapshotUpdatePeriod;

    bool SaveRunningQueries;
    bool SaveUsers;

    TDuration ClearQueryFinishInfosPeriod;

    REGISTER_YSON_STRUCT(TQueryRegistryConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TQueryRegistryConfig)

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

DEFINE_REFCOUNTED_TYPE(TQuerySamplingConfig)

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

DEFINE_REFCOUNTED_TYPE(TClickHouseTableConfig)

////////////////////////////////////////////////////////////////////////////////

class TUserDefinedSqlObjectsStorageConfig
    : public NYTree::TYsonStruct
{
public:
    bool Enabled;
    NYPath::TYPath Path;
    TDuration UpdatePeriod;
    TDuration ExpireAfterSuccessfulSyncTime;

    REGISTER_YSON_STRUCT(TUserDefinedSqlObjectsStorageConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TUserDefinedSqlObjectsStorageConfig)

////////////////////////////////////////////////////////////////////////////////

class TSystemLogTableExporterConfig
    : public NServer::TArchiveReporterConfig
{
public:
    //! Max unflushed data size in ArchiveReporter.
    i64 MaxInProgressDataSize;

    //! Max bytes to keep in memory in a circular buffer.
    i64 MaxBytesToKeep;
    //! Max rows to keep in memory in a circular buffer.
    i64 MaxRowsToKeep;

    //! Table attributes specified during creation of a new table.
    NYTree::IMapNodePtr CreateTableAttributes;

    REGISTER_YSON_STRUCT(TSystemLogTableExporterConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TSystemLogTableExporterConfig)

////////////////////////////////////////////////////////////////////////////////

class TSystemLogTableExportersConfig
    : public NYTree::TYsonStruct
{
public:
    //! A cypress directory where to store all exporter tables.
    //! A system user (aka yt-clickhouse) should have [read; write; remove; mount] permissions to
    //! this directory and the "use" permission to the corresponding account and tablet cell bundle.
    NYPath::TYPath CypressRootDirectory;

    //! Custom exporter configs for every table by its name.
    THashMap<TString, TSystemLogTableExporterConfigPtr> Tables;
    //! Default exporter config for tables not present in `Tables`.
    TSystemLogTableExporterConfigPtr Default;

    REGISTER_YSON_STRUCT(TSystemLogTableExportersConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TSystemLogTableExportersConfig)

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
    //! Clique size for better profiling.
    int CliqueInstanceCount;

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

    std::optional<int> QueryStickyGroupSize;

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

    THashMap<TString, TString> DatabaseDirectories;

    TShowTablesConfigPtr ShowTables;

    TDuration TotalMemoryTrackerUpdatePeriod;

    TQuerySettingsPtr QuerySettings;

    TQueryRegistryConfigPtr QueryRegistry;

    TQuerySamplingConfigPtr QuerySampling;

    TUserDefinedSqlObjectsStorageConfigPtr UserDefinedSqlObjectsStorage;

    TSystemLogTableExportersConfigPtr SystemLogTableExporters;

    bool EnableHttpHeaderLog;
    NRe2::TRe2Ptr HttpHeaderBlacklist;

    REGISTER_YSON_STRUCT(TYtConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TYtConfig)

////////////////////////////////////////////////////////////////////////////////

class TLauncherConfig
    : public NYTree::TYsonStruct
{
public:
    int Version;

    REGISTER_YSON_STRUCT(TLauncherConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TLauncherConfig)

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

DEFINE_REFCOUNTED_TYPE(TMemoryConfig)

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
    : public NServer::TNativeServerBootstrapConfig
    , public TSingletonsConfig
{
public:
    NCoreDump::TCoreDumperConfigPtr CoreDumper;

    NProfiling::TSolomonExporterConfigPtr SolomonExporter;

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

DEFINE_REFCOUNTED_TYPE(TClickHouseServerBootstrapConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
