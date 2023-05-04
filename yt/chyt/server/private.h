#pragma once

#include <yt/yt/core/logging/log.h>

#include <yt/yt/library/profiling/sensor.h>

#include <yt/yt/library/clickhouse_functions/public.h>

#include <Common/ProfileEvents.h>
#include <Common/COW.h>

#include <Interpreters/Context_fwd.h>

#include <Parsers/IAST_fwd.h>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

//! General-purpose logger for our code.
inline const NLogging::TLogger ClickHouseYtLogger("ClickHouseYT");
//! Logger which is used by ClickHouse native code.
inline const NLogging::TLogger ClickHouseNativeLogger("ClickHouseNative");
//! Root profiler for all metrics.
inline const NProfiling::TProfiler ClickHouseProfiler("/clickhouse");
//! Profiler for our own metrics.
inline const NProfiling::TProfiler ClickHouseYtProfiler = ClickHouseProfiler.WithPrefix("/yt");
//! Profiler exporting raw ClickHouse metrics.
inline const NProfiling::TProfiler ClickHouseNativeProfiler = ClickHouseProfiler.WithPrefix("/native");

// Set 0 to not see graceful exits in failed jobs.
constexpr int GracefulInterruptionExitCode = 0;
constexpr int MemoryLimitExceededExitCode = 42;
constexpr int InterruptionTimedOutExitCode = 43;
constexpr int InvokerLivenessCheckerExitCode = 44;

constexpr int SentinelMaxStringLength = 50;

constexpr int YqlOperationIdLength = 24;

extern const TString CacheUserName;
extern const TString InternalRemoteUserName;
extern const std::vector<TString> TableAttributesToFetch;

////////////////////////////////////////////////////////////////////////////////

namespace NProto {

////////////////////////////////////////////////////////////////////////////////

class TSubquerySpec;
class TChytRequest;

////////////////////////////////////////////////////////////////////////////////

} // namespace NProto

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TEngineConfig);
DECLARE_REFCOUNTED_CLASS(TDictionarySourceYtConfig);
DECLARE_REFCOUNTED_CLASS(TDictionarySourceConfig);
DECLARE_REFCOUNTED_CLASS(TDictionaryConfig);
DECLARE_REFCOUNTED_CLASS(TClickHouseServerBootstrapConfig);
DECLARE_REFCOUNTED_CLASS(TUserConfig);
DECLARE_REFCOUNTED_CLASS(TShowTablesConfig);
DECLARE_REFCOUNTED_CLASS(TSubqueryConfig);
DECLARE_REFCOUNTED_CLASS(TSystemLogConfig);
DECLARE_REFCOUNTED_CLASS(TMemoryWatchdogConfig);
DECLARE_REFCOUNTED_CLASS(THealthCheckerConfig);
DECLARE_REFCOUNTED_CLASS(TQueryRegistry);
DECLARE_REFCOUNTED_CLASS(THealthChecker);
DECLARE_REFCOUNTED_STRUCT(TTable);
DECLARE_REFCOUNTED_STRUCT(TQueryContext);
DECLARE_REFCOUNTED_STRUCT(TStorageContext);
DECLARE_REFCOUNTED_CLASS(TClickHouseConfig);
DECLARE_REFCOUNTED_CLASS(TYtConfig);
DECLARE_REFCOUNTED_STRUCT(IClickHouseHost);
DECLARE_REFCOUNTED_STRUCT(IClickHouseServer);
DECLARE_REFCOUNTED_CLASS(TQuerySettings);
DECLARE_REFCOUNTED_CLASS(THost);
DECLARE_REFCOUNTED_CLASS(TSecurityManagerConfig);
DECLARE_REFCOUNTED_CLASS(TLauncherConfig);
DECLARE_REFCOUNTED_CLASS(TMemoryConfig);
DECLARE_REFCOUNTED_CLASS(TMemoryWatchdog);
DECLARE_REFCOUNTED_CLASS(TCompositeSettings);
DECLARE_REFCOUNTED_CLASS(TDynamicTableSettings);
DECLARE_REFCOUNTED_CLASS(TTestingSettings);
DECLARE_REFCOUNTED_CLASS(TExecutionSettings);
DECLARE_REFCOUNTED_CLASS(TClickHouseIndex);
DECLARE_REFCOUNTED_STRUCT(IQueryStatisticsReporter);
DECLARE_REFCOUNTED_CLASS(TQueryStatisticsReporterConfig);
DECLARE_REFCOUNTED_CLASS(TGossipConfig);
DECLARE_REFCOUNTED_CLASS(TInvokerLivenessCheckerConfig);
DECLARE_REFCOUNTED_CLASS(TQueryRegistryConfig);
DECLARE_REFCOUNTED_CLASS(TQuerySamplingConfig);
DECLARE_REFCOUNTED_CLASS(TClickHouseTableConfig);
DECLARE_REFCOUNTED_CLASS(TQueryLogConfig);
DECLARE_REFCOUNTED_CLASS(TSerializableSpanContext);
DECLARE_REFCOUNTED_CLASS(TSecondaryQueryHeader);
DECLARE_REFCOUNTED_CLASS(TInvokerLivenessChecker);
DECLARE_REFCOUNTED_CLASS(TConcatTablesSettings);
DECLARE_REFCOUNTED_CLASS(TCachingSettings);
DECLARE_REFCOUNTED_CLASS(TListDirSettings);

struct TValue;
class TSubquerySpec;
struct TSubquery;
struct TQueryAnalysisResult;
class TClickHouseIndexBuilder;

struct IStorageDistributor;
using IStorageDistributorPtr = std::shared_ptr<IStorageDistributor>;

////////////////////////////////////////////////////////////////////////////////

//! This enum corresponds to DB::ClientInfo::QueryKind.
DEFINE_ENUM(EQueryKind,
    ((NoQuery)        (0))
    ((InitialQuery)   (1))
    ((SecondaryQuery) (2))
);

//! This enum corresponds to DB::ClientInfo::Interface.
DEFINE_ENUM(EInterface,
    ((TCP)            (1))
    ((HTTP)           (2))
);

DEFINE_ENUM(EInstanceState,
    ((Active)         (0))
    ((Stopped)        (1))
);

DEFINE_ENUM(EQueryPhase,
    ((Start)          (0))
    ((Preparation)    (1))
    ((Execution)      (2))
    ((Finish)         (3))
);

DEFINE_ENUM(EDeducedStatementMode,
    ((In)             (0))
    ((DNF)            (1))
);

DEFINE_ENUM(EJoinPolicy,
    // Always execute join localy.
    // Both left and right tables will be read according to SelectPolicy.
    ((Local)             (0))
    // Distribute join in Initial Queries, but execute it localy in Secondary Queries
    // to avoid exponential number of the secondary queries.
    ((DistributeInitial) (1))
    // Always distribute join.
    ((Distribute)        (2))
);

DEFINE_ENUM(ESelectPolicy,
    // Always read tables on local node only.
    ((Local)             (0))
    // Distribute select in initial queries, but read tables localy in secondary queries.
    ((DistributeInitial) (1))
    // Always distribute select queries.
    ((Distribute)        (2))
);

DEFINE_ENUM(EDistributedInsertStage,
    // Never distribute.
    ((None)               (0))
    // Always distribute, even if aggregation is not completed.
    ((WithMergeableState) (1))
    // Distribute queries if aggregation can be done localy.
    // Limit/OrderBy statements are processed localy, but the whole result is not sorted
    // and can contain up to (instance count) * (N limit) rows.
    ((AfterAggregation)   (2))
    // Distribute only when query can be fully processed on workers.
    ((Complete)           (3))
);

// For concatYtTables.
DEFINE_ENUM(EMissingColumnMode,
    ((Throw)      (0))
    ((Drop)       (1))
    ((ReadAsNull) (2))
);

// For concatYtTables.
DEFINE_ENUM(ETypeMismatchMode,
    ((Throw)     (0))
    ((Drop)      (1))
    ((ReadAsAny) (2))
);

DEFINE_ENUM(EInvalidateCacheMode,
    ((None)  (0))
    ((Local) (1))
    ((Async) (2))
    ((Sync)  (3))
);

////////////////////////////////////////////////////////////////////////////////

using TQueryId = TGuid;

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_ERROR_ENUM(
    ((SubqueryDataWeightLimitExceeded) (2900))
);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer

namespace DB {

////////////////////////////////////////////////////////////////////////////////

// Forward declaration of all necessary ClickHouse classes and enum values.

namespace ErrorCodes {

////////////////////////////////////////////////////////////////////////////////

extern const int CANNOT_SELECT;
extern const int INCOMPATIBLE_COLUMNS;
extern const int LOGICAL_ERROR;
extern const int NOT_IMPLEMENTED;
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
extern const int TOO_FEW_ARGUMENTS_FOR_FUNCTION;
extern const int TOO_MANY_ARGUMENTS_FOR_FUNCTION;
extern const int UNKNOWN_TYPE;
extern const int IP_ADDRESS_NOT_ALLOWED;
extern const int UNKNOWN_USER;
extern const int ILLEGAL_COLUMN;
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
extern const int NO_SUCH_COLUMN_IN_TABLE;

////////////////////////////////////////////////////////////////////////////////

} // namespace ErrorCodes

////////////////////////////////////////////////////////////////////////////////

class AccessControl;
class ColumnsDescription;
class Field;
class IDatabase;
class KeyCondition;
class NamesAndTypesList;
class StorageFactory;

struct ASTTableExpression;
struct ProcessListForUserInfo;
struct QueryStatusInfo;
struct SelectQueryInfo;

class IDataType;
using DataTypePtr = std::shared_ptr<const IDataType>;

class IColumn;
using ColumnPtr = COW<IColumn>::Ptr;
using MutableColumnPtr = COW<IColumn>::MutablePtr;


// TODO(max42): get rid of this!
void registerStorageMemory(StorageFactory & factory);
void registerStorageBuffer(StorageFactory & factory);

////////////////////////////////////////////////////////////////////////////////

} // namespace DB

////////////////////////////////////////////////////////////////////////////////

namespace ProfileEvents {

////////////////////////////////////////////////////////////////////////////////

extern const Event Query;
extern const Event SelectQuery;
extern const Event InsertQuery;
extern const Event InsertedRows;
extern const Event InsertedBytes;
extern const Event ContextLock;
extern const Event RealTimeMicroseconds;
extern const Event UserTimeMicroseconds;
extern const Event SystemTimeMicroseconds;
extern const Event SoftPageFaults;
extern const Event HardPageFaults;
extern const Event OSIOWaitMicroseconds;
extern const Event OSCPUWaitMicroseconds;
extern const Event OSCPUVirtualTimeMicroseconds;
extern const Event OSReadChars;
extern const Event OSWriteChars;
extern const Event OSReadBytes;
extern const Event OSWriteBytes;


////////////////////////////////////////////////////////////////////////////////

} // namespace ProfileEvents

////////////////////////////////////////////////////////////////////////////////

namespace CurrentMetrics {

////////////////////////////////////////////////////////////////////////////////

extern const size_t Revision;
extern const size_t VersionInteger;
extern const size_t MemoryTracking;

////////////////////////////////////////////////////////////////////////////////

} // namespace CurrentMetrics

////////////////////////////////////////////////////////////////////////////////
