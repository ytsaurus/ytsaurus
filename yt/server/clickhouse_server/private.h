#pragma once

#include <yt/core/logging/log.h>
#include <yt/core/profiling/profiler.h>
#include <Common/ProfileEvents.h>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

//! General-purpose logger for our code.
extern const NLogging::TLogger ClickHouseYtLogger;
//! Profiler for our own metrics.
extern const NLogging::TLogger ClickHouseNativeLogger;
//! Logger which is used by ClickHouse native code.
extern const NProfiling::TProfiler ClickHouseYtProfiler;
//! Profiler exporting raw ClickHouse metrics.
extern const NProfiling::TProfiler ClickHouseNativeProfiler;

constexpr int MemoryLimitExceededExitCode = 42;

extern const TString CacheUserName;

////////////////////////////////////////////////////////////////////////////////

namespace NProto {

////////////////////////////////////////////////////////////////////////////////

class TSubquerySpec;

////////////////////////////////////////////////////////////////////////////////

} // namespace NProto

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TEngineConfig);
DECLARE_REFCOUNTED_CLASS(TDictionarySourceYtConfig);
DECLARE_REFCOUNTED_CLASS(TDictionarySourceConfig);
DECLARE_REFCOUNTED_CLASS(TDictionaryConfig);
DECLARE_REFCOUNTED_CLASS(TClickHouseServerBootstrapConfig);
DECLARE_REFCOUNTED_CLASS(TUserConfig);
DECLARE_REFCOUNTED_CLASS(TSubqueryConfig);
DECLARE_REFCOUNTED_CLASS(TSystemLogConfig);
DECLARE_REFCOUNTED_CLASS(TMemoryWatchdogConfig);
DECLARE_REFCOUNTED_CLASS(THealthCheckerConfig);
DECLARE_REFCOUNTED_CLASS(TQueryRegistry);
DECLARE_REFCOUNTED_CLASS(THealthChecker);
DECLARE_REFCOUNTED_STRUCT(TTable);
DECLARE_REFCOUNTED_STRUCT(TQueryContext);
DECLARE_REFCOUNTED_CLASS(TClickHouseConfig);
DECLARE_REFCOUNTED_CLASS(TYtConfig);
DECLARE_REFCOUNTED_STRUCT(IClickHouseHost);
DECLARE_REFCOUNTED_STRUCT(IClickHouseServer);
DECLARE_REFCOUNTED_CLASS(THost);
DECLARE_REFCOUNTED_CLASS(TSecurityManagerConfig);

struct TValue;
class TSubquerySpec;
struct TSubquery;
struct TQueryAnalysisResult;

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

////////////////////////////////////////////////////////////////////////////////

using TQueryId = TGuid;

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EErrorCode,
    ((SubqueryDataWeightLimitExceeded) (2200))
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

class IDatabase;
class Context;
class KeyCondition;
struct SelectQueryInfo;
class Field;
class ColumnsDescription;
class StorageFactory;
class IUsersManager;
class IExternalLoaderConfigRepository;
class IRuntimeComponentsFactory;
struct ProcessListForUserInfo;
struct QueryStatusInfo;
class IAST;
struct ASTTableExpression;

// TODO(max42): get rid of this!
void registerStorageMemory(StorageFactory & factory);

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
extern const Event NetworkErrors;
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

// Why this class is outside of namespace DB? 0_o
class IGeoDictionariesLoader;

////////////////////////////////////////////////////////////////////////////////

namespace CurrentMetrics {

////////////////////////////////////////////////////////////////////////////////

extern const size_t Revision;
extern const size_t VersionInteger;

////////////////////////////////////////////////////////////////////////////////

} // namespace CurrentMetrics

////////////////////////////////////////////////////////////////////////////////
