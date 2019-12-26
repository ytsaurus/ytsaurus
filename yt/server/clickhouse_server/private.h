#pragma once

#include <yt/core/logging/log.h>
#include <yt/core/profiling/profiler.h>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

extern const NLogging::TLogger ServerLogger;
extern const NLogging::TLogger EngineLogger;
extern const NProfiling::TProfiler ServerProfiler;

constexpr int MemoryLimitExceededExitCode = 42;

////////////////////////////////////////////////////////////////////////////////

namespace NProto {

class TSubquerySpec;

} // namespace NProto

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TEngineConfig);
DECLARE_REFCOUNTED_CLASS(TDictionarySourceYtConfig);
DECLARE_REFCOUNTED_CLASS(TDictionarySourceConfig);
DECLARE_REFCOUNTED_CLASS(TDictionaryConfig);
DECLARE_REFCOUNTED_CLASS(TClickHouseServerBootstrapConfig);
DECLARE_REFCOUNTED_CLASS(TUserConfig);
DECLARE_REFCOUNTED_CLASS(TSubqueryConfig);
DECLARE_REFCOUNTED_CLASS(TMemoryWatchdogConfig);
DECLARE_REFCOUNTED_CLASS(TClickHouseHost);
DECLARE_REFCOUNTED_CLASS(TQueryRegistry);
DECLARE_REFCOUNTED_STRUCT(ISubscriptionManager);

class TClickHouseTableSchema;
struct TClickHouseColumn;
struct TValue;
struct TQueryContext;
class TBootstrap;
class TSubquerySpec;
struct TSubquery;

//! This enum corresponds to DB::ClientInfo::QueryKind.
DEFINE_ENUM(EQueryKind,
    ((NoQuery)(0))
    ((InitialQuery)(1))
    ((SecondaryQuery)(2))
);

//! This enum corresponds to DB::ClientInfo::Interface.
DEFINE_ENUM(EInterface,
    ((TCP)(1))
    ((HTTP)(2))
);

DEFINE_ENUM(EInstanceState,
    ((Active)(0))
    ((Stopped)(1))
);

using TQueryId = TGuid;

////////////////////////////////////////////////////////////////////////////////

#define DECLARE_SHARED_STRUCT(TStruct) \
    struct TStruct; \
    using TStruct ## Ptr = std::shared_ptr<TStruct>

DECLARE_SHARED_STRUCT(IAuthorizationToken);
DECLARE_SHARED_STRUCT(ICliqueAuthorizationManager);
DECLARE_SHARED_STRUCT(IColumnBuilder);
DECLARE_SHARED_STRUCT(IDocument);
DECLARE_SHARED_STRUCT(IQueryContext);
DECLARE_SHARED_STRUCT(ITableReader);
DECLARE_SHARED_STRUCT(TClickHouseTable);

#undef DELCARE_SHARED_STRUCT

} // namespace NYT::NClickHouseServer

namespace DB {

// Forward declaration of all necessary ClickHouse classes and enum values.

////////////////////////////////////////////////////////////////////////////////

namespace ErrorCodes {

/////////////////////////////////////////////////////////////////////////////

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

/////////////////////////////////////////////////////////////////////////////

} // namespace ErrorCodes

/////////////////////////////////////////////////////////////////////////////

class IDatabase;
class Context;
class KeyCondition;
struct SelectQueryInfo;
class Field;
class StorageFactory;
class IUsersManager;
class IExternalLoaderConfigRepository;
class IRuntimeComponentsFactory;
struct ProcessListForUser;
struct QueryStatusInfo;
class IAST;
struct ASTTableExpression;

// TODO(max42): get rid of this!
void registerStorageMemory(StorageFactory & factory);

////////////////////////////////////////////////////////////////////////////////

} // namespace DB

////////////////////////////////////////////////////////////////////////////////

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
