#pragma once

#include <yt/core/logging/log.h>
#include <yt/core/profiling/profiler.h>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

extern const NLogging::TLogger ServerLogger;
extern const NProfiling::TProfiler ServerProfiler;

////////////////////////////////////////////////////////////////////////////////

namespace NProto {

class TReadJobSpec;

} // namespace NProto

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TEngineConfig);
DECLARE_REFCOUNTED_CLASS(TClickHouseServerBootstrapConfig);
DECLARE_REFCOUNTED_CLASS(TUserConfig);
DECLARE_REFCOUNTED_CLASS(TClickHouseHost);
DECLARE_REFCOUNTED_STRUCT(ISubscriptionManager);

class TClickHouseTableSchema;
struct TColumn;
struct TValue;
struct TQueryContext;
class TBootstrap;
class TReadJobSpec;

//! This enum corresponds to DB::ClientInfo::QueryKind.
DEFINE_ENUM(EQueryKind,
    ((NoQuery)(0))
    ((InitialQuery)(1))
    ((SecondaryQuery)(2))
);

using TQueryId = TGuid;

////////////////////////////////////////////////////////////////////////////////

#define DECLARE_SHARED_STRUCT(TStruct) \
    struct TStruct; \
    using TStruct ## Ptr = std::shared_ptr<TStruct>

DECLARE_SHARED_STRUCT(IAuthorizationToken);
DECLARE_SHARED_STRUCT(ICliqueAuthorizationManager);
DECLARE_SHARED_STRUCT(IColumnBuilder);
DECLARE_SHARED_STRUCT(ICoordinationService);
DECLARE_SHARED_STRUCT(IDocument);
DECLARE_SHARED_STRUCT(ILogger);
DECLARE_SHARED_STRUCT(IQueryContext);
DECLARE_SHARED_STRUCT(ITableReader);
DECLARE_SHARED_STRUCT(TTable);

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
extern const int NO_ELEMENTS_IN_CONFIG;
extern const int NOT_IMPLEMENTED;
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
extern const int TOO_LESS_ARGUMENTS_FOR_FUNCTION;
extern const int TOO_MANY_ARGUMENTS_FOR_FUNCTION;
extern const int UNKNOWN_TYPE;

/////////////////////////////////////////////////////////////////////////////

} // namespace ErrorCodes

/////////////////////////////////////////////////////////////////////////////

class IDatabase;
class Context;
class KeyCondition;
struct SelectQueryInfo;
class Field;
class StorageFactory;

// TODO(max42): get rid of this!
void registerStorageMemory(StorageFactory & factory);

////////////////////////////////////////////////////////////////////////////////

} // namespace DB
