#pragma once

#include "provider.h"
#include "public.h"
#include "range.h"

#include <yt/yt/client/cache/cache.h>
#include <yt/yt/flow/library/cpp/common/public.h>

#include <yt/yt/client/ypath/rich.h>

#include <yt/yt/client/api/client.h>
#include <yt/yt/client/api/table_client.h>
#include <yt/yt/client/api/table_reader.h>

#include <yt/yt/client/table_client/helpers.h>

#include <yt/yt/core/yson/string.h>
#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EFetchType,
    //! Experimental for now, TableReader is more stable.
    (SelectRows)
    (TableReader)
);

struct TTableFetcherSpec
    : public NYTree::TYsonStruct
{
    NYPath::TRichYPath TablePath;
    std::optional<THashSet<std::string>> ValueColumns;
    i64 Attempts{};
    TDuration RetryTimeout;
    EFetchType FetchType{};

    REGISTER_YSON_STRUCT(TTableFetcherSpec);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TTableFetcherSpec);

////////////////////////////////////////////////////////////////////////////////

//! Result of building a parameterized SELECT query.
//! Contains the query string with named placeholders and the corresponding YSON map of placeholder values.
struct TParameterizedSelectRowsQuery
{
    //! Query string with named placeholders like {lower_0}, {upper_1}, {row_limit}.
    std::string Query;
    //! YSON map with placeholder values to be passed via TSelectRowsOptions::PlaceholderValues.
    NYson::TYsonString PlaceholderValues;
};

//! Builds a parameterized SELECT query with named placeholders instead of inlined values.
//! The returned struct contains both the query string and the YSON placeholder values map.
TParameterizedSelectRowsQuery BuildParameterizedSelectRowsQuery(
    const std::string& tablePath,
    const NTableClient::TTableSchemaPtr& schema,
    const TServiceLogRangePtr& range,
    i64 rowLimit);

////////////////////////////////////////////////////////////////////////////////

IServiceLogRowsProviderPtr CreateTableFetcher(
    NLogging::TLogger logger,
    NProfiling::TProfiler profiler,
    IStatusProfilerPtr statusProfiler,
    NClient::NCache::IClientsCachePtr clientsCache,
    const TTableFetcherSpecPtr& spec);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
