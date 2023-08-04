#pragma once

#include <yt/yt/client/query_client/public.h>

#include <yt/yt/client/transaction_client/public.h>

#include <yt/yt/client/table_client/public.h>

#include <yt/yt/core/ypath/public.h>

namespace NYT::NQueryClient {

using NTransactionClient::TTimestamp;

using NTableClient::TRowRange;

using TReadSessionId = TGuid;

struct TDataSplit;

////////////////////////////////////////////////////////////////////////////////

namespace NProto {

class TColumnDescriptor;
class TExpression;
class TGroupClause;
class TProjectClause;
class TJoinClause;
class TQuery;
class TQueryOptions;
class TDataSource;

} // namespace NProto

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TExpression)
typedef TIntrusivePtr<const TExpression> TConstExpressionPtr;

DECLARE_REFCOUNTED_STRUCT(TFunctionExpression)
typedef TIntrusivePtr<const TFunctionExpression> TConstFunctionExpressionPtr;

DECLARE_REFCOUNTED_STRUCT(TJoinClause)
typedef TIntrusivePtr<const TJoinClause> TConstJoinClausePtr;

DECLARE_REFCOUNTED_STRUCT(TGroupClause)
typedef TIntrusivePtr<const TGroupClause> TConstGroupClausePtr;

DECLARE_REFCOUNTED_STRUCT(TOrderClause)
typedef TIntrusivePtr<const TOrderClause> TConstOrderClausePtr;

DECLARE_REFCOUNTED_STRUCT(TProjectClause)
typedef TIntrusivePtr<const TProjectClause> TConstProjectClausePtr;

DECLARE_REFCOUNTED_STRUCT(TBaseQuery)
typedef TIntrusivePtr<const TBaseQuery> TConstBaseQueryPtr;

DECLARE_REFCOUNTED_STRUCT(TFrontQuery)
typedef TIntrusivePtr<const TFrontQuery> TConstFrontQueryPtr;

DECLARE_REFCOUNTED_STRUCT(TQuery)
typedef TIntrusivePtr<const TQuery> TConstQueryPtr;

struct IPrepareCallbacks;

struct TQueryStatistics;

struct TQueryOptions;

DECLARE_REFCOUNTED_STRUCT(IAggregateFunctionDescriptor)

DECLARE_REFCOUNTED_STRUCT(ICallingConvention)

DECLARE_REFCOUNTED_STRUCT(IExecutor)

DECLARE_REFCOUNTED_STRUCT(IEvaluator)

DECLARE_REFCOUNTED_CLASS(TExecutorConfig)

DECLARE_REFCOUNTED_CLASS(TColumnEvaluator)

DECLARE_REFCOUNTED_STRUCT(IColumnEvaluatorCache)

DECLARE_REFCOUNTED_CLASS(TColumnEvaluatorCacheConfig)
DECLARE_REFCOUNTED_CLASS(TColumnEvaluatorCacheDynamicConfig)

DECLARE_REFCOUNTED_STRUCT(TExternalCGInfo)
typedef TIntrusivePtr<const TExternalCGInfo> TConstExternalCGInfoPtr;

DECLARE_REFCOUNTED_STRUCT(TTypeInferrerMap)
typedef TIntrusivePtr<const TTypeInferrerMap> TConstTypeInferrerMapPtr;

const TConstTypeInferrerMapPtr GetBuiltinTypeInferrers();

DECLARE_REFCOUNTED_STRUCT(IFunctionRegistry)
DECLARE_REFCOUNTED_STRUCT(ITypeInferrer)

DECLARE_REFCOUNTED_CLASS(TFunctionImplCache)

using NTableClient::ISchemafulUnversionedReader;
using NTableClient::ISchemafulUnversionedReaderPtr;
using NTableClient::ISchemalessUnversionedReader;
using NTableClient::ISchemalessUnversionedReaderPtr;
using NTableClient::IUnversionedRowsetWriter;
using NTableClient::IUnversionedRowsetWriterPtr;
using NTableClient::EValueType;
using NTableClient::TTableSchema;
using NTableClient::TTableSchemaPtr;
using NTableClient::TColumnSchema;
using NTableClient::TKeyColumns;
using NTableClient::TColumnFilter;
using NTableClient::TRowRange;

using NTransactionClient::TTimestamp;
using NTransactionClient::NullTimestamp;

using NTableClient::TRowBuffer;
using NTableClient::TRowBufferPtr;

typedef std::vector<NTableClient::TColumnSchema> TSchemaColumns;

typedef NTableClient::TUnversionedRow TRow;
typedef NTableClient::TMutableUnversionedRow TMutableRow;
typedef NTableClient::TUnversionedRowHeader TRowHeader;
typedef NTableClient::TUnversionedRowBuilder TRowBuilder;
typedef NTableClient::TUnversionedOwningRow TOwningRow;
typedef NTableClient::TUnversionedOwningRowBuilder TOwningRowBuilder;
typedef NTableClient::TUnversionedValue TValue;
typedef NTableClient::TUnversionedValueData TValueData;
typedef NTableClient::TUnversionedOwningValue TOwningValue;
typedef NTableClient::TLegacyOwningKey TLegacyOwningKey;

typedef std::pair<TLegacyOwningKey, TLegacyOwningKey> TKeyRange;
typedef std::pair<TMutableRow, TMutableRow> TMutableRowRange;
typedef std::vector<TRowRange> TRowRanges;
typedef std::vector<TMutableRowRange> TMutableRowRanges;

////////////////////////////////////////////////////////////////////////////////

extern const NYPath::TYPath QueryPoolsPath;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient

