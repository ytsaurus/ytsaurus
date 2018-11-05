#pragma once

#include <yt/client/query_client/public.h>

#include <yt/ytlib/node_tracker_client/public.h>

#include <yt/ytlib/table_client/public.h>

#include <yt/ytlib/tablet_client/public.h>

// TODO(babenko): kill this when refactoring TDataSplit
namespace NYT { namespace NChunkClient { namespace NProto {
    class TChunkSpec;
} } }

// TODO(lukyan): kill after refactoring protobuf in client
namespace NYT { namespace NTableClient { namespace NProto {
    class TTabletInfo;
} } }

namespace NYT {
namespace NQueryClient {

////////////////////////////////////////////////////////////////////////////////

namespace NProto {

class TColumnDescriptor;
class TExpression;
class TGroupClause;
class TProjectClause;
class TJoinClause;
class TQuery;
class TQueryOptions;
class TDataRanges;
class TTableMountInfo;

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

DECLARE_REFCOUNTED_STRUCT(IAggregateFunctionDescriptor)

DECLARE_REFCOUNTED_STRUCT(ICallingConvention)

DECLARE_REFCOUNTED_STRUCT(IExecutor)

DECLARE_REFCOUNTED_STRUCT(ISubexecutor)

DECLARE_REFCOUNTED_CLASS(TEvaluator)

DECLARE_REFCOUNTED_CLASS(TExecutorConfig)

DECLARE_REFCOUNTED_CLASS(TColumnEvaluator)

DECLARE_REFCOUNTED_CLASS(TColumnEvaluatorCache)

DECLARE_REFCOUNTED_CLASS(TColumnEvaluatorCacheConfig)

DECLARE_REFCOUNTED_STRUCT(IFunctionRegistry)

DECLARE_REFCOUNTED_STRUCT(IFunctionCodegen)

DECLARE_REFCOUNTED_STRUCT(IAggregateCodegen)

DECLARE_REFCOUNTED_STRUCT(TExternalCGInfo)
typedef TIntrusivePtr<const TExternalCGInfo> TConstExternalCGInfoPtr;

DECLARE_REFCOUNTED_STRUCT(TTypeInferrerMap)
typedef TIntrusivePtr<const TTypeInferrerMap> TConstTypeInferrerMapPtr;

DECLARE_REFCOUNTED_STRUCT(TRangeExtractorMap)
typedef TIntrusivePtr<const TRangeExtractorMap> TConstRangeExtractorMapPtr;

DECLARE_REFCOUNTED_STRUCT(TFunctionProfilerMap)
typedef TIntrusivePtr<const TFunctionProfilerMap> TConstFunctionProfilerMapPtr;

DECLARE_REFCOUNTED_STRUCT(TAggregateProfilerMap)
typedef TIntrusivePtr<const TAggregateProfilerMap> TConstAggregateProfilerMapPtr;

extern const TConstTypeInferrerMapPtr BuiltinTypeInferrersMap;
extern const TConstRangeExtractorMapPtr BuiltinRangeExtractorMap;
extern const TConstFunctionProfilerMapPtr BuiltinFunctionProfilers;
extern const TConstAggregateProfilerMapPtr BuiltinAggregateProfilers;

DECLARE_REFCOUNTED_STRUCT(IFunctionRegistry)
DECLARE_REFCOUNTED_STRUCT(ITypeInferrer)

DECLARE_REFCOUNTED_CLASS(TFunctionImplCache)

// TODO(babenko): kill this when refactoring TDataSplit
typedef NChunkClient::NProto::TChunkSpec TDataSplit;
typedef std::vector<TDataSplit> TDataSplits;

using NTableClient::ISchemafulReader;
using NTableClient::ISchemafulReaderPtr;
using NTableClient::ISchemafulWriter;
using NTableClient::ISchemafulWriterPtr;
using NTableClient::EValueType;
using NTableClient::TTableSchema;
using NTableClient::TColumnSchema;
using NTableClient::TKeyColumns;
using NTableClient::TColumnFilter;
using NTableClient::TRowRange;

using NTransactionClient::TTimestamp;
using NTransactionClient::NullTimestamp;

using NTableClient::TRowBuffer;
using NTableClient::TRowBufferPtr;

using NNodeTrackerClient::TNodeDirectoryPtr;

struct TBaseColumn;
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
typedef NTableClient::TOwningKey TOwningKey;

typedef std::pair<TOwningKey, TOwningKey> TKeyRange;
typedef std::pair<TMutableRow, TMutableRow> TMutableRowRange;
typedef std::vector<TRowRange> TRowRanges;
typedef std::vector<TMutableRowRange> TMutableRowRanges;

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

