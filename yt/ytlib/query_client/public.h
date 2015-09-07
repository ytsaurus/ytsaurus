#pragma once

#include <ytlib/tablet_client/public.h>
#include <ytlib/table_client/public.h>
#include <ytlib/node_tracker_client/public.h>

// TODO(babenko): kill this when refactoring TDataSplit
namespace NYT { namespace NChunkClient { namespace NProto {
    class TChunkSpec;
} } }


namespace NYT {
namespace NQueryClient {

////////////////////////////////////////////////////////////////////////////////

namespace NProto {

class TExpression;
class TGroupClause;
class TProjectClause;
class TJoinClause;
class TQuery;
class TPlanFragment;
class TQueryStatistics;

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

DECLARE_REFCOUNTED_STRUCT(TQuery);
typedef TIntrusivePtr<const TQuery> TConstQueryPtr;

DECLARE_REFCOUNTED_STRUCT(TPlanFragment);
typedef TIntrusivePtr<const TPlanFragment> TConstPlanFragmentPtr;

struct IPrepareCallbacks;

struct TQueryStatistics;

DECLARE_REFCOUNTED_STRUCT(IFunctionDescriptor)

DECLARE_REFCOUNTED_STRUCT(IAggregateFunctionDescriptor)

DECLARE_REFCOUNTED_STRUCT(ICallingConvention)

DECLARE_REFCOUNTED_STRUCT(IExecutor)

DECLARE_REFCOUNTED_CLASS(TEvaluator)

DECLARE_REFCOUNTED_CLASS(TExecutorConfig)

DECLARE_REFCOUNTED_CLASS(TColumnEvaluator)

DECLARE_REFCOUNTED_CLASS(TColumnEvaluatorCache)

DECLARE_REFCOUNTED_CLASS(TColumnEvaluatorCacheConfig)

DECLARE_REFCOUNTED_STRUCT(IFunctionRegistry)

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

using NTransactionClient::TTimestamp;
using NTransactionClient::NullTimestamp;

using NTableClient::TRowBuffer;
using NTableClient::TRowBufferPtr;

using NNodeTrackerClient::TNodeDirectoryPtr;

typedef NTableClient::TUnversionedRow TRow;
typedef NTableClient::TUnversionedRowHeader TRowHeader;
typedef NTableClient::TUnversionedRowBuilder TRowBuilder;
typedef NTableClient::TUnversionedOwningRow TOwningRow;
typedef NTableClient::TUnversionedOwningRowBuilder TOwningRowBuilder;
typedef NTableClient::TUnversionedValue TValue;
typedef NTableClient::TUnversionedValueData TValueData;
typedef NTableClient::TUnversionedOwningValue TOwningValue;
typedef NTableClient::TOwningKey TKey;

typedef std::pair<TKey, TKey> TKeyRange;
typedef std::pair<TRow, TRow> TRowRange;
typedef std::vector<TRowRange> TRowRanges;

const int MaxRowsPerRead = 1024;
const int MaxRowsPerWrite = 1024;

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

