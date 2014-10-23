#pragma once

#include <ytlib/tablet_client/public.h>
#include <ytlib/new_table_client/public.h>
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
class TQuery;
class TPlanFragment;
class TQueryStatistics;

} // namespace NProto

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TExpression)
typedef ::NYT::TIntrusivePtr<const TExpression> TConstExpressionPtr;
void Ref(const TExpression* obj) REF_UNREF_DECLARATION_ATTRIBUTES;
void Unref(const TExpression* obj) REF_UNREF_DECLARATION_ATTRIBUTES;

DECLARE_REFCOUNTED_CLASS(TQuery);
typedef ::NYT::TIntrusivePtr<const TQuery> TConstQueryPtr;
void Ref(const TQuery* obj) REF_UNREF_DECLARATION_ATTRIBUTES;
void Unref(const TQuery* obj) REF_UNREF_DECLARATION_ATTRIBUTES;

DECLARE_REFCOUNTED_CLASS(TPlanFragment);
typedef ::NYT::TIntrusivePtr<const TPlanFragment> TConstPlanFragmentPtr;
void Ref(const TPlanFragment* obj) REF_UNREF_DECLARATION_ATTRIBUTES;
void Unref(const TPlanFragment* obj) REF_UNREF_DECLARATION_ATTRIBUTES;

struct IPrepareCallbacks;
struct IEvaluateCallbacks;

struct TQueryStatistics;

DECLARE_REFCOUNTED_STRUCT(IExecutor)

DECLARE_REFCOUNTED_CLASS(TExecutorConfig)

// TODO(babenko): kill this when refactoring TDataSplit
typedef NChunkClient::NProto::TChunkSpec TDataSplit;
typedef std::vector<TDataSplit> TDataSplits;
typedef std::vector<TDataSplits> TGroupedDataSplits;

using NVersionedTableClient::ISchemafulReader;
using NVersionedTableClient::ISchemafulReaderPtr;
using NVersionedTableClient::ISchemafulWriter;
using NVersionedTableClient::ISchemafulWriterPtr;
using NVersionedTableClient::EValueType;
using NVersionedTableClient::TTableSchema;
using NVersionedTableClient::TColumnSchema;
using NVersionedTableClient::TKeyColumns;

using NTransactionClient::TTimestamp;
using NTransactionClient::NullTimestamp;

using NVersionedTableClient::TRowBuffer;

using NNodeTrackerClient::TNodeDirectoryPtr;

typedef NVersionedTableClient::TUnversionedRow TRow;
typedef NVersionedTableClient::TUnversionedRowHeader TRowHeader;
typedef NVersionedTableClient::TUnversionedValue TValue;
typedef NVersionedTableClient::TUnversionedOwningValue TOwningValue;
typedef NVersionedTableClient::TUnversionedValueData TValueData;
typedef NVersionedTableClient::TUnversionedOwningRow TOwningRow;
typedef NVersionedTableClient::TUnversionedRowBuilder TRowBuilder;
typedef NVersionedTableClient::TUnversionedOwningRowBuilder TOwningRowBuilder;
typedef NVersionedTableClient::TOwningKey TKey;

typedef std::pair<TKey, TKey> TKeyRange;

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

