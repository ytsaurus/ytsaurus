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
class TOperator;
class TPlanFragment;

} // namespace NProto

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TExpression)
typedef ::NYT::TIntrusivePtr<const TExpression> TConstExpressionPtr;
void Ref(const TExpression* obj) REF_UNREF_DECLARATION_ATTRIBUTES;
void Unref(const TExpression* obj) REF_UNREF_DECLARATION_ATTRIBUTES;

DECLARE_REFCOUNTED_STRUCT(TOperator)
typedef ::NYT::TIntrusivePtr<const TOperator> TConstOperatorPtr;
void Ref(const TOperator* obj) REF_UNREF_DECLARATION_ATTRIBUTES;
void Unref(const TOperator* obj) REF_UNREF_DECLARATION_ATTRIBUTES;

DECLARE_REFCOUNTED_CLASS(TPlanFragment);
typedef ::NYT::TIntrusivePtr<const TPlanFragment> TConstPlanFragmentPtr;
void Ref(const TPlanFragment* obj) REF_UNREF_DECLARATION_ATTRIBUTES;
void Unref(const TPlanFragment* obj) REF_UNREF_DECLARATION_ATTRIBUTES;

struct IPrepareCallbacks;
struct ICoordinateCallbacks;
struct IEvaluateCallbacks;

DECLARE_REFCOUNTED_STRUCT(IExecutor)

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
using NVersionedTableClient::TKeyColumns;

using NTransactionClient::TTimestamp;
using NTransactionClient::NullTimestamp;

typedef NVersionedTableClient::TUnversionedRow TRow;
typedef NVersionedTableClient::TUnversionedRowHeader TRowHeader;
typedef NVersionedTableClient::TUnversionedValue TValue;
typedef NVersionedTableClient::TUnversionedValueData TValueData;
typedef NVersionedTableClient::TUnversionedOwningRow TOwningRow;
typedef NVersionedTableClient::TUnversionedOwningRowBuilder TOwningRowBuilder;

typedef NVersionedTableClient::TOwningKey TKey;
typedef std::pair<TKey, TKey> TKeyRange;

using NVersionedTableClient::TRowBuffer;

using NNodeTrackerClient::TNodeDirectoryPtr;

struct TQueryStatistics;

namespace NProto {

class TQueryStatistics;

} // namespace NProto

TPlanFragmentPtr PreparePlanFragment(
    IPrepareCallbacks* callbacks,
    const Stroka& source,
    i64 inputRowLimit = std::numeric_limits<i64>::max(),
    i64 outputRowLimit = std::numeric_limits<i64>::max(),
    TTimestamp timestamp = NullTimestamp);

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

