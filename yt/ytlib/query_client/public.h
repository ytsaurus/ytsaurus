#pragma once

#include <ytlib/tablet_client/public.h>
#include <ytlib/new_table_client/public.h>

// TODO(babenko): kill this when refactoring TDataSplit
namespace NYT { namespace NChunkClient { namespace NProto {
    class TChunkSpec;
} } }


namespace NYT {
namespace NQueryClient {

////////////////////////////////////////////////////////////////////////////////

namespace NProto {

class TPlanFragment;
class TExpression;
class TOperator;

} // namespace NProto

////////////////////////////////////////////////////////////////////////////////

class TOperator;
class TExpression;

class TPlanContext;
DECLARE_REFCOUNTED_CLASS(TPlanContext)

class TPlanFragment;

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

typedef NVersionedTableClient::TOwningKey TKey;
typedef std::pair<TKey, TKey> TKeyRange;

using NVersionedTableClient::TRowBuffer;

struct TQueryStatistics
{
    i64 RowsRead = 0;
    i64 RowsWritten = 0;
    TDuration SyncTime;
    TDuration AsyncTime;
    bool IncompleteInput = false;
    bool IncompleteOutput = false;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

