#pragma once

#include <ytlib/new_table_client/public.h>

namespace NYT { namespace NChunkClient { namespace NProto {
    class TChunkSpec;
} } }

namespace NYT {
namespace NQueryClient {

////////////////////////////////////////////////////////////////////////////////

class TOperator;
class TExpression;

class TPlanContext;
typedef TIntrusivePtr<TPlanContext> TPlanContextPtr;

class TPlanFragment;

struct IPrepareCallbacks;
struct ICoordinateCallbacks;
struct IEvaluateCallbacks;

class IExecutor;
typedef TIntrusivePtr<IExecutor> IExecutorPtr;

typedef NChunkClient::NProto::TChunkSpec TDataSplit;

using NVersionedTableClient::IReaderPtr;
using NVersionedTableClient::IWriterPtr;

using NVersionedTableClient::EValueType;

using NVersionedTableClient::TTableSchema;
using NVersionedTableClient::TKeyColumns;

// TODO(sandello): Change me after migrating to TUnversionedValue in query_client.
typedef NVersionedTableClient::TVersionedRow TRow;
typedef NVersionedTableClient::TVersionedValue TValue;

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

