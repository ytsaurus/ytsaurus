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

class TQueryContext;
typedef TIntrusivePtr<TQueryContext> TQueryContextPtr;

class TQueryFragment;

struct IPrepareCallbacks;
struct ICoordinateCallbacks;
struct IEvaluateCallbacks;

class IExecutor;
typedef TIntrusivePtr<IExecutor> IExecutorPtr;

typedef NChunkClient::NProto::TChunkSpec TDataSplit;

using NVersionedTableClient::IReaderPtr;
typedef NVersionedTableClient::TChunkWriterPtr TWriterPtr;

using NVersionedTableClient::EColumnType;
using NVersionedTableClient::TTableSchema;
using NVersionedTableClient::TKeyColumns;

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

