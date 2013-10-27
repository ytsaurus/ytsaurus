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
typedef NVersionedTableClient::IReaderPtr IReaderPtr; 

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

