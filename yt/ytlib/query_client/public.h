#pragma once

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
typedef TIntrusivePtr<TPlanContext> TPlanContextPtr;

class TPlanFragment;

struct IPrepareCallbacks;
struct ICoordinateCallbacks;
struct IEvaluateCallbacks;

DECLARE_REFCOUNTED_STRUCT(IExecutor)

// TODO(babenko): kill this when refactoring TDataSplit
typedef NChunkClient::NProto::TChunkSpec TDataSplit;

using NVersionedTableClient::ISchemedReaderPtr;
using NVersionedTableClient::IWriterPtr;

using NVersionedTableClient::EValueType;

using NVersionedTableClient::TTableSchema;
using NVersionedTableClient::TKeyColumns;

typedef NVersionedTableClient::TUnversionedRow TRow;
typedef NVersionedTableClient::TUnversionedValue TValue;

typedef NVersionedTableClient::TOwningKey TKey;
typedef std::pair<TKey, TKey> TKeyRange;

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

