#pragma once

#include <core/misc/common.h>

namespace NYT {
namespace NQueryClient {

////////////////////////////////////////////////////////////////////////////////

class TOperator;
class TExpression;

class TQueryContext;
typedef TIntrusivePtr<TQueryContext> TQueryContextPtr;

class TQueryFragment;

struct TDebugInformation;

class IExecutor;
typedef TIntrusivePtr<IExecutor> IExecutorPtr;

struct IPrepareCallbacks;
struct ICoordinateCallbacks;
struct IEvaluateCallbacks;

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

