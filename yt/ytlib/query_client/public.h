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

struct IPreparationHooks;

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

