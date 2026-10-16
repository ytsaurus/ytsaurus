#include "source_computation.h"

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

TProcessFunctionSourceComputation::TProcessFunctionSourceComputation(
    TComputationContextPtr context,
    TDynamicComputationContextPtr dynamicContext)
    : TProcessFunctionComputationBase(std::move(context), std::move(dynamicContext))
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
