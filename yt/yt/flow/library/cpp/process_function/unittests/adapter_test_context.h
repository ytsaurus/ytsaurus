#pragma once

#include <yt/yt/flow/library/cpp/common/computation.h>
#include <yt/yt/flow/library/cpp/common/spec.h>

namespace NYT::NFlow::NTesting {

////////////////////////////////////////////////////////////////////////////////

//! The contexts a worker supplies to a computation, reduced to what an adapter's constructor
//! reads, so that a test can construct the adapter class itself instead of a test harness.

TComputationContextPtr MakeAdapterTestComputationContext(
    const IInvokerPtr& invoker,
    TComputationSpecPtr spec);

TDynamicComputationContextPtr MakeAdapterTestDynamicComputationContext();

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NTesting
