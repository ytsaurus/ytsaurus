#pragma once

#include <util/datetime/base.h>
#include <util/generic/function_ref.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

//! Runs |callback| synchronously, reporting unaccounted CPU microseconds on each
//! suspension and on completion, including exception unwinding.
//! Both callbacks must remain alive until this function returns; |consumeMicroseconds|
//! must not throw or switch fibers.
void RunWithCpuAccounting(
    TFunctionRef<void()> callback,
    TFunctionRef<void(ui64)> consumeMicroseconds);

//! Runs |callback| synchronously and returns the CPU time charged by this call.
TDuration RunWithCpuAccounting(TFunctionRef<void()> callback);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
