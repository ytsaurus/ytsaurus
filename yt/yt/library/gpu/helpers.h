#pragma once

#include <util/datetime/base.h>

#include <util/generic/fwd.h>

namespace NYT::NGpu {

////////////////////////////////////////////////////////////////////////////////

//! Returns a mapping from GPU UUID to Minor Number using `nvidia-smi -q`.
THashMap<TString, int> GetGpuMinorNumbers(TDuration timeout);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NGpu
