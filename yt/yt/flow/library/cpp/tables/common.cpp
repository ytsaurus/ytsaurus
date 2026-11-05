#include "common.h"

#include <yt/yt/flow/library/cpp/common/spec.h>

namespace NYT::NFlow::NTables {

////////////////////////////////////////////////////////////////////////////////

TSelectLimiter::TSelectLimiter(TDynamicTableRequestSpecPtr spec)
    : Spec_(spec)
    , Current_(Spec_->SelectMinLimit)
{ }

i64 TSelectLimiter::Get()
{
    auto value = Current_;
    Current_ = std::min<i64>(Current_ * Spec_->SelectLimitMultiplier, Spec_->SelectMaxLimit);
    return value;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NTables
