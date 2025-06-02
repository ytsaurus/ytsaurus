#include "private.h"

namespace NYT::NJobProxy {

////////////////////////////////////////////////////////////////////////////////

void TOneShotFlag::Set(bool value) noexcept
{
    YT_VERIFY(!Flag_.has_value());
    Flag_ = value;
}

bool TOneShotFlag::Get() const noexcept
{
    YT_VERIFY(Flag_.has_value());
    return Flag_.value();
}

void TOneShotFlag::operator = (bool value) noexcept
{
    Set(value);
}

TOneShotFlag::operator bool () const noexcept
{
    return Get();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobProxy
