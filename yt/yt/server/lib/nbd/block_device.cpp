#include "block_device.h"

namespace NYT::NNbd {

////////////////////////////////////////////////////////////////////////////////

//! Get the latest error set for device.
const TError& TBaseBlockDevice::GetError() const
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    auto guard = TGuard(Lock_);
    return Error_;
}

//! Set an error (error.IsOK() == false) for device.
void TBaseBlockDevice::SetError(TError error)
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    if (error.IsOK()) {
        THROW_ERROR_EXCEPTION("Unexpected error")
            << TErrorAttribute("error", error)
            << TErrorAttribute("debug_info", DebugString());
    }

    auto guard = TGuard(Lock_);
    Error_ = std::move(error);
}

void TBaseBlockDevice::OnShouldStopUsingDevice() const
{
    ShouldStopUsingDevice_.Fire();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNbd
