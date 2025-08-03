#include "block_device.h"

namespace NYT::NNbd {

////////////////////////////////////////////////////////////////////////////////

const TError& TBaseBlockDevice::GetError() const
{
    return Error_;
}

void TBaseBlockDevice::SetError(TError error)
{
    Error_ = std::move(error);
}

void TBaseBlockDevice::OnShouldStopUsingDevice() const
{
    ShouldStopUsingDevice_.Fire();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNbd
