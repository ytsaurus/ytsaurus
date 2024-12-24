#include "helpers.h"

#include <yt/yt/core/misc/string_builder.h>

namespace NYT::NContainers {

////////////////////////////////////////////////////////////////////////////////

void FormatValue(TStringBuilderBase* builder, const TDevice& device, TStringBuf /*spec*/)
{
    if (device.Access) {
        builder->AppendFormat("{Device: %v, Access: %v}", device.DeviceName, device.Access);
    } else {
        builder->AppendString(device.DeviceName);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NContainers
