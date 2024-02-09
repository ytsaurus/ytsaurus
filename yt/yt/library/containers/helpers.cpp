#include "helpers.h"

#include <yt/yt/core/misc/string_builder.h>

namespace NYT::NContainers {

////////////////////////////////////////////////////////////////////////////////

void FormatValue(TStringBuilderBase* builder, const TDevice& device, TStringBuf /* format */)
{
    builder->AppendString(device.DeviceName);
}

TString ToString(const TDevice& device)
{
    return ToStringViaBuilder(device);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NContainers
