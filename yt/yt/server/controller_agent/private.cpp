#include "private.h"

#include <library/cpp/yt/string/guid.h>

namespace NYT::NControllerAgent {

////////////////////////////////////////////////////////////////////////////////

void FormatValue(TStringBuilderBase* builder, const TJobMonitoringDescriptor& descriptor, TStringBuf /*format*/)
{
    builder->AppendFormat(
        "%v/%v",
        descriptor.IncarnationId,
        descriptor.Index);
}

TString ToString(const TJobMonitoringDescriptor& descriptor)
{
    return ToStringViaBuilder(descriptor);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent
