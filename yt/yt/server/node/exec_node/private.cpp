#include "private.h"

#include <library/cpp/yt/string/format.h>
#include <library/cpp/yt/string/guid.h>

namespace NYT::NExecNode {

////////////////////////////////////////////////////////////////////////////////

void FormatValue(
    TStringBuilderBase* builder,
    const TControllerAgentDescriptor& controllerAgentDescriptor,
    TStringBuf /*format*/)
{
    builder->AppendFormat(
        "{Address: %v, IncarnationId: %v}",
        controllerAgentDescriptor.Address,
        controllerAgentDescriptor.IncarnationId);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
