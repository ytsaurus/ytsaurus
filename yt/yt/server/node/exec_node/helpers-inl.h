#ifndef HELPERS_INL_H_
#error "Direct inclusion of this file is not allowed, include helpers.h"
// For the sake of sane code completion.
#include "helpers.h"
#endif

namespace NYT::NExecNode {

////////////////////////////////////////////////////////////////////////////////

void SetNodeInfoToRequest(
    NNodeTrackerClient::TNodeId nodeId,
    const NNodeTrackerClient::TNodeDescriptor& nodeDescriptor,
    const auto& request)
{
    request->set_node_id(NYT::ToProto<ui32>(nodeId));
    ToProto(request->mutable_node_descriptor(), nodeDescriptor);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
