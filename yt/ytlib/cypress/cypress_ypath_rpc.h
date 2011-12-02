#pragma once

#include "common.h"
#include "cypress_ypath_rpc.pb.h"

#include "../ytree/ypath_rpc.h"

namespace NYT {
namespace NCypress {

////////////////////////////////////////////////////////////////////////////////

struct TCypressYPathProxy
    : NYTree::TYPathProxy
{
    DEFINE_YPATH_PROXY_METHOD(NProto, Lock);
    DEFINE_YPATH_PROXY_METHOD(NProto, Create);
    DEFINE_YPATH_PROXY_METHOD(NProto, GetId);
};

NYTree::TYPath GetPathFromNodeId(const TNodeId& nodeId);

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypress
} // namespace NYT
