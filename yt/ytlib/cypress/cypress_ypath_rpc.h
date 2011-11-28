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
    YPATH_PROXY_METHOD(NProto, Lock);
    YPATH_PROXY_METHOD(NProto, Create);
    YPATH_PROXY_METHOD(NProto, GetId);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypress
} // namespace NYT
