#pragma once

#include "common.h"
#include "ypath_client.h"
#include "ypath_rpc.pb.h"

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

struct TYPathProxy
{
    YPATH_PROXY_METHOD(NProto, Get);
    YPATH_PROXY_METHOD(NProto, Set);
    YPATH_PROXY_METHOD(NProto, Remove);
    YPATH_PROXY_METHOD(NProto, List);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
