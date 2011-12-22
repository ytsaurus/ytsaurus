#pragma once

#include "common.h"
#include "ypath_client.h"
#include "ypath.pb.h"

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

struct TYPathProxy
{
    DEFINE_YPATH_PROXY_METHOD(NProto, Get);
    DEFINE_YPATH_PROXY_METHOD(NProto, GetNode);
    DEFINE_YPATH_PROXY_METHOD(NProto, Set);
    DEFINE_YPATH_PROXY_METHOD(NProto, SetNode);
    DEFINE_YPATH_PROXY_METHOD(NProto, Remove);
    DEFINE_YPATH_PROXY_METHOD(NProto, List);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
