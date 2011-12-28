#pragma once

#include "common.h"
#include "table_ypath.pb.h"

#include "../ytree/ypath_proxy.h"

namespace NYT {
namespace NTableServer {

////////////////////////////////////////////////////////////////////////////////

struct TTableYPathProxy
    : NYTree::TYPathProxy
{
    DEFINE_YPATH_PROXY_METHOD(NProto, GetChunkListId);
    DEFINE_YPATH_PROXY_METHOD(NProto, Fetch);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableServer
} // namespace NYT
