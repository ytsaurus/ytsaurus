#pragma once

#include "common.h"
#include "table_ypath_rpc.pb.h"

#include "../ytree/ypath_rpc.h"

namespace NYT {
namespace NTableServer {

////////////////////////////////////////////////////////////////////////////////

struct TTableYPathProxy
    : NYTree::TYPathProxy
{
    YPATH_PROXY_METHOD(NProto, AddTableChunks);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableServer
} // namespace NYT
