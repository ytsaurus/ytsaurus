#pragma once

#include "common.h"
#include <ytlib/table_server/table_ypath.pb.h>

#include <ytlib/ytree/ypath_proxy.h>

namespace NYT {
namespace NTableServer {

////////////////////////////////////////////////////////////////////////////////

struct TTableYPathProxy
    : NYTree::TYPathProxy
{
    DEFINE_YPATH_PROXY_METHOD(NProto, GetChunkListForUpdate);
    DEFINE_YPATH_PROXY_METHOD(NProto, Fetch);
    DEFINE_YPATH_PROXY_METHOD(NProto, SetSorted);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableServer
} // namespace NYT
