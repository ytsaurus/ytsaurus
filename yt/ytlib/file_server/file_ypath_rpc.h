#pragma once

#include "common.h"
#include "file_ypath_rpc.pb.h"

#include "../ytree/ypath_rpc.h"

namespace NYT {
namespace NFileServer {

////////////////////////////////////////////////////////////////////////////////

struct TFileYPathProxy
    : NYTree::TYPathProxy
{
    DEFINE_YPATH_PROXY_METHOD(NProto, GetFileChunk);
    DEFINE_YPATH_PROXY_METHOD(NProto, SetFileChunk);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NFileServer
} // namespace NYT
