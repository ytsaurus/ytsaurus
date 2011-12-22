#pragma once

#include "common.h"
#include "file_ypath.pb.h"

#include "../ytree/ypath_proxy.h"

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
