#pragma once

#include "common.h"
#include <ytlib/file_server/file_ypath.pb.h>

#include <ytlib/misc/configurable.h>
#include <ytlib/ytree/ypath_proxy.h>
#include <ytlib/chunk_server/public.h>

namespace NYT {
namespace NFileServer {

////////////////////////////////////////////////////////////////////////////////

struct TFileYPathProxy
    : NYTree::TYPathProxy
{
    DEFINE_YPATH_PROXY_METHOD(NProto, Fetch);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NFileServer
} // namespace NYT
