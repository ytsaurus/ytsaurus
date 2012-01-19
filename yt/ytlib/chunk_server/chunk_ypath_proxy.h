#pragma once

#include "chunk_ypath.pb.h"

#include <ytlib/ytree/ypath_proxy.h>

namespace NYT {
namespace NChunkServer {

////////////////////////////////////////////////////////////////////////////////

struct TChunkYPathProxy
    : NYTree::TYPathProxy
{
    DEFINE_YPATH_PROXY_METHOD(NProto, Fetch);
    DEFINE_YPATH_PROXY_METHOD(NProto, Confirm);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
