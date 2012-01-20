#pragma once

#include "chunk_list_ypath.pb.h"

#include <ytlib/object_server/object_ypath_proxy.h>

namespace NYT {
namespace NChunkServer {

////////////////////////////////////////////////////////////////////////////////

struct TChunkListYPathProxy
    : public NObjectServer::TObjectYPathProxy
{
    DEFINE_YPATH_PROXY_METHOD(NProto, Attach);
    DEFINE_YPATH_PROXY_METHOD(NProto, Detach);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
