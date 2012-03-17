#pragma once

#include "public.h"
#include "chunk_ypath.pb.h"

#include <ytlib/object_server/object_ypath_proxy.h>

namespace NYT {
namespace NChunkServer {

////////////////////////////////////////////////////////////////////////////////

struct TChunkYPathProxy
    : public NObjectServer::TObjectYPathProxy
{
    DEFINE_YPATH_PROXY_METHOD(NProto, Fetch);
    DEFINE_YPATH_PROXY_METHOD(NProto, Confirm);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
