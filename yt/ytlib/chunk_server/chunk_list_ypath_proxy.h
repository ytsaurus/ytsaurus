#pragma once

#include <ytlib/chunk_server/chunk_list_ypath.pb.h>

#include <ytlib/object_server/object_ypath_proxy.h>

namespace NYT {
namespace NChunkServer {

////////////////////////////////////////////////////////////////////////////////

struct TChunkListYPathProxy
    : public NObjectServer::TObjectYPathProxy
{
    DEFINE_YPATH_PROXY_METHOD(NProto, Attach);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
