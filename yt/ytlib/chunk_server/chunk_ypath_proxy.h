#pragma once

#include "public.h"
#include <ytlib/chunk_server/chunk_ypath.pb.h>
#include <ytlib/table_server/table_ypath.pb.h>

#include <ytlib/object_server/object_ypath_proxy.h>

namespace NYT {
namespace NChunkServer {

////////////////////////////////////////////////////////////////////////////////

struct TChunkYPathProxy
    : public NObjectServer::TObjectYPathProxy
{
    DEFINE_YPATH_PROXY_METHOD(NProto, Locate);

    // NB: works only for table chunks.
    DEFINE_YPATH_PROXY_METHOD(NTableServer::NProto, Fetch);
    DEFINE_YPATH_PROXY_METHOD(NProto, Confirm);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
