#pragma once

#include <ytlib/chunk_client/chunk_list_ypath.pb.h>

#include <ytlib/object_client/object_ypath_proxy.h>

namespace NYT {
namespace NChunkClient {

////////////////////////////////////////////////////////////////////////////////

struct TChunkListYPathProxy
    : public NObjectClient::TObjectYPathProxy
{
    static Stroka GetServiceName()
    {
        return "ChunkList";
    }

    DEFINE_MUTATING_YPATH_PROXY_METHOD(NProto, Attach);
    DEFINE_YPATH_PROXY_METHOD(NProto, GetStatistics);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
