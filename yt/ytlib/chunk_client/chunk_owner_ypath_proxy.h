#pragma once

#include <core/misc/public.h>

#include <ytlib/chunk_client/chunk_owner_ypath.pb.h>

#include <ytlib/object_client/object_ypath_proxy.h>

#include <core/ytree/ypath_proxy.h>

namespace NYT {
namespace NChunkClient {

///////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EUpdateMode,
    (None)
    (Append)
    (Overwrite)
);

struct TChunkOwnerYPathProxy
    : public NObjectClient::TObjectYPathProxy
{
    static Stroka GetServiceName()
    {
        return "ChunkOwner";
    }

    DEFINE_YPATH_PROXY_METHOD(NProto, Fetch);
    DEFINE_MUTATING_YPATH_PROXY_METHOD(NProto, PrepareForUpdate);
};

///////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
