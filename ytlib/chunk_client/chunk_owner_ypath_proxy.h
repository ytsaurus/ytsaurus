#pragma once

#include <yt/ytlib/chunk_client/chunk_owner_ypath.pb.h>

#include <yt/ytlib/cypress_client/cypress_ypath_proxy.h>

#include <yt/core/misc/public.h>

#include <yt/core/ytree/ypath_proxy.h>

namespace NYT {
namespace NChunkClient {

///////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EUpdateMode,
    (None)
    (Append)
    (Overwrite)
);

struct TChunkOwnerYPathProxy
    : public NCypressClient::TCypressYPathProxy
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
