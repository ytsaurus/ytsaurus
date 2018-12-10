#pragma once

#include <yt/ytlib/chunk_client/chunk_owner_ypath.pb.h>

#include <yt/ytlib/cypress_client/cypress_ypath_proxy.h>

#include <yt/core/misc/public.h>

#include <yt/core/ytree/ypath_proxy.h>

namespace NYT::NChunkClient {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EUpdateMode,
    (None)
    (Append)
    (Overwrite)
);

struct TChunkOwnerYPathProxy
    : public NCypressClient::TCypressYPathProxy
{
    DEFINE_YPATH_PROXY(ChunkOwner);

    DEFINE_YPATH_PROXY_METHOD(NProto, Fetch);
    DEFINE_MUTATING_YPATH_PROXY_METHOD(NProto, BeginUpload);
    DEFINE_YPATH_PROXY_METHOD(NProto, GetUploadParams);
    DEFINE_MUTATING_YPATH_PROXY_METHOD(NProto, EndUpload);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
