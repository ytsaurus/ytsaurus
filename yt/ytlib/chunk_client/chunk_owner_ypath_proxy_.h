#pragma once

#include <ytlib/chunk_client/chunk_owner_ypath.pb.h>
#include <ytlib/ytree/ypath_proxy.h>

namespace NYT {
namespace NChunkClient {

////////////////////////////////////////////////////////////////////////////////

DECLARE_ENUM(EUpdateMode,
    (None)
    (Append)
    (Overwrite)
);

struct TChunkOwnerYPathProxy
    : public NYTree::TYPathProxy
{
    DEFINE_YPATH_PROXY_METHOD(NProto, PrepareForUpdate);
    DEFINE_YPATH_PROXY_METHOD(NProto, Fetch);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
