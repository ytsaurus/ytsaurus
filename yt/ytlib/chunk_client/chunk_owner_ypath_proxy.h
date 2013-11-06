#pragma once

#include <core/misc/common.h>

#include <ytlib/chunk_client/chunk_owner_ypath.pb.h>

#include <core/ytree/ypath_proxy.h>

namespace NYT {
namespace NChunkClient {

///////////////////////////////////////////////////////////////////////////////

DECLARE_ENUM(EUpdateMode,
    (None)
    (Append)
    (Overwrite)
);

struct TChunkOwnerYPathProxy
    : public NYTree::TYPathProxy
{
    DEFINE_YPATH_PROXY_METHOD(NProto, Fetch);
    DEFINE_MUTATING_YPATH_PROXY_METHOD(NProto, PrepareForUpdate);
};

///////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
