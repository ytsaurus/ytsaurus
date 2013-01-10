#pragma once

#include "public.h"

#include <ytlib/file_client/file_ypath.pb.h>

#include <ytlib/ytree/ypath_proxy.h>

namespace NYT {
namespace NFileClient {

////////////////////////////////////////////////////////////////////////////////

DECLARE_ENUM(EFileUpdateMode,
    (None)
    (Overwrite)
);

struct TFileYPathProxy
    : NYTree::TYPathProxy
{
    DEFINE_YPATH_PROXY_METHOD(NProto, PrepareForUpdate);
    // TODO(babenko): rename back to Fetch
    DEFINE_YPATH_PROXY_METHOD(NProto, FetchFile);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NFileClient
} // namespace NYT
