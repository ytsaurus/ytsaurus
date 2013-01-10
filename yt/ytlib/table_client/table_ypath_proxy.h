#pragma once

#include "public.h"

#include <ytlib/table_client/table_ypath.pb.h>

#include <ytlib/ytree/ypath_proxy.h>

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

DECLARE_ENUM(ETableUpdateMode,
    (None)
    (Append)
    (Overwrite)
);

struct TTableYPathProxy
    : public NYTree::TYPathProxy
{
    DEFINE_YPATH_PROXY_METHOD(NProto, PrepareForUpdate);
    DEFINE_YPATH_PROXY_METHOD(NProto, Fetch);
    DEFINE_YPATH_PROXY_METHOD(NProto, SetSorted);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
