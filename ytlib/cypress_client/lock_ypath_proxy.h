#pragma once

#include "public.h"

#include <yt/ytlib/object_client/object_ypath_proxy.h>

#include <yt/core/rpc/public.h>
#include <yt/core/rpc/proto/rpc.pb.h>

namespace NYT {
namespace NCypressClient {

////////////////////////////////////////////////////////////////////////////////

struct TLockYPathProxy
    : public NObjectClient::TObjectYPathProxy
{ };

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypressClient
} // namespace NYT
