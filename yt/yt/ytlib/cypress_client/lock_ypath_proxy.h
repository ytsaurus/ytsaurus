#pragma once

#include "public.h"

#include <yt/yt/ytlib/object_client/object_ypath_proxy.h>

#include <yt/yt/core/rpc/public.h>
#include <yt/yt_proto/yt/core/rpc/proto/rpc.pb.h>

namespace NYT::NCypressClient {

////////////////////////////////////////////////////////////////////////////////

struct TLockYPathProxy
    : public NObjectClient::TObjectYPathProxy
{ };

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressClient
