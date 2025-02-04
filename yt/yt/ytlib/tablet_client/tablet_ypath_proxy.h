#pragma once

#include "public.h"

#include <yt/yt/ytlib/tablet_client/proto/tablet_ypath.pb.h>

#include <yt/yt/core/ytree/ypath_client.h>

namespace NYT::NTabletClient {

////////////////////////////////////////////////////////////////////////////////

struct TTabletYPathProxy
{
    DEFINE_YPATH_PROXY(Tablet);

    DEFINE_MUTATING_YPATH_PROXY_METHOD(NProto, CancelTabletTransition);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
