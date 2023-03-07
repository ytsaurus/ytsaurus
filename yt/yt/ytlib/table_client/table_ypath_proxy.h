#pragma once

#include "public.h"

#include <yt/ytlib/chunk_client/chunk_owner_ypath_proxy.h>

#include <yt/ytlib/table_client/proto/table_ypath.pb.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

struct TTableYPathProxy
    : public NChunkClient::TChunkOwnerYPathProxy
{
    DEFINE_YPATH_PROXY(Table);

    DEFINE_YPATH_PROXY_METHOD(NProto, GetMountInfo);

    // Those are not mutating anymore, see server/object_server/object_service.cpp.
    DEFINE_MUTATING_YPATH_PROXY_METHOD(NProto, Mount);
    DEFINE_MUTATING_YPATH_PROXY_METHOD(NProto, Unmount);
    DEFINE_MUTATING_YPATH_PROXY_METHOD(NProto, Freeze);
    DEFINE_MUTATING_YPATH_PROXY_METHOD(NProto, Unfreeze);
    DEFINE_MUTATING_YPATH_PROXY_METHOD(NProto, Remount);
    DEFINE_MUTATING_YPATH_PROXY_METHOD(NProto, Reshard);

    DEFINE_MUTATING_YPATH_PROXY_METHOD(NProto, ReshardAutomatic);

    DEFINE_MUTATING_YPATH_PROXY_METHOD(NProto, Alter);

    DEFINE_MUTATING_YPATH_PROXY_METHOD(NProto, LockDynamicTable);
    DEFINE_YPATH_PROXY_METHOD(NProto, CheckDynamicTableLock);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
