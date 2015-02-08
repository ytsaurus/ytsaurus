#pragma once

#include "public.h"

#include <ytlib/new_table_client/table_ypath.pb.h>

#include <ytlib/chunk_client/chunk_owner_ypath_proxy.h>

namespace NYT {
namespace NVersionedTableClient {

////////////////////////////////////////////////////////////////////////////////

struct TTableYPathProxy
    : public NChunkClient::TChunkOwnerYPathProxy
{
    static Stroka GetServiceName()
    {
        return "Table";
    }

    DEFINE_YPATH_PROXY_METHOD(NProto, GetMountInfo);
    DEFINE_MUTATING_YPATH_PROXY_METHOD(NProto, SetSorted);
    DEFINE_MUTATING_YPATH_PROXY_METHOD(NProto, Mount);
    DEFINE_MUTATING_YPATH_PROXY_METHOD(NProto, Unmount);
    DEFINE_MUTATING_YPATH_PROXY_METHOD(NProto, Remount);
    DEFINE_MUTATING_YPATH_PROXY_METHOD(NProto, Reshard);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
