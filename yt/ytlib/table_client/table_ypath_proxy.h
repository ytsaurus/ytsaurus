#pragma once

#include "public.h"

#include <ytlib/table_client/table_ypath.pb.h>

#include <ytlib/chunk_client/chunk_owner_ypath_proxy.h>

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

struct TTableYPathProxy
    : public NChunkClient::TChunkOwnerYPathProxy
{
    static Stroka GetServiceName()
    {
        return "TableNode";
    }

    DEFINE_YPATH_PROXY_METHOD(NProto, GetMountInfo);
    DEFINE_MUTATING_YPATH_PROXY_METHOD(NProto, SetSorted);
    DEFINE_MUTATING_YPATH_PROXY_METHOD(NProto, Mount);
    DEFINE_MUTATING_YPATH_PROXY_METHOD(NProto, Unmount);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
