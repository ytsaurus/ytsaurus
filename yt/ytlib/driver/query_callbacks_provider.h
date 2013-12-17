#pragma once

#include "public.h"

#include <core/rpc/public.h>

#include <yt/ytlib/query_client/public.h>

#include <yt/ytlib/tablet_client/public.h>

namespace NYT {
namespace NDriver {

////////////////////////////////////////////////////////////////////////////////

class TQueryCallbacksProvider
    : public TRefCounted
{
public:
    TQueryCallbacksProvider(
        NRpc::IChannelPtr masterChannel,
        NTabletClient::TTableMountCachePtr tableMountCache);
    ~TQueryCallbacksProvider();

    NQueryClient::IPrepareCallbacks* GetPrepareCallbacks();
    NQueryClient::ICoordinateCallbacks* GetCoordinateCallbacks();

private:
    class TImpl;
    TIntrusivePtr<TImpl> Impl_;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT


