#pragma once

#include "public.h"

#include <yt/ytlib/query_client/public.h>

#include <core/rpc/public.h>

namespace NYT {
namespace NDriver {

////////////////////////////////////////////////////////////////////////////////

class TQueryCallbacksProvider
    : public TRefCounted
{
public:
    TQueryCallbacksProvider(
        NRpc::IChannelPtr masterChannel,
        TTableMountCachePtr tableMountCache);
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


