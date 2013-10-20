#pragma once

#include <yt/ytlib/query_client/public.h>

#include <core/rpc/public.h>

namespace NYT {
namespace NDriver {

////////////////////////////////////////////////////////////////////////////////

class TQueryCallbacksProvider
    : public TRefCounted
{
public:
    explicit TQueryCallbacksProvider(NRpc::IChannelPtr masterChannel);
    ~TQueryCallbacksProvider();

    NQueryClient::IPrepareCallbacks* GetPrepareCallbacks();
    NQueryClient::ICoordinateCallbacks* GetCoordinateCallbacks();

private:
    class TImpl;
    TIntrusivePtr<TImpl> Impl_;

};

typedef TIntrusivePtr<TQueryCallbacksProvider> TQueryCallbacksProviderPtr;

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT


