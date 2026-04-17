#pragma once

#include <yt/yt/ytlib/ban_client/proto/ban_service.pb.h>

#include <yt/yt/core/rpc/client.h>

namespace NYT::NBanClient {

////////////////////////////////////////////////////////////////////////////////

class TBanServiceProxy
    : public NRpc::TProxyBase
{
public:
    DEFINE_RPC_PROXY(TBanServiceProxy, BanService);

    DEFINE_RPC_PROXY_METHOD(NProto, GetUserBanned);
    DEFINE_RPC_PROXY_METHOD(NProto, SetUserBanned);
    DEFINE_RPC_PROXY_METHOD(NProto, ListBannedUsers);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLeaseClient
