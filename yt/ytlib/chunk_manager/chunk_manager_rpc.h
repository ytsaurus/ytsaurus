#pragma once

#include "common.h"
#include "chunk_manager.pb.h"

#include "../rpc/service.h"
#include "../rpc/client.h"

namespace NYT {
namespace NChunkManager {

////////////////////////////////////////////////////////////////////////////////

class TChunkManagerProxy
    : public NRpc::TProxyBase
{
public:
    typedef TIntrusivePtr<TChunkManagerProxy> TPtr;

    DECLARE_DERIVED_ENUM(NRpc::EErrorCode, EErrorCode,
        ((NoSuchTransaction)(1))
        ((NoSuchHolderId)(2))
    );

    static Stroka GetServiceName()
    {
        return "TransactionManager";
    }

    TChunkManagerProxy(NRpc::TChannel::TPtr channel)
        : TProxyBase(channel, GetServiceName())
    { }

    RPC_PROXY_METHOD(NProto, RegisterHolder);
    RPC_PROXY_METHOD(NProto, HolderHeartbeat);
    //RPC_PROXY_METHOD(NProto, AbortTransaction);
    //RPC_PROXY_METHOD(NProto, RenewTransactionLease);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkManager
} // namespace NYT
