#pragma once

#include "common.h"
#include "cypress_service_rpc.pb.h"

#include "../ytree/ypath_client.h"
#include "../ytree/ypath_detail.h"
#include "../transaction_server/common.h"

namespace NYT {
namespace NCypress {

////////////////////////////////////////////////////////////////////////////////

class TCypressServiceProxy
    : public NRpc::TProxyBase
{
public:
    typedef TIntrusivePtr<TCypressServiceProxy> TPtr;

    RPC_DECLARE_PROXY(CypressService,
        ((NoSuchTransaction)(1))
    );

    TCypressServiceProxy(NRpc::IChannel* channel)
        : TProxyBase(channel, GetServiceName())
    { }

    DEFINE_RPC_PROXY_METHOD(NProto, Execute);

    template <class TTypedRequest>
    TIntrusivePtr< TFuture< TIntrusivePtr<typename TTypedRequest::TTypedResponse> > >
    Execute(
        NYTree::TYPath path,
        const NTransactionServer::TTransactionId& transactionId,
        TTypedRequest* innerRequest);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypress
} // namespace NYT

#define CYPRESS_SERVICE_RPC_INL_H_
#include "cypress_service_rpc-inl.h"
#undef CYPRESS_SERVICE_RPC_INL_H_
