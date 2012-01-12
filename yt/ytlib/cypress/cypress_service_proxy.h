#pragma once

#include "common.h"
#include "cypress_service.pb.h"

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

    static Stroka GetServiceName()
    {
        return "CypressService";
    }

    DECLARE_ENUM(EErrorCode,
        ((NoSuchTransaction)(1))
    );

    TCypressServiceProxy(NRpc::IChannel* channel)
        : TProxyBase(channel, GetServiceName())
    { }

    DEFINE_RPC_PROXY_METHOD(NProto, Execute);

    template <class TTypedRequest>
    TIntrusivePtr< TFuture< TIntrusivePtr<typename TTypedRequest::TTypedResponse> > >
    Execute(
        TTypedRequest* innerRequest,
        const NYTree::TYPath& path = "",
        const TTransactionId& transactionId = NullTransactionId);

    template <class TTypedRequest>
    TIntrusivePtr< TFuture< TIntrusivePtr<typename TTypedRequest::TTypedResponse> > >
    Execute(
        TTypedRequest* innerRequest,
        const NObjectServer::TObjectId& objectId,
        const TTransactionId& transactionId = NullTransactionId);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypress
} // namespace NYT

#define CYPRESS_SERVICE_PROXY_INL_H_
#include "cypress_service_proxy-inl.h"
#undef CYPRESS_SERVICE_PROXY_INL_H_
