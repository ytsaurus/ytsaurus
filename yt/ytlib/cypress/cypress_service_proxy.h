#pragma once

#include "common.h"
#include "cypress_service.pb.h"

#include <ytlib/ytree/ypath_client.h>
#include <ytlib/ytree/ypath_detail.h>

namespace NYT {
namespace NCypress {

////////////////////////////////////////////////////////////////////////////////

extern const NYTree::TYPath ObjectIdMarker;
extern const NYTree::TYPath TransactionIdMarker;

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
        const NYTree::TYPath& path);

    template <class TTypedRequest>
    TIntrusivePtr< TFuture< TIntrusivePtr<typename TTypedRequest::TTypedResponse> > >
    Execute(
        TTypedRequest* innerRequest);

    template <class TTypedRequest>
    TIntrusivePtr< TFuture< TIntrusivePtr<typename TTypedRequest::TTypedResponse> > >
    Execute(
        TTypedRequest* innerRequest,
        const NYTree::TYPath& path,
        const TTransactionId& transactionId);

    template <class TTypedRequest>
    TIntrusivePtr< TFuture< TIntrusivePtr<typename TTypedRequest::TTypedResponse> > >
    Execute(
        TTypedRequest* innerRequest,
        const NObjectServer::TObjectId& objectId);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypress
} // namespace NYT

#define CYPRESS_SERVICE_PROXY_INL_H_
#include "cypress_service_proxy-inl.h"
#undef CYPRESS_SERVICE_PROXY_INL_H_
