#pragma once

#include "common.h"
#include "cypress_service.pb.h"

#include <ytlib/ytree/ypath_client.h>
#include <ytlib/ytree/ypath_detail.h>
#include <ytlib/transaction_server/common.h>

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
        const NYTree::TYPath& path,
        const NTransactionServer::TTransactionId& transactionId,
        TTypedRequest* innerRequest);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypress
} // namespace NYT

#define CYPRESS_SERVICE_PROXY_INL_H_
#include "cypress_service_proxy-inl.h"
#undef CYPRESS_SERVICE_PROXY_INL_H_
