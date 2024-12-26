#include "master_ypath_proxy.h"

namespace NYT::NObjectClient {

////////////////////////////////////////////////////////////////////////////////

#define DEFINE_VECTORIZED_REQUEST_BATCHER(ns, method) \
    TMasterYPathProxy::TVectorized##method##Batcher TMasterYPathProxy::Create##method##Batcher( \
        const NApi::NNative::IClientPtr& client, \
        const TIntrusivePtr<NYTree::TTypedYPathRequest<ns::TReq##method, ns::TRsp##method>>& typedRequestPtr, \
        TRange<TObjectId> objectIds, \
        TTransactionId cypressTransactionId) \
    { \
        return TVectorized##method##Batcher(client, typedRequestPtr, objectIds, cypressTransactionId); \
    } \
    static_assert(true)

DEFINE_VECTORIZED_REQUEST_BATCHER(NYT::NYTree::NProto, Get);
DEFINE_VECTORIZED_REQUEST_BATCHER(NYT::NCypressClient::NProto, SerializeNode);

#undef DEFINE_VECTORIZED_REQUEST_BATCHER

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NObjectClient
