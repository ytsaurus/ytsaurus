#pragma once

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/ytlib/object_client/proto/master_ypath.pb.h>

#include <yt/yt/core/ytree/ypath_proxy.h>

namespace NYT::NObjectClient {

////////////////////////////////////////////////////////////////////////////////

#define DECLARE_VECTORIZED_REQUEST_BATCHER(ns, method) \
    using TVectorized##method##Batcher = TVectorizedRequestBatcher<ns::TReq##method, ns::TRsp##method>; \
    \
    static TVectorized##method##Batcher Create##method##Batcher( \
        const NApi::NNative::IClientPtr& client, \
        const TIntrusivePtr<NYTree::TTypedYPathRequest<ns::TReq##method, ns::TRsp##method>>& typedRequestPtr, \
        TRange<TObjectId> objectIds); \
    static_assert(true)

////////////////////////////////////////////////////////////////////////////////

struct TMasterYPathProxy
{
    DEFINE_YPATH_PROXY(Master);

    // NB: when introducing a new method here, consider marking up such requests
    // with suppress_transaction_coordinator_sync.

    DEFINE_MUTATING_YPATH_PROXY_METHOD(NProto, CreateObject);
    DEFINE_YPATH_PROXY_METHOD(NProto, GetClusterMeta);
    DEFINE_YPATH_PROXY_METHOD(NProto, CheckPermissionByAcl);

    DEFINE_MUTATING_YPATH_PROXY_METHOD(NProto, AddMaintenance);
    DEFINE_MUTATING_YPATH_PROXY_METHOD(NProto, RemoveMaintenance);

    template <class TRequest, class TResponse>
    class TVectorizedRequestBatcher;

    // NB: This request succeedes even if a subrequest fails.
    DEFINE_YPATH_PROXY_METHOD(NProto, VectorizedRead);

    DECLARE_VECTORIZED_REQUEST_BATCHER(NYT::NYTree::NProto, Get);
};

////////////////////////////////////////////////////////////////////////////////

#undef DECLARE_VECTORIZED_REQUEST_BATCHER

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NObjectClient

#define MASTER_YPATH_PROXY_INL_H_
#include "master_ypath_proxy-inl.h"
#undef MASTER_YPATH_PROXY_INL_H_
