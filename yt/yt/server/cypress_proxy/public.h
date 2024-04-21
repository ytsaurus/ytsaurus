#pragma once

#include <yt/yt/ytlib/cypress_client/public.h>

#include <yt/yt/core/rpc/public.h>

#include <yt/yt/core/misc/public.h>

namespace NYT::NCypressProxy {

////////////////////////////////////////////////////////////////////////////////

struct IBootstrap;

DECLARE_REFCOUNTED_STRUCT(ISequoiaServiceContext)
DECLARE_REFCOUNTED_CLASS(TSequoiaServiceContextWrapper)

template <class RequestMessage, class ResponseMessage>
using TTypedSequoiaServiceContext = NRpc::TGenericTypedServiceContext<
    ISequoiaServiceContext,
    TSequoiaServiceContextWrapper,
    RequestMessage,
    ResponseMessage>;

DECLARE_REFCOUNTED_STRUCT(IObjectService)
DECLARE_REFCOUNTED_STRUCT(ISequoiaService)

DECLARE_REFCOUNTED_CLASS(TDynamicConfigManager)

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TObjectServiceDynamicConfig)
DECLARE_REFCOUNTED_CLASS(TCypressProxyDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressProxy
