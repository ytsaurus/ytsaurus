#ifndef YPATH_DETAIL_INL_H_
#error "Direct inclusion of this file is not allowed, include ypath_detail.h"
// For the sake of sane code completion.
#include "ypath_detail.h"
#endif

namespace NYT::NYTree {

////////////////////////////////////////////////////////////////////////////////

template <class TRequestMessage, class TResponseMessage>
TTypedYPathServiceContext<TRequestMessage, TResponseMessage>::TTypedYPathServiceContext(
    IYPathServiceContextPtr context,
    const NRpc::THandlerInvocationOptions& options)
    : TBase(context, options)
    , UnderlyingYPathContext_(context.Get())
{ }

template <class TRequestMessage, class TResponseMessage>
IYPathServiceContext* TTypedYPathServiceContext<TRequestMessage, TResponseMessage>::GetUnderlyingYPathContext()
{
    return UnderlyingYPathContext_;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTree
