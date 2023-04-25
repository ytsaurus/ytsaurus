#ifndef YPATH_DETAIL_INL_H_
#error "Direct inclusion of this file is not allowed, include ypath_detail.h"
// For the sake of sane code completion.
#include "ypath_detail.h"
#endif

namespace NYT::NYTree {

////////////////////////////////////////////////////////////////////////////////

template <class TContextPtr>
void TSupportsExistsBase::Reply(const TContextPtr& context, bool value)
{
    context->Response().set_value(value);
    context->SetResponseInfo("Result: %v", value);
    context->Reply();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTree
