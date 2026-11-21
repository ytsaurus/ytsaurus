#ifndef NODE_DETAIL_INL_H_
#error "Direct inclusion of this file is not allowed, include node_detail.h"
// For the sake of sane code completion.
#include "node_detail.h"
#endif

namespace NYT::NCypressServer {

////////////////////////////////////////////////////////////////////////////////

template <class TChild>
void TMapNodeImpl<TChild>::RegisterMetadata(auto&& registrar)
{
    registrar.template BaseType<TCompositeCypressNode>();

    PHOENIX_REGISTER_FIELD(1, ChildCountDelta_);
    PHOENIX_REGISTER_FIELD(2, Children_);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressServer
