#ifndef NODE_INL_H_
#error "Direct inclusion of this file is not allowed, include node.h"
#endif

namespace NYT {
namespace NCypressServer {

////////////////////////////////////////////////////////////////////////////////

inline bool TCypressNodeRefComparer::Compare(const TCypressNodeBase* lhs, const TCypressNodeBase* rhs)
{
    return lhs->GetVersionedId() < rhs->GetVersionedId();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypressServer
} // namespace NYT
