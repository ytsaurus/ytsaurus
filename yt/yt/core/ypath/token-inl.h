#ifndef TOKEN_INL_H_
#error "Direct inclusion of this file is not allowed, include token.h"
// For the sake of sane code completion.
#include "token.h"
#endif

namespace NYT::NYPath {

////////////////////////////////////////////////////////////////////////////////

template <class E, class>
TString ToYPathLiteral(E value)
{
    return ToString(value);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYPath

