#include "stdafx.h"
#include "convert.h"

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

template TYsonString ConvertToYsonString<int>(const int&);
template TYsonString ConvertToYsonString<unsigned long>(const unsigned long&);
template TYsonString ConvertToYsonString<TRawString>(const TRawString&);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT

