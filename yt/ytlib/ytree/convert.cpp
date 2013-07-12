#include "stdafx.h"
#include "convert.h"

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

template TYsonString ConvertToYsonString<int>(const int&);
template TYsonString ConvertToYsonString<unsigned long>(const unsigned long&);
template TYsonString ConvertToYsonString<Stroka>(const Stroka&);
template TYsonString ConvertToYsonString<const char*>(const char*);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT

