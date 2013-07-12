#include "stdafx.h"
#include "convert.h"

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

template TYsonString ConvertToYsonString<int>(const int&);
template TYsonString ConvertToYsonString<unsigned long>(const unsigned long&);
template TYsonString ConvertToYsonString<Stroka>(const Stroka&);

template <>
TYsonString ConvertToYsonString<Stroka>(const char* value)
{
	return ConvertToYsonString(Stroka(value));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT

