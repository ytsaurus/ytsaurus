#include "stdafx.h"
#include "tclap_helpers.h"

#include <iterator>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

Stroka ReadAll(std::istringstream& input)
{
    Stroka result(input.str());
    input.ignore(std::numeric_limits<std::streamsize>::max());
    return result;
}

std::istringstream& operator >> (std::istringstream& input, TGuid& guid)
{
    auto str = ReadAll(input);
    guid = TGuid::FromString(str);
    return input;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

