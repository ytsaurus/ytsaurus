#pragma once

#include <yt/ytlib/ypath/rich.h>

#include <tclap/CmdLine.h>

/////////////////////////////////////////////////////////////////////////////

namespace TCLAP {

template <>
struct ArgTraits< NYT::NYPath::TRichYPath >
{
    typedef ValueLike ValueCategory;
};

} // namespace TCLAP

/////////////////////////////////////////////////////////////////////////////

namespace NYT {
namespace NYPath {

std::istringstream& operator >> (std::istringstream& input, TRichYPath& path);

} // namespace NYPath
} // namespace NYT

/////////////////////////////////////////////////////////////////////////////
