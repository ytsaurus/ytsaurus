#include "helpers.h"

namespace NYT::NSequoiaClient {

using namespace NYPath;

////////////////////////////////////////////////////////////////////////////////

TYPath MangleCypressPath(TYPath rawPath)
{
    YT_ASSERT(!rawPath.empty());
    YT_ASSERT(rawPath.back() != '/');

    rawPath.push_back('/');
    return rawPath;
}

TYPath DemangleCypressPath(TYPath mangledPath)
{
    YT_ASSERT(!mangledPath.empty());
    YT_ASSERT(mangledPath.back() == '/');

    mangledPath.pop_back();
    return mangledPath;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSequoiaClient
