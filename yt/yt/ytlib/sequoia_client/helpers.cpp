#include "helpers.h"

namespace NYT::NSequoiaClient {

using namespace NYPath;

////////////////////////////////////////////////////////////////////////////////

TMangledSequoiaPath MangleSequoiaPath(const TYPath& path)
{
    YT_VERIFY(!path.empty());
    YT_VERIFY(path == "/" || path.back() != '/');
    return TMangledSequoiaPath(path + '/');
}

TYPath DemangleSequoiaPath(TMangledSequoiaPath mangledPath)
{
    YT_VERIFY(!mangledPath.Underlying().empty());
    YT_VERIFY(mangledPath.Underlying().back() == '/');
    return mangledPath.Underlying().substr(0, mangledPath.Underlying().size() - 1);
}

TMangledSequoiaPath MakeLexigraphicallyMaximalMangledSequoiaPathForPrefix(TMangledSequoiaPath prefix)
{
    return TMangledSequoiaPath(prefix.Underlying() + '\xFF');
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSequoiaClient
