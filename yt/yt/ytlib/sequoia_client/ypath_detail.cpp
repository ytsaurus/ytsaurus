#include "ypath_detail.h"

namespace NYT::NSequoiaClient {

////////////////////////////////////////////////////////////////////////////////

TYPath::TYPath(const TMangledSequoiaPath& mangledPath)
    : TBase(DemangleSequoiaPath(mangledPath))
{ }

TString ToString(const TYPath& path)
{
    return path.ToString();
}

TString ToString(const TYPathBuf path)
{
    return path.ToString();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSequoiaClient
