#include "stdafx.h"
#include "cypress_ypath_proxy.h"

#include <ytlib/ytree/ypath_detail.h>

namespace NYT {
namespace NCypress {

using namespace NYTree;
using namespace NObjectServer;

////////////////////////////////////////////////////////////////////////////////

const char ObjectIdMarker = '#';

////////////////////////////////////////////////////////////////////////////////

TYPath YPathFromObjectId(const TObjectId& id)
{
    return YPathRoot + ObjectIdMarker + id.ToString();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypress
} // namespace NYT

