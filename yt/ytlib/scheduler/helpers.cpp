#include "stdafx.h"
#include "helpers.h"

#include <ytlib/ytree/ypath_client.h>

namespace NYT {
namespace NScheduler {

using namespace NYTree;

////////////////////////////////////////////////////////////////////

TYPath GetOperationPath(const TOperationId& id)
{
    return "//sys/operations/" + EscapeYPathToken(id.ToString());
}

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

