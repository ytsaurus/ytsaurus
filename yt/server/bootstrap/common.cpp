#include "stdafx.h"
#include "common.h"

#include <ytlib/ytree/ypath_client.h>
#include <ytlib/ytree/convert.h>
#include <yt/build.h>

namespace NYT {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

void SetBuildAttributes(NYTree::IYPathServicePtr orchidRoot)
{
    SyncYPathSet(orchidRoot, "/@version", ConvertToYsonString(YT_VERSION));
    SyncYPathSet(orchidRoot, "/@build_host", ConvertToYsonString(YT_BUILD_HOST));
    SyncYPathSet(orchidRoot, "/@build_time", ConvertToYsonString(YT_BUILD_TIME));
    SyncYPathSet(orchidRoot, "/@build_machine", ConvertToYsonString(YT_BUILD_MACHINE));
    SyncYPathSet(orchidRoot, "/@start_time", ConvertToYsonString(TInstant::Now()));
}

////////////////////////////////////////////////////////////////////////////////
            
} // namespace NYT
