#include "stdafx.h"
#include "build_attributes.h"

#include <ytlib/ytree/ypath_client.h>
#include <ytlib/ytree/convert.h>

#include <yt/build.h>

namespace NYT {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

void SetBuildAttributes(IYPathServicePtr orchidRoot, const char* serviceName)
{
    SyncYPathSet(orchidRoot, "/service", TYsonString("{}"));
    SyncYPathSet(orchidRoot, "/service/version", ConvertToYsonString(YT_VERSION));
    SyncYPathSet(orchidRoot, "/service/host", ConvertToYsonString(YT_BUILD_HOST));
    SyncYPathSet(orchidRoot, "/service/time", ConvertToYsonString(YT_BUILD_TIME));
    SyncYPathSet(orchidRoot, "/service/machine", ConvertToYsonString(YT_BUILD_MACHINE));
    SyncYPathSet(orchidRoot, "/service/start_time", ConvertToYsonString(TInstant::Now()));
    SyncYPathSet(orchidRoot, "/service/name", ConvertToYsonString(serviceName));

    // COMPAT(lukyan)
    SyncYPathSet(orchidRoot, "/@version", ConvertToYsonString(YT_VERSION));
    SyncYPathSet(orchidRoot, "/@build_host", ConvertToYsonString(YT_BUILD_HOST));
    SyncYPathSet(orchidRoot, "/@build_time", ConvertToYsonString(YT_BUILD_TIME));
    SyncYPathSet(orchidRoot, "/@build_machine", ConvertToYsonString(YT_BUILD_MACHINE));
    SyncYPathSet(orchidRoot, "/@start_time", ConvertToYsonString(TInstant::Now()));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
