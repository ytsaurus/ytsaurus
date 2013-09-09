#include "stdafx.h"
#include "build_attributes.h"

#include <core/ytree/ypath_client.h>
#include <core/ytree/fluent.h>

#include <yt/build.h>

namespace NYT {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

void SetBuildAttributes(IYPathServicePtr orchidRoot, const char* serviceName)
{
    SyncYPathSet(
        orchidRoot,
        "/service",
        BuildYsonStringFluently()
            .BeginAttributes()
                .Item("opaque").Value(true)
            .EndAttributes()
            .BeginMap()
                .Item("version").Value(YT_VERSION)
                .Item("host").Value(YT_BUILD_HOST)
                .Item("time").Value(YT_BUILD_TIME)
                .Item("machine").Value(YT_BUILD_MACHINE)
                .Item("start_time").Value(TInstant::Now())
                .Item("name").Value(serviceName)
            .EndMap());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
