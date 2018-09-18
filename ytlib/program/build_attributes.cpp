#include "build_attributes.h"

#include <yt/build/build.h>

#include <yt/core/ytree/fluent.h>
#include <yt/core/ytree/ypath_client.h>

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
                .Item("name").Value(serviceName)
                .Item("version").Value(GetVersion())
                .Item("build_host").Value(GetBuildHost())
                .Item("build_time").Value(GetBuildTime())
                .Item("build_machine").Value(GetBuildMachine())
                .Item("start_time").Value(TInstant::Now())
            .EndMap());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

