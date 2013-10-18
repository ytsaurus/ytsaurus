#include "stdafx.h"
#include "build_attributes.h"

#include <core/ytree/ypath_client.h>
#include <core/ytree/fluent.h>

#include <core/build.h>

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

