#include "build_attributes.h"

#include <yt/yt/build/build.h>

#include <yt/yt/core/ytree/fluent.h>
#include <yt/yt/core/ytree/ypath_client.h>

namespace NYT {

using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

void BuildBuildAttributes(IYsonConsumer* consumer, const char* serviceName)
{
    BuildYsonFluently(consumer)
        .BeginMap()
            .OptionalItem("name", serviceName)
            .Item("version").Value(GetVersion())
            .Item("build_host").Value(GetBuildHost())
            .Item("build_time").Value(GetBuildTime())
            .Item("start_time").Value(TInstant::Now())
        .EndMap();
}

void SetBuildAttributes(IYPathServicePtr orchidRoot, const char* serviceName)
{
    SyncYPathSet(
        orchidRoot,
        "/service",
        BuildYsonStringFluently()
            .BeginAttributes()
                .Item("opaque").Value(true)
            .EndAttributes()
            .Do(BIND([=] (TFluentAnyWithoutAttributes fluent) {
                BuildBuildAttributes(fluent.GetConsumer(), serviceName);
            })));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

