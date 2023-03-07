#include "helpers.h"

#include <yt/core/misc/format.h>

#include <yt/client/node_tracker_client/proto/node.pb.h>

namespace NYT::NNodeTrackerClient {

////////////////////////////////////////////////////////////////////////////////

NYPath::TYPath GetClusterNodesPath()
{
    return "//sys/cluster_nodes";
}

////////////////////////////////////////////////////////////////////////////////

namespace NProto {

void FormatValue(TStringBuilderBase* builder, const TDiskLocationResources& locationResources, TStringBuf spec)
{
    builder->AppendFormat(
        "{usage: %v, limit: %v, medium_index: %v}",
        locationResources.usage(),
        locationResources.limit(),
        locationResources.medium_index());
}

TString ToString(const TDiskLocationResources& locationResources)
{
    return ToStringViaBuilder(locationResources);
}

void FormatValue(TStringBuilderBase* builder, const TDiskResources& diskResources, TStringBuf spec)
{
    MakeFormattableView(diskResources.disk_location_resources(), [] (TStringBuilderBase* builder, const NProto::TDiskLocationResources& diskLocationResources) {
        FormatValue(builder, diskLocationResources, "%v");
    });
}

TString ToString(const TDiskResources& diskResources)
{
    return ToStringViaBuilder(diskResources);
}

} // namespace NProto

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNodeTrackerClient

