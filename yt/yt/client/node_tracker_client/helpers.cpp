#include "helpers.h"

#include <yt/yt/core/misc/error.h>

#include <yt/yt_proto/yt/client/node_tracker_client/proto/node.pb.h>

namespace NYT::NNodeTrackerClient {

////////////////////////////////////////////////////////////////////////////////

NYPath::TYPath GetClusterNodesPath()
{
    return "//sys/cluster_nodes";
}

NYPath::TYPath GetExecNodesPath()
{
    return "//sys/exec_nodes";
}

void ValidateMaintenanceComment(const TString& comment)
{
    if (comment.length() > MaxMaintenanceCommentLength) {
        THROW_ERROR_EXCEPTION("Maintenance comment length is exceeded")
            << TErrorAttribute("comment_length", comment.length())
            << TErrorAttribute("max_comment_length", MaxMaintenanceCommentLength);
    }
}

////////////////////////////////////////////////////////////////////////////////

namespace NProto {

void FormatValue(TStringBuilderBase* builder, const TDiskLocationResources& locationResources, TStringBuf /*spec*/)
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

void FormatValue(TStringBuilderBase* builder, const TDiskResources& diskResources, TStringBuf /*spec*/)
{
    builder->AppendFormat(
        "%v",
        MakeFormattableView(diskResources.disk_location_resources(), [] (TStringBuilderBase* builder, const TDiskLocationResources& diskLocationResources) {
            FormatValue(builder, diskLocationResources, "%v");
        }));
}

TString ToString(const TDiskResources& diskResources)
{
    return ToStringViaBuilder(diskResources);
}

} // namespace NProto

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNodeTrackerClient

