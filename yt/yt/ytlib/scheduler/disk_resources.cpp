#include "disk_resources.h"

#include <yt/yt/ytlib/chunk_client/medium_directory.h>

#include <yt/yt_proto/yt/client/node_tracker_client/proto/node.pb.h>

#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NScheduler {

using NYT::FromProto;
using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

void TDiskResources::TDiskLocationResources::Persist(const TStreamPersistenceContext& context)
{
    using NYT::Persist;

    Persist(context, Usage);
    Persist(context, Limit);
    Persist(context, MediumIndex);
}

void TDiskResources::Persist(const TStreamPersistenceContext& context)
{
    using NYT::Persist;

    Persist(context, DiskLocationResources);
    Persist(context, DefaultMediumIndex);
}

////////////////////////////////////////////////////////////////////////////////

void ToProto(
    NNodeTrackerClient::NProto::TDiskLocationResources* protoDiskLocationResources,
    const TDiskResources::TDiskLocationResources& diskLocationResources)
{
    protoDiskLocationResources->set_usage(diskLocationResources.Usage);
    protoDiskLocationResources->set_limit(diskLocationResources.Limit);
    protoDiskLocationResources->set_medium_index(diskLocationResources.MediumIndex);
}

void FromProto(
    TDiskResources::TDiskLocationResources* diskLocationResources,
    const NNodeTrackerClient::NProto::TDiskLocationResources& protoDiskLocationResources)
{
    diskLocationResources->Usage = protoDiskLocationResources.usage();
    diskLocationResources->Limit = protoDiskLocationResources.limit();
    diskLocationResources->MediumIndex = protoDiskLocationResources.medium_index();
}

void ToProto(NNodeTrackerClient::NProto::TDiskResources* protoDiskResources, const TDiskResources& diskResources)
{
    ToProto(protoDiskResources->mutable_disk_location_resources(), diskResources.DiskLocationResources);
    protoDiskResources->set_default_medium_index(diskResources.DefaultMediumIndex);
}

void FromProto(TDiskResources* diskResources, const NNodeTrackerClient::NProto::TDiskResources& protoDiskResources)
{
    diskResources->DiskLocationResources = FromProto<std::vector<TDiskResources::TDiskLocationResources>>(protoDiskResources.disk_location_resources());
    diskResources->DefaultMediumIndex = protoDiskResources.default_medium_index();
}

////////////////////////////////////////////////////////////////////////////////

void FormatValue(TStringBuilderBase* builder, const TDiskResources& diskResources, TStringBuf /*spec*/)
{
    builder->AppendFormat(
        "{Locations: [%v], DefaultMediumIndex: %v}",
        MakeFormattableView(diskResources.DiskLocationResources, [] (TStringBuilderBase* builder, const TDiskResources::TDiskLocationResources& locationResources) {
            builder->AppendFormat(
                "{Usage: %v, Limit: %v, MediumIndex: %v}",
                locationResources.Usage,
                locationResources.Limit,
                locationResources.MediumIndex);
        }),
        diskResources.DefaultMediumIndex);
}

TString ToString(
    const TDiskResources& diskResources,
    const NChunkClient::TMediumDirectoryPtr& mediumDirectory)
{
    return Format(
        "{Locations: [%v], DefaultMediumIndex: %v}",
        MakeFormattableView(
            diskResources.DiskLocationResources,
            [&mediumDirectory] (TStringBuilderBase* builder, const TDiskResources::TDiskLocationResources& locationResources)
            {
                int mediumIndex = locationResources.MediumIndex;
                builder->AppendFormat("{Usage: %v, Limit: %v, MediumIndex: %v, MediumName: %v}",
                    locationResources.Usage,
                    locationResources.Limit,
                    mediumIndex,
                    mediumDirectory->GetMediumName(mediumIndex));
            }),
        diskResources.DefaultMediumIndex);
}

void Serialize(const TDiskResources& diskResources, NYson::IYsonConsumer* consumer)
{
    NYTree::BuildYsonFluently(consumer)
        .BeginMap()
            .Item("locations").DoListFor(diskResources.DiskLocationResources, [&] (auto fluent, const TDiskResources::TDiskLocationResources& locationResources) {
                fluent.Item().BeginMap()
                    .Item("usage").Value(locationResources.Usage)
                    .Item("limit").Value(locationResources.Limit)
                    .Item("medium_index").Value(locationResources.MediumIndex)
                .EndMap();
            })
            .Item("default_medium_index").Value(diskResources.DefaultMediumIndex)
        .EndMap();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler

