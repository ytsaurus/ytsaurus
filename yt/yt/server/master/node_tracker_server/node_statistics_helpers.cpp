#include "node_statistics_helpers.h"

#include <yt/yt/server/master/chunk_server/chunk_manager.h>
#include <yt/yt/server/master/chunk_server/medium_base.h>

namespace NYT::NNodeTrackerServer {

using namespace NChunkServer;
using namespace NChunkServer;
using namespace NYson;
using namespace NYTree;

using NYT::FromProto;

////////////////////////////////////////////////////////////////////////////////

void static BuildIOStatisticsYson(
    const NNodeTrackerClient::NProto::TIOStatistics& io,
    NYTree::TFluentMap fluent)
{
    fluent
        .Item("filesystem_read_rate").Value(io.filesystem_read_rate())
        .Item("filesystem_write_rate").Value(io.filesystem_write_rate())
        .Item("disk_read_rate").Value(io.disk_read_rate())
        .Item("disk_write_rate").Value(io.disk_write_rate())
        .Item("disk_read_capacity").Value(io.disk_read_capacity())
        .Item("disk_write_capacity").Value(io.disk_write_capacity());
}

void Serialize(
    const NNodeTrackerClient::NProto::TChunkLocationStatistics& statistics,
    TFluentMap fluent,
    const IChunkManagerPtr& chunkManager)
{
    auto mediumIndex = statistics.medium_index();
    const auto* medium = chunkManager->FindMediumByIndex(mediumIndex);
    fluent
        .Item("medium_name").Value(IsObjectAlive(medium) ? medium->GetName() : Format("<unknown medium %v>", mediumIndex))
        .Item("available_space").Value(statistics.available_space())
        .Item("used_space").Value(statistics.used_space())
        .Item("low_watermark_space").Value(statistics.low_watermark_space())
        .Item("chunk_count").Value(statistics.chunk_count())
        .Item("session_count").Value(statistics.session_count())
        .Item("full").Value(statistics.full())
        .Item("enabled").Value(statistics.enabled())
        .Item("throttling_reads").Value(statistics.throttling_reads())
        .Item("throttling_writes").Value(statistics.throttling_writes())
        .Item("sick").Value(statistics.sick())
        .Item("disk_family").Value(statistics.disk_family())
        .Item("io_statistics").DoMap(BIND(&BuildIOStatisticsYson, statistics.io_statistics()));
}

void Serialize(
    const NNodeTrackerClient::NProto::TSlotLocationStatistics& statistics,
    TFluentMap fluent,
    const IChunkManagerPtr& chunkManager)
{
    auto mediumIndex = statistics.medium_index();
    const auto* medium = chunkManager->FindMediumByIndex(mediumIndex);
    fluent
        .Item("medium_name").Value(IsObjectAlive(medium) ? medium->GetName() : Format("<unknown medium %v>", mediumIndex))
        .Item("available_space").Value(statistics.available_space())
        .Item("used_space").Value(statistics.used_space())
        .Item("slot_space_usages")
            .BeginAttributes()
                .Item("opaque").Value("true")
            .EndAttributes()
            .Value(statistics.slot_space_usages())
        .DoIf(statistics.has_error(), [&] (TFluentMap fluent) {
            fluent
                .Item("error").Value(statistics.error());
        });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNodeTrackerServer
