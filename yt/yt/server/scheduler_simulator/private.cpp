#include "private.h"

#include <yt/ytlib/chunk_client/medium_directory.h>

#include <yt/ytlib/chunk_client/proto/medium_directory.pb.h>

namespace NYT::NSchedulerSimulator {

////////////////////////////////////////////////////////////////////////////////

const NLogging::TLogger SchedulerSimulatorLogger("Simulator");

////////////////////////////////////////////////////////////////////////////////

NChunkClient::TMediumDirectoryPtr CreateDefaultMediumDirectory()
{
    // TODO(ignat): support custom medium in simulator.
    NChunkClient::NProto::TMediumDirectory protoDirectory;
    auto* item = protoDirectory.add_items();
    item->set_name(NChunkClient::DefaultSlotsMediumName);
    item->set_index(NChunkClient::DefaultSlotsMediumIndex);
    item->set_priority(0);

    auto mediumDirectory = New<NChunkClient::TMediumDirectory>();
    mediumDirectory->LoadFrom(protoDirectory);
    return mediumDirectory;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler

