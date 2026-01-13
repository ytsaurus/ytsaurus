#include "private.h"

#include <yt/yt/ytlib/chunk_client/medium_directory.h>

#include <yt/yt/ytlib/chunk_client/proto/medium_directory.pb.h>

namespace NYT::NSchedulerSimulator {

////////////////////////////////////////////////////////////////////////////////

NChunkClient::TMediumDirectoryPtr CreateDefaultMediumDirectory()
{
    // TODO(ignat): support custom medium in simulator.
    NChunkClient::NProto::TMediumDirectory protoDirectory;
    auto* protoMediumDescriptor = protoDirectory.add_medium_descriptors();
    protoMediumDescriptor->set_name(NChunkClient::DefaultSlotsMediumName);
    protoMediumDescriptor->set_index(NChunkClient::DefaultSlotsMediumIndex);
    protoMediumDescriptor->set_priority(0);

    auto mediumDirectory = New<NChunkClient::TMediumDirectory>();
    mediumDirectory->LoadFrom(protoDirectory);
    return mediumDirectory;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSchedulerSimulator

