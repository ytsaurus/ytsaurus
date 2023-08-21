#include "location_directory.h"

#include <yt/yt/ytlib/data_node_tracker_client/proto/data_node_tracker_service.pb.h>

#include <yt/yt/core/misc/collection_helpers.h>
#include <yt/yt/core/misc/guid.h>
#include <yt/yt/core/misc/protobuf_helpers.h>

namespace NYT::NDataNodeTrackerClient {

using namespace NChunkClient;

using NYT::FromProto;

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////

bool IsUnique(auto container)
{
    Sort(container);
    return std::unique(container.begin(), container.end()) == container.end();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace

////////////////////////////////////////////////////////////////////////////////

TChunkLocationDirectory::TChunkLocationDirectory(int sizeHint)
{
    Uuids_.reserve(sizeHint);
}

int TChunkLocationDirectory::GetOrCreateIndex(TChunkLocationUuid uuid)
{
    int index = Find(Uuids_, uuid) - Uuids_.begin();

    if (index == std::ssize(Uuids_)) {
        Uuids_.push_back(uuid);
    }

    return index;
}

bool TChunkLocationDirectory::IsValid() const &
{
    return IsUnique(Uuids_);
}

bool TChunkLocationDirectory::IsValid() &&
{
    return IsUnique(std::move(Uuids_));
}

void ToProto(
    google::protobuf::RepeatedPtrField<NYT::NProto::TGuid>* serialized,
    const TChunkLocationDirectory& origin)
{
    ToProto(serialized, origin.Uuids());
}

void FromProto(
    TChunkLocationDirectory* origin,
    const google::protobuf::RepeatedPtrField<NYT::NProto::TGuid>& serialized)
{
    FromProto(&origin->Uuids_, serialized);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDataNodeTrackerClient
