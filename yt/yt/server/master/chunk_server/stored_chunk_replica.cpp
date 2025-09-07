#include "stored_chunk_replica.h"

#include "medium_base.h"
#include "chunk_location.h"

#include <yt/yt/server/master/node_tracker_server/node.h>

namespace NYT::NChunkServer {

using namespace NChunkClient;
using namespace NObjectServer;
using namespace NNodeTrackerClient;

////////////////////////////////////////////////////////////////////////////////

TStoredReplica::TStoredReplica()
{ }

TStoredReplica::TStoredReplica(const TChunkLocationPtrWithReplicaInfo& chunkLocation)
    : ReplicaInfo_(chunkLocation)
{
    YT_VERIFY(chunkLocation.GetPtr());
}

TStoredReplica::TStoredReplica(const TMediumPtrWithReplicaInfo& medium)
    : ReplicaInfo_(medium)
{
    YT_VERIFY(medium.GetPtr());
}

bool TStoredReplica::IsChunkLocation() const
{
    return std::holds_alternative<TChunkLocationPtrWithReplicaInfo>(ReplicaInfo_);
}

TChunkLocationPtrWithReplicaInfo& TStoredReplica::AsChunkLocation()
{
    auto* location = std::get_if<TChunkLocationPtrWithReplicaInfo>(&ReplicaInfo_);
    YT_VERIFY(location);
    return *location;
}

const TChunkLocationPtrWithReplicaInfo& TStoredReplica::AsChunkLocation() const
{
    const auto* location = std::get_if<TChunkLocationPtrWithReplicaInfo>(&ReplicaInfo_);
    YT_VERIFY(location);
    return *location;
}

bool TStoredReplica::IsMedium() const
{
    return std::holds_alternative<TMediumPtrWithReplicaInfo>(ReplicaInfo_);
}

TMediumPtrWithReplicaInfo& TStoredReplica::AsMedium()
{
    auto* medium = std::get_if<TMediumPtrWithReplicaInfo>(&ReplicaInfo_);
    YT_VERIFY(medium);
    return *medium;
}

const TMediumPtrWithReplicaInfo& TStoredReplica::AsMedium() const
{
    const auto* medium = std::get_if<TMediumPtrWithReplicaInfo>(&ReplicaInfo_);
    YT_VERIFY(medium);
    return *medium;
}

TStoredReplica TStoredReplica::ToGenericState() const
{
    return Visit(ReplicaInfo_,
        [] (const TChunkLocationPtrWithReplicaInfo& chunkLocation) -> TStoredReplica {
            return chunkLocation.ToGenericState();
        },
        [] (const TMediumPtrWithReplicaInfo& medium) -> TStoredReplica {
            return medium.ToGenericState();
        });
}

int TStoredReplica::GetEffectiveMediumIndex() const
{
    return Visit(ReplicaInfo_,
        [] (const TChunkLocationPtrWithReplicaInfo& chunkLocation) {
            auto* location = chunkLocation.GetPtr();
            return IsObjectAlive(location) ? location->GetEffectiveMediumIndex() : GenericMediumIndex;
        },
        [] (const TMediumPtrWithReplicaInfo& mediumLocation) {
            auto* medium = mediumLocation.GetPtr();
            return IsObjectAlive(medium) ? medium->GetIndex() : GenericMediumIndex;
        });
}

int TStoredReplica::GetReplicaIndex() const
{
    return Visit(ReplicaInfo_,
        [] (const TChunkLocationPtrWithReplicaInfo& chunkLocation) {
            return chunkLocation.GetReplicaIndex();
        },
        [] (const TMediumPtrWithReplicaInfo& medium) {
            return medium.GetReplicaIndex();
        });
}

EChunkReplicaState TStoredReplica::GetReplicaState() const
{
    return Visit(ReplicaInfo_,
        [] (const TChunkLocationPtrWithReplicaInfo& chunkLocation) {
            return chunkLocation.GetReplicaState();
        },
        [] (const TMediumPtrWithReplicaInfo& medium) {
            return medium.GetReplicaState();
        });
}

TChunkLocationIndex TStoredReplica::GetChunkLocationIndex() const
{
    return Visit(ReplicaInfo_,
        [] (const TChunkLocationPtrWithReplicaInfo& chunkLocation) {
            auto* location = chunkLocation.GetPtr();
            if (!IsObjectAlive(location)) {
                return InvalidChunkLocationIndex;
            }

            return location->GetIndex();
        },
        [] (const TMediumPtrWithReplicaInfo& /*medium*/) {
            return InvalidChunkLocationIndex;
        });
}

TChunkLocationUuid TStoredReplica::GetLocationUuid() const
{
    return Visit(ReplicaInfo_,
        [] (const TChunkLocationPtrWithReplicaInfo& chunkLocation) {
            auto* location = chunkLocation.GetPtr();
            if (!IsObjectAlive(location)) {
                return InvalidChunkLocationUuid;
            }

            return location->GetUuid();
        },
        [] (const TMediumPtrWithReplicaInfo& /*medium*/) {
            return InvalidChunkLocationUuid;
        });
}

TNodeId TStoredReplica::GetNodeId() const
{
    return Visit(ReplicaInfo_,
        [] (const TChunkLocationPtrWithReplicaInfo& chunkLocation) {
            auto* location = chunkLocation.GetPtr();
            if (!IsObjectAlive(location)) {
                return InvalidNodeId;
            }
            auto node = location->GetNode();
            if (!IsObjectAlive(node)) {
                return InvalidNodeId;
            }
            return node->GetId();
        },
        [] (const TMediumPtrWithReplicaInfo& /*medium*/) {
            return OffshoreNodeId;
        });
}

void TStoredReplica::Save(NCellMaster::TSaveContext& context) const
{
    using NYT::Save;

    Save(context, ReplicaInfo_);
}

void TStoredReplica::Load(NCellMaster::TLoadContext& context)
{
    using NYT::Load;

    if (context.GetVersion() < NCellMaster::EMasterReign::RefactoringAroundChunkStoredReplicas) {
        Load(context, AsChunkLocation());
    } else {
        Load(context, ReplicaInfo_);
    }
}

////////////////////////////////////////////////////////////////////////////////

void FormatValue(TStringBuilderBase* builder, TStoredReplica value, TStringBuf spec)
{
    Visit(value.ReplicaInfo_,
        [&] (const TChunkLocationPtrWithReplicaInfo& chunkLocation) {
            FormatValue(builder, chunkLocation, spec);
        },
        [&] (const TMediumPtrWithReplicaInfo& medium) {
            FormatValue(builder, medium, spec);
        });
}

void ToProto(ui64* protoValue, TStoredReplica value)
{
    Visit(value.ReplicaInfo_,
        [&] (const TChunkLocationPtrWithReplicaInfo& chunkLocation) {
            ToProto(protoValue, chunkLocation);
        },
        [&] (const TMediumPtrWithReplicaInfo& medium) {
            ToProto(protoValue, medium);
        });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
