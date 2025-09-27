#include "stored_chunk_replica.h"

#include "medium_base.h"
#include "chunk_location.h"

#include <yt/yt/server/master/node_tracker_server/node.h>

namespace NYT::NChunkServer {

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

bool TStoredReplica::operator<(const TStoredReplica& other) const
{
    return ReplicaInfo_ < other.ReplicaInfo_;
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
            return chunkLocation.GetPtr()->GetEffectiveMediumIndex();
        },
        [] (const TMediumPtrWithReplicaInfo& medium) {
            return medium.GetPtr()->GetIndex();
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

NNodeTrackerClient::TChunkLocationIndex TStoredReplica::GetChunkLocationIndex() const
{
    return Visit(ReplicaInfo_,
        [] (const TChunkLocationPtrWithReplicaInfo& chunkLocation) {
            auto* location = chunkLocation.GetPtr();
            if (!NObjectServer::IsObjectAlive(location)) {
                return NNodeTrackerClient::InvalidChunkLocationIndex;
            }

            return location->GetIndex();
        },
        [] (const TMediumPtrWithReplicaInfo& /*medium*/) {
            return NNodeTrackerClient::InvalidChunkLocationIndex;
        });
}

NChunkClient::TChunkLocationUuid TStoredReplica::GetLocationUuid() const
{
    return Visit(ReplicaInfo_,
        [] (const TChunkLocationPtrWithReplicaInfo& chunkLocation) {
            auto* location = chunkLocation.GetPtr();
            if (!NObjectServer::IsObjectAlive(location)) {
                return NChunkClient::InvalidChunkLocationUuid;
            }

            return location->GetUuid();
        },
        [] (const TMediumPtrWithReplicaInfo& /*medium*/) {
            return NChunkClient::InvalidChunkLocationUuid;
        });
}

NNodeTrackerClient::TNodeId TStoredReplica::GetNodeId() const
{
    return Visit(ReplicaInfo_,
        [] (const TChunkLocationPtrWithReplicaInfo& chunkLocation) {
            auto* location = chunkLocation.GetPtr();
            if (!NObjectServer::IsObjectAlive(location)) {
                return NNodeTrackerClient::InvalidNodeId;
            }
            auto node = location->GetNode();
            if (!NObjectServer::IsObjectAlive(node)) {
                return NNodeTrackerClient::InvalidNodeId;
            }
            return node->GetId();
        },
        [] (const TMediumPtrWithReplicaInfo& /*medium*/) {
            return NNodeTrackerClient::OffshoreNodeId;
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
