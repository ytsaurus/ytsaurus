#pragma once

#include "public.h"
#include "chunk_replica.h"

namespace NYT::NChunkServer {

////////////////////////////////////////////////////////////////////////////////

class TStoredReplica
{
public:
    TStoredReplica();
    TStoredReplica(const TChunkLocationPtrWithReplicaInfo& chunkLocation);
    TStoredReplica(const TMediumPtrWithReplicaInfo& medium);

    bool operator==(const TStoredReplica& other) const = default;
    bool operator<(const TStoredReplica& other) const;

    bool IsChunkLocation() const;
    TChunkLocationPtrWithReplicaInfo& AsChunkLocation();
    const TChunkLocationPtrWithReplicaInfo& AsChunkLocation() const;

    bool IsMedium() const;
    TMediumPtrWithReplicaInfo& AsMedium();
    const TMediumPtrWithReplicaInfo& AsMedium() const;

    TStoredReplica ToGenericState() const;

    int GetEffectiveMediumIndex() const;
    int GetReplicaIndex() const;

    EChunkReplicaState GetReplicaState() const;

    NNodeTrackerClient::TChunkLocationIndex GetChunkLocationIndex() const;
    NChunkClient::TChunkLocationUuid GetLocationUuid() const;
    NNodeTrackerClient::TNodeId GetNodeId() const;

    void Save(NCellMaster::TSaveContext& context) const;
    void Load(NCellMaster::TLoadContext& context);
private:
    std::variant<TChunkLocationPtrWithReplicaInfo, TMediumPtrWithReplicaInfo> ReplicaInfo_;

    friend void FormatValue(TStringBuilderBase* builder, TStoredReplica value, TStringBuf spec);
    friend void ToProto(ui64* protoValue, TStoredReplica value);
};

using TStoredReplicaList = TCompactVector<TStoredReplica, TypicalReplicaCount>;
using TChunkToStoredReplicaList = THashMap<TChunkId, TErrorOr<TStoredReplicaList>>;

////////////////////////////////////////////////////////////////////////////////

void FormatValue(TStringBuilderBase* builder, TStoredReplica value, TStringBuf spec);

//! Serializes node id, replica index, medium index.
void ToProto(ui64* protoValue, TStoredReplica value);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
