#ifndef STORED_CHUNK_REPLICA_INL_H_
#error "Direct inclusion of this file is not allowed, include stored_chunk_replica.h"
// For the sake of sane code completion.
#include "stored_chunk_replica.h"
#endif

// COMPAT(cherepashka): remove this include after RefactoringAroundChunkStoredReplicas will be dropped.
#include "chunk_replica.h"

#include <yt/yt/server/master/cell_master/serialize.h>

namespace NYT::NChunkServer {

////////////////////////////////////////////////////////////////////////////////

template <class T>
TStoredChunkReplicaPtrWithReplicaInfo::TStoredChunkReplicaPtrWithReplicaInfo(T* ptr, int index, EChunkReplicaState replicaState)
    requires ((std::is_same_v<T, TChunkLocation> || std::is_same_v<T, TMedium>))
: Value_(
    reinterpret_cast<uintptr_t>(ptr) |
    static_cast<uintptr_t>(replicaState) |
    (static_cast<uintptr_t>(index) << 56) |
    (static_cast<uintptr_t>((std::is_same_v<T, TChunkLocation> ? EStoredReplicaType::ChunkLocation : EStoredReplicaType::OffshoreMedia)) << 48))
{
    YT_ASSERT((reinterpret_cast<uintptr_t>(ptr) & 0xffff000000000003LL) == 0);
    YT_ASSERT(index >= 0 && index <= 0xff);
    YT_ASSERT(static_cast<uintptr_t>(replicaState) <= 0x3);
}

template <class C>
void TStoredChunkReplicaPtrWithReplicaInfo::Save(C& context) const
{
    using NYT::Save;
    auto type = GetStoredReplicaType();
    Save(context, type);
    Save(context, GetReplicaIndex());
    Save(context, GetReplicaState());

    switch (type) {
        case EStoredReplicaType::ChunkLocation: {
            SaveWith<NCellMaster::TRawNonversionedObjectPtrSerializer>(context, AsChunkLocationPtr());
            break;
        }
        case EStoredReplicaType::OffshoreMedia: {
            SaveWith<NCellMaster::TRawNonversionedObjectPtrSerializer>(context, AsMediumPtr());
            break;
        }
    }
}

template <class C>
void TStoredChunkReplicaPtrWithReplicaInfo::Load(C& context)
{
    using NYT::Load;

    if (context.GetVersion() < NCellMaster::EMasterReign::RefactoringAroundChunkStoredReplicas) {
        auto chunkLocation = Load<TChunkLocationPtrWithReplicaInfo>(context);
        *this = TStoredChunkReplicaPtrWithReplicaInfo(chunkLocation.GetPtr(), chunkLocation.GetReplicaIndex(), chunkLocation.GetReplicaState());
    } else {
        auto type = Load<EStoredReplicaType>(context);
        int index = Load<ui8>(context);
        auto state = Load<EChunkReplicaState>(context);
        switch (type) {
            case EStoredReplicaType::ChunkLocation: {
                auto* ptr = LoadWith<NCellMaster::TRawNonversionedObjectPtrSerializer, TChunkLocation*>(context);
                *this = TStoredChunkReplicaPtrWithReplicaInfo(ptr, index,state);
                break;
            }
            case EStoredReplicaType::OffshoreMedia: {
                auto* ptr = LoadWith<NCellMaster::TRawNonversionedObjectPtrSerializer, TMedium*>(context);
                *this = TStoredChunkReplicaPtrWithReplicaInfo(ptr, index,state);
                break;
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
