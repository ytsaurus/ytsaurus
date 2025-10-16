#ifndef STORED_CHUNK_REPLICA_INL_H_
#error "Direct inclusion of this file is not allowed, include stored_chunk_replica.h"
// For the sake of sane code completion.
#include "stored_chunk_replica.h"
#endif

namespace NYT::NChunkServer {

////////////////////////////////////////////////////////////////////////////////

template <>
struct TStoredReplicaTraits<EStoredReplicaType::ChunkLocation> {
    using Type = TAugmentedLocationChunkReplicaPtr;
};

template <>
struct TStoredReplicaTraits<EStoredReplicaType::OffshoreMedia> {
    using Type = TAugmentedMediumChunkReplicaPtr;
};

////////////////////////////////////////////////////////////////////////////////

template <class T>
TAugmentedStoredChunkReplicaPtr::TAugmentedStoredChunkReplicaPtr(T* ptr, int index, EChunkReplicaState replicaState)
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

template <EStoredReplicaType type>
const typename TStoredReplicaTraits<type>::Type* TAugmentedStoredChunkReplicaPtr::As() const
{
    if (GetStoredReplicaType() != type) {
        return nullptr;
    }

    YT_ASSERT(this);

    if constexpr (type == EStoredReplicaType::ChunkLocation) {
        return static_cast<const TAugmentedLocationChunkReplicaPtr*>(this);
    }
    else if constexpr (type == EStoredReplicaType::OffshoreMedia) {
        return static_cast<const TAugmentedMediumChunkReplicaPtr*>(this);
    }

    Y_UNREACHABLE();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
