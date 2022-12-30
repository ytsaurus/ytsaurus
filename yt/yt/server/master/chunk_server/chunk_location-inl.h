#ifndef CHUNK_LOCATION_INL_H_
#error "Direct inclusion of this file is not allowed, include chunk_location.h"
// For the sake of sane code completion.
#include "chunk_location.h"
#endif

namespace NYT::NChunkServer {

////////////////////////////////////////////////////////////////////////////////

template <bool WithReplicaState, int IndexCount, template <class> class TAugmentationAccessor>
NNodeTrackerServer::TNode* GetChunkLocationNode(
    TAugmentedPtr<TChunkLocation, WithReplicaState, IndexCount, TAugmentationAccessor> ptr)
{
    return ptr.GetPtr()->GetNode();
}

template <bool WithReplicaState, int IndexCount, template <class> class TAugmentationAccessor>
NNodeTrackerServer::TNodeId GetChunkLocationNodeId(
    TAugmentedPtr<TChunkLocation, WithReplicaState, IndexCount, TAugmentationAccessor> ptr)
{
    return NDetail::GetNodeId(GetChunkLocationNode(ptr));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class C>
void TSerializerTraits<NChunkServer::TChunkLocation*, C>::TSerializer::Save(
    C& context,
    const NChunkServer::TChunkLocation* location)
{
    using NYT::Save;
    using namespace NChunkServer;
    Save(context, location->IsImaginary());
    if (location->IsImaginary()) {
        Save(context, location->GetNode());
        Save(context, location->GetEffectiveMediumIndex());
    } else {
        Save(context, location->AsReal());
    }
}

template <class C>
void TSerializerTraits<NChunkServer::TChunkLocation*, C>::TSerializer::Load(
    C& context,
    NChunkServer::TChunkLocation*& location)
{
    using NYT::Load;
    using namespace NChunkServer;
    using namespace NNodeTrackerServer;
    if (Load<bool>(context)) {
        // Imaginary chunk location.
        auto* node = Load<TNode*>(context);
        auto mediumIndex = Load<int>(context);
        location = TImaginaryChunkLocation::GetOrCreate(node, mediumIndex, /*duringSnapshotLoading*/ true);
    } else {
        // Real chunk location.
        location = Load<TRealChunkLocation*>(context);
    }
}

template <class C>
bool TSerializerTraits<NChunkServer::TChunkLocation*, C>::TComparer::Compare(
    const NChunkServer::TChunkLocation* lhs,
    const NChunkServer::TChunkLocation* rhs)
{
    using namespace NChunkServer;
    if (lhs->IsImaginary() != rhs->IsImaginary()) {
        // Imaginary locations are less then real ones.
        return lhs->IsImaginary();
    }

    if (lhs == rhs) {
        return false;
    }

    if (lhs->IsImaginary()) {
        return *lhs->AsImaginary() < *rhs->AsImaginary();
    } else {
        return *lhs->AsReal() < *rhs->AsReal();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
