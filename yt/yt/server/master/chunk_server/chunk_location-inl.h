#ifndef CHUNK_LOCATION_INL_H_
#error "Direct inclusion of this file is not allowed, include chunk_location.h"
// For the sake of sane code completion.
#include "chunk_location.h"
#endif

#include <yt/yt/server/master/cell_master/serialize.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class C>
void TSerializerTraits<NChunkServer::TChunkLocationRawPtr, C>::TSerializer::Save(
    NCellMaster::TSaveContext& context,
    NChunkServer::TChunkLocationRawPtr location)
{
    SaveWith<NCellMaster::TRawNonversionedObjectPtrSerializer>(context, location);
}

template <class C>
void TSerializerTraits<NChunkServer::TChunkLocationRawPtr, C>::TSerializer::Load(
    NCellMaster::TLoadContext& context,
    NChunkServer::TChunkLocationRawPtr& location)
{
    LoadWith<NCellMaster::TRawNonversionedObjectPtrSerializer>(context, location);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
