#include "public.h"

#include <yt/yt/ytlib/table_client/public.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <>
void Save(NTabletNode::TSaveContext& context, const NTableClient::THunkChunkRef& hunkChunkRef);

template <>
void Load<NTableClient::THunkChunkRef, NTabletNode::TLoadContext>(
    NTabletNode::TLoadContext& context,
    NTableClient::THunkChunkRef& hunkChunkRef);

template <>
void Save(NTabletNode::TSaveContext& context, const NTableClient::THunkChunksInfo& hunkChunkInfo);

template <>
void Load<NTableClient::THunkChunksInfo, NTabletNode::TLoadContext>(
    NTabletNode::TLoadContext& context,
    NTableClient::THunkChunksInfo& hunkChunkInfo);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
