#pragma once

#include <yt/yt/client/table_client/public.h>

namespace NYT::NSequoiaClient {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(ESequoiaTable,
    (ChunkMetaExtensions)
    (ResolveNode)
    (ChunkReplicas)
    (LocationReplicas)
    (ReverseResolveNode)
);

namespace NRecords {

struct TChunkMetaExtensionsKey;
struct TChunkMetaExtensions;

struct TResolveNode;
struct TReverseResolveNode;

struct TChunkReplicas;
struct TLocationReplicas;

} // namespace NRecords

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(ISequoiaClient)
DECLARE_REFCOUNTED_STRUCT(ISequoiaTransaction)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSequoiaClient
