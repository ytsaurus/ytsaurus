#pragma once

#include <yt/yt/client/table_client/public.h>

namespace NYT::NSequoiaClient {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(ESequoiaTable,
    (ChunkMetaExtensions)
    (ResolveNode)
);

namespace NRecords {

struct TChunkMetaExtensionsKey;
struct TChunkMetaExtensions;
struct TResolveNode;

} // namespace NRecords

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(ISequoiaTransaction)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSequoiaClient
