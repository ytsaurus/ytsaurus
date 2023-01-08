#pragma once

#include <yt/yt/client/table_client/public.h>

namespace NYT::NSequoiaClient {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(ESequoiaTable,
    (ChunkMetaExtensions)
);

namespace NRecords {

struct TChunkMetaExtensionsKey;
struct TChunkMetaExtensions;

} // namespace NRecords

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSequoiaClient
