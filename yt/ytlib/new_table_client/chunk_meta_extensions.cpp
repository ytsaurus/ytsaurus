#include "stdafx.h"

#include "chunk_meta_extensions.h"

#include <ytlib/table_client/chunk_meta_extensions.h>

namespace NYT {
namespace NVersionedTableClient {

using namespace NProto;

using NTableClient::NProto::TOldBoundaryKeysExt;
using NChunkClient::NProto::TChunkMeta;

////////////////////////////////////////////////////////////////////////////////

void GetBoundaryKeys(const TChunkMeta& chunkMeta, TOwningKey* minKey, TOwningKey* maxKey)
{
    if (chunkMeta.version() == ETableChunkFormat::Old) {
        auto boundaryKeys = GetProtoExtension<TOldBoundaryKeysExt>(chunkMeta.extensions());
        FromProto(minKey, boundaryKeys.start());
        FromProto(maxKey, boundaryKeys.end());
    } else {
        auto boundaryKeys = GetProtoExtension<TBoundaryKeysExt>(chunkMeta.extensions());
        FromProto(minKey, boundaryKeys.min());
        FromProto(maxKey, boundaryKeys.max());
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NVersionedTableClient
} // namespace NYT
