#pragma once

#include "private.h"
#include "serialize.h"

#include <yt/server/chunk_pools/public.h>

#include <yt/ytlib/table_client/helpers.h>

namespace NYT {
namespace NControllerAgent {

////////////////////////////////////////////////////////////////////////////////

struct TEdgeDescriptor
{
    TEdgeDescriptor() = default;

    NChunkPools::IChunkPoolInput* DestinationPool = nullptr;
    bool RequiresRecoveryInfo = false;
    NTableClient::TTableWriterOptionsPtr TableWriterOptions;
    NTableClient::TTableUploadOptions TableUploadOptions;
    NYson::TYsonString TableWriterConfig;
    NTransactionClient::TTimestamp Timestamp;

    bool WriteToChunkList = true;
    // CellTag to allocate chunks list.
    NObjectClient::TCellTag CellTag;

    void Persist(const TPersistenceContext& context);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NControllerAgent
} // namespace NYT