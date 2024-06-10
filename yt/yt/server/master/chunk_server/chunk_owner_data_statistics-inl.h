#ifndef CHUNK_OWNER_DATA_STATISTICS_
#error "Direct inclusion of this file is not allowed, include chunk_owner_data_statistics.h"
// For the sake of sane code completion.
#include "chunk_owner_data_statistics.h"
#endif
#undef CHUNK_OWNER_DATA_STATISTICS_

namespace NYT::NChunkServer {

using namespace NCypressServer;
using namespace NCellMaster;
using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

template <class TContext>
void TChunkOwnerDataStatistics::Save(TContext& context) const
{
    using NYT::Save;

    Save(context, UncompressedDataSize);
    Save(context, CompressedDataSize);
    Save(context, RowCount);
    Save(context, ChunkCount);
    Save(context, RegularDiskSpace);
    Save(context, ErasureDiskSpace);
    Save(context, DataWeight);
    Save(context, ChunkCount);
}

template <class TContext>
void TChunkOwnerDataStatistics::Load(TContext& context)
{
    using NYT::Load;

    Load(context, UncompressedDataSize);
    Load(context, CompressedDataSize);
    Load(context, RowCount);
    Load(context, ChunkCount);
    Load(context, RegularDiskSpace);
    Load(context, ErasureDiskSpace);
    Load(context, DataWeight);
    Load(context, ChunkCount);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
