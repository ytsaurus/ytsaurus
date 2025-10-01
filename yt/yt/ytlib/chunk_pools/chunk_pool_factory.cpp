#include "chunk_pool_factory.h"

namespace NYT::NChunkPools {

////////////////////////////////////////////////////////////////////////////////

Y_WEAK IChunkPoolPtr CreateChunkPool(
    NTableClient::ETablePartitionMode /*partitionMode*/,
    i64 /*dataWeightPerJob*/,
    std::optional<int> /*maxPartitionCount*/,
    NLogging::TLogger /*logger*/)
{
    THROW_ERROR_EXCEPTION("Partition table functionality is not linked in");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkPools
