#include "chunk_pool_factory.h"

namespace NYT::NChunkPools {

////////////////////////////////////////////////////////////////////////////////

Y_WEAK IChunkPoolPtr CreateChunkPool(NTableClient::EPartitionMode /* partitionMode */, i64 /* dataWeightPerJob */, const NLogging::TLogger& /* logger */) {
    THROW_ERROR_EXCEPTION("Partition table functionality is not linked in");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkPools
