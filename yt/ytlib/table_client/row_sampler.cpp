#include "row_sampler.h"

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IRowSampler> CreateChunkRowSampler(
    const NChunkClient::TChunkId& chunkId,
    double rate,
    ui64 seed)
{
    seed ^= FarmFingerprint(chunkId.Parts64[0]);
    seed ^= FarmFingerprint(chunkId.Parts64[1]);
    return std::make_unique<TFarmhashRowSampler<std::minstd_rand0>>(rate, seed);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient::NYT
