#pragma once

#include "vchunk_config.h"

#include <contrib/ydb/core/nbs/cloud/blockstore/libs/common/block_range.h>
#include <contrib/ydb/core/nbs/cloud/blockstore/libs/service/volume_config.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

size_t GetRegionIndex(const TVolumeConfig& volumeConfig, TBlockRange64 range);

TBlockRange64 TranslateToRegion(
    const TVolumeConfig& volumeConfig,
    TBlockRange64 range);

size_t GetVChunkIndex(
    const TVolumeConfig& volumeConfig,
    TBlockRange64 regionRange);

TBlockRange64 TranslateToVChunk(
    const TVolumeConfig& volumeConfig,
    TBlockRange64 regionRange);

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
