#include "common.h"

#include "../misc/config.h"

namespace NYT {
namespace NChunkHolder {

////////////////////////////////////////////////////////////////////////////////

NLog::TLogger ChunkHolderLogger("ChunkHolder");

////////////////////////////////////////////////////////////////////////////////

void TChunkHolderConfig::Read(const TJsonObject* jsonConfig)
{
    if (jsonConfig == NULL)
        return;

    NYT::TryRead(jsonConfig, L"CacheCapacity", &CacheCapacity);
    NYT::TryRead(jsonConfig, L"Locations", &Locations);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkHolder
} // namespace NYT
