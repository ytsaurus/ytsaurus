#include "common.h"

#include "../misc/config.h"

namespace NYT {
namespace NChunkHolder {

////////////////////////////////////////////////////////////////////////////////

NLog::TLogger ChunkHolderLogger("Chunk");

////////////////////////////////////////////////////////////////////////////////

void TChunkHolderConfig::Read(const TJsonObject* jsonConfig)
{
    if (jsonConfig == NULL)
        return;

    NYT::TryRead(jsonConfig, L"MaxCachedFiles", &MaxCachedFiles);
    NYT::TryRead(jsonConfig, L"MaxCachedBlocks", &MaxCachedBlocks);
    NYT::TryRead(jsonConfig, L"Locations", &Locations);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkHolder
} // namespace NYT
