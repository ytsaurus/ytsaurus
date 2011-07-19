#include "common.h"

#include "../misc/config.h"

namespace NYT {
namespace NChunkHolder {

////////////////////////////////////////////////////////////////////////////////

NLog::TLogger ChunkHolderLogger("Chunk");

////////////////////////////////////////////////////////////////////////////////

// TODO: read everything
void TChunkHolderConfig::Read(const TJsonObject* json)
{
    if (json == NULL)
        return;

    NYT::TryRead(json, L"MaxCachedFiles", &MaxCachedFiles);
    NYT::TryRead(json, L"MaxCachedBlocks", &MaxCachedBlocks);
    NYT::TryRead(json, L"Locations", &Locations);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkHolder
} // namespace NYT
