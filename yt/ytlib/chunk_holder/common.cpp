#include "stdafx.h"
#include "common.h"

#include "../misc/config.h"

namespace NYT {
namespace NChunkHolder {

////////////////////////////////////////////////////////////////////////////////

NLog::TLogger ChunkHolderLogger("ChunkHolder");

////////////////////////////////////////////////////////////////////////////////

// TODO: read everything
void TChunkHolderConfig::Read(TJsonObject* json)
{
    if (json == NULL)
        return;

    TJsonObject* mastersJson = GetSubTree(json, "Masters");
    if (mastersJson != NULL) {
        Masters.Read(mastersJson);
    }
    NYT::TryRead(json, L"MaxCachedFiles", &MaxCachedFiles);
    NYT::TryRead(json, L"MaxCachedBlocks", &MaxCachedBlocks);
    NYT::TryRead(json, L"Locations", &Locations);

    int maxChunksSpace;
    if (NYT::TryRead(json, L"MaxChunksSpace", &maxChunksSpace)) {
        MaxChunksSpace = maxChunksSpace;
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkHolder
} // namespace NYT
