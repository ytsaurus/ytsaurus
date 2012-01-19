#include "stdafx.h"
#include "common.h"

#include <ytlib/misc/id_generator.h>

namespace NYT {
namespace NChunkServer {

////////////////////////////////////////////////////////////////////////////////

NLog::TLogger ChunkServerLogger("ChunkServer");

TChunkListId NullChunkListId = TChunkListId(0, 0, 0, 0);
TChunkTreeId NullChunkTreeId = TChunkListId(0, 0, 0, 0);

ui64 ChunkIdSeed = 0x7390bac62f716a19;
ui64 ChunkListIdSeed = 0x761ba739c541fcd0;

////////////////////////////////////////////////////////////////////////////////

EChunkTreeKind GetChunkTreeKind(const TChunkTreeId& treeId)
{
    return
        TIdGenerator<TGuid>::IsValid(treeId, ChunkIdSeed)
        ? EChunkTreeKind::Chunk
        : EChunkTreeKind::ChunkList;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT

