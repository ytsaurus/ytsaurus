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

int MaxReplicationFanOut = 4;
int MaxReplicationFanIn = 8;
int MaxRemovalJobsPerHolder = 16;
TDuration ChunkRefreshDelay = TDuration::Seconds(15);
TDuration ChunkRefreshQuantum = TDuration::MilliSeconds(100);
int MaxChunksPerRefresh = 1000;
double MinChunkBalancingFillCoeffDiff = 0.2;
double MinChunkBalancingFillCoeff = 0.1;
double MaxHolderFillCoeff = 0.99;
i64 MinHolderFreeSpace = 10 * 1024 * 1024; // 10MB
double ActiveSessionsPenalityCoeff = 0.1;
TDuration MaxJobDuration = TDuration::Minutes(5);

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

