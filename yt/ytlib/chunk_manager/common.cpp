#include "common.h"

namespace NYT {
namespace NChunkManager {

////////////////////////////////////////////////////////////////////////////////

NLog::TLogger ChunkManagerLogger("ChunkManager");

////////////////////////////////////////////////////////////////////////////////

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

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkManager
} // namespace NYT

