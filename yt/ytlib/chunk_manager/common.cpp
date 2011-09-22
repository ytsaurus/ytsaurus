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
double MinChunkBalancingLoadFactorDiff = 0.5;
double MinChunkBalancingLoadFactor = 0.0;

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkManager
} // namespace NYT

