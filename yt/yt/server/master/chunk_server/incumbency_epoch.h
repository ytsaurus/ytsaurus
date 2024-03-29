#include "public.h"

namespace NYT::NChunkServer {

////////////////////////////////////////////////////////////////////////////////

TIncumbencyEpoch GetIncumbencyEpoch(int shardIndex);
void SetIncumbencyEpoch(int shardIndex, TIncumbencyEpoch incumbencyEpoch);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
