#pragma once

#include "public.h"

namespace NYT::NZookeeper {

////////////////////////////////////////////////////////////////////////////////

struct TNodeStat
{
    i64 Czxid = 0;
    i64 Mzxid = 0;
    i64 Ctime = 0;
    i64 Mtime = 0;
    int Version = 0;
    int Cversion = 0;
    int Aversion = 0;
    i64 EphemeralOwner = 0;
    int DataLength = 0;
    int NumChildren = 0;
    i64 Pzxid = 0;

    void Serialize(IZookeeperProtocolWriter* writer);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NZookeeper
