#include "structs.h"

#include "protocol.h"

namespace NYT::NZookeeper {

////////////////////////////////////////////////////////////////////////////////

void TNodeStat::Serialize(IZookeeperProtocolWriter* writer)
{
    writer->WriteLong(Czxid);
    writer->WriteLong(Mzxid);
    writer->WriteLong(Ctime);
    writer->WriteLong(Mtime);
    writer->WriteInt(Version);
    writer->WriteInt(Cversion);
    writer->WriteInt(Aversion);
    writer->WriteLong(EphemeralOwner);
    writer->WriteInt(DataLength);
    writer->WriteInt(NumChildren);
    writer->WriteLong(Pzxid);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NZookeeper
