#include "commands.h"

#include "protocol.h"

namespace NYT::NZookeeper {

////////////////////////////////////////////////////////////////////////////////

void TReqStartSession::Deserialize(IZookeeperProtocolReader* reader)
{
    ProtocolVersion = reader->ReadInt();
    LastZxidSeen = reader->ReadLong();
    Timeout = TDuration::MilliSeconds(reader->ReadInt());
    SessionId = reader->ReadLong();
    Password = reader->ReadString();
    ReadOnly = reader->ReadBool();
}

void TRspStartSession::Serialize(IZookeeperProtocolWriter* writer) const
{
    writer->WriteInt(ProtocolVersion);
    writer->WriteInt(Timeout.MilliSeconds());
    writer->WriteLong(SessionId);
    writer->WriteString(Password);
    writer->WriteBool(ReadOnly);
}

////////////////////////////////////////////////////////////////////////////////

void TReqPing::Deserialize(IZookeeperProtocolReader* /*reader*/)
{ }

void TRspPing::Serialize(IZookeeperProtocolWriter* /*writer*/) const
{ }

////////////////////////////////////////////////////////////////////////////////

void TReqGetChildren2::Deserialize(IZookeeperProtocolReader* reader)
{
    Path = reader->ReadString();
    Watch = reader->ReadBool();
}

void TRspGetChildren2::Serialize(IZookeeperProtocolWriter* writer)
{
    writer->WriteInt(Children.size());
    for (const auto& child : Children) {
        writer->WriteString(child);
    }

    Stat.Serialize(writer);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NZookeeper
