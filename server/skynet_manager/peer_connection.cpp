#include "peer_connection.h"
#include "msgpack.h"
#include "private.h"

#include <util/string/hex.h>

#include <yt/core/concurrency/async_stream.h>
#include <yt/core/ytree/fluent.h>
#include <yt/core/net/connection.h>
#include <yt/core/misc/ref.h>

#ifdef __APPLE__
    #include <libkern/OSByteOrder.h>
    #define htobe32(x) OSSwapHostToBigInt32(x)
    #define be32toh(x) OSSwapBigToHostInt32(x)
#endif

namespace NYT {
namespace NSkynetManager {

using namespace NConcurrency;
using namespace NNet;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TPeerConnection::TPeerConnection(const IConnectionPtr& connection)
    : ConnectionId_(TGuid::Create())
    , Logger(SkynetManagerLogger)
    , Connection_(connection)
    , Input_(CreateSyncAdapter(connection))
    , Output_(CreateBufferedSyncAdapter(connection))
{
    Logger.AddTag("RemoteAddress: %v", connection->RemoteAddress());
}

void TPeerConnection::SendHandshake(const THandshake& handshake)
{
    Output_->Write("SKYBIT");
    SendRawMessage("SKYBIT_HANDSHAKE_1", BuildYsonNodeFluently()
        .BeginMap()
            .Item("version").Value(1)
            .Item("uid").Value(HexDecode(handshake.ResourceId))
            .Item("capabilities").BeginList()
                .Item().Value("skybit")
                .Item().Value("dfs")
            .EndList()
            .Item("world_uid_hex").Entity()
            .Item("world_uid_str").Value(handshake.PeerId)
            .Item("world_desc").Value(handshake.PeerName)
        .EndMap());
}

void TPeerConnection::SendPing()
{
    SendRawMessage("PING", BuildYsonNodeFluently().Value("PING"));
}

void TPeerConnection::SendHasResource()
{
    SendRawMessage("HEAD_HAS", BuildYsonNodeFluently().Value(1));
}

void TPeerConnection::SendResource(const INodePtr& node)
{
    SendRawMessage("HEAD", node);
}

void TPeerConnection::SendLinks(const INodePtr& node)
{
    SendRawMessage("LINKS", node);
}

THandshake TPeerConnection::ReceiveHandshake()
{
    std::array<char, 6> magic;
    Input_->LoadOrFail(reinterpret_cast<void*>(magic.data()), magic.size());
    if (magic != std::array<char, 6>{'S', 'K', 'Y', 'B', 'I', 'T'}) {
        THROW_ERROR_EXCEPTION("Invalid magic")
            << TErrorAttribute("connection_id", ConnectionId_);
    }

    auto msg = ReceiveRawMessage();
    if (msg.first != "SKYBIT_HANDSHAKE_1") {
        THROW_ERROR_EXCEPTION("Unsupported handshake type")
            << TErrorAttribute("type", msg.first)
            << TErrorAttribute("connection_id", ConnectionId_)
            << TErrorAttribute("msg", msg.second);
    }

    THandshake handshake;
    auto rawResourceId = ConvertTo<TString>(msg.second->AsMap()->FindChild("uid"));
    handshake.ResourceId = to_lower(HexEncode(rawResourceId));
    handshake.PeerId = ConvertTo<TString>(msg.second->AsMap()->FindChild("world_uid_str"));
    handshake.PeerName = ConvertTo<TString>(msg.second->AsMap()->FindChild("world_desc"));
    return handshake;
}

EPeerMessage TPeerConnection::ReceiveMessage()
{
    auto type = ReceiveRawMessage().first;
    if (type == "HEAD_WANT") {
        return EPeerMessage::WantResource;
    } else if (type == "HEAD_HAS") {
        return EPeerMessage::HasResource;
    } else if (type == "PING") {
        return EPeerMessage::Ping;
    } else {
        LOG_WARNING("Ignoring unknown message (Type: %s)", type);
        return EPeerMessage::Unknown;
    }
}

std::pair<TString, INodePtr> TPeerConnection::ReceiveRawMessage()
{
    const i32 MaxSize = 16_MB;
    ui32 size;
    Input_->LoadOrFail(&size, sizeof(size));
    size = be32toh(size);
    if (size > MaxSize) {
        THROW_ERROR_EXCEPTION("Invalid size")
            << TErrorAttribute("size", size);
    }
    auto buffer = TSharedMutableRef::Allocate(size);
    Input_->LoadOrFail(buffer.Begin(), buffer.Size());
    auto msg = ParseFromMsgpack(buffer);
    auto type = ConvertTo<TString>(msg->AsList()->FindChild(0));
    LOG_DEBUG("Received message from peer (Type: %s)", type);
    return {
        type,
        msg->AsList()->FindChild(1)
    };
}

void TPeerConnection::SendRawMessage(const TString& type, const INodePtr& msg)
{
    LOG_DEBUG("Sending message to peer (Type: %s)", type);
    auto buffer = SerializeToMsgpack(BuildYsonNodeFluently()
        .BeginList()
            .Item().Value(type)
            .Item().Value(msg)
        .EndList());

    ui32 size = htobe32(buffer.Size());
    Output_->Write(&size, sizeof(size));
    Output_->Write(buffer.Begin(), buffer.Size());
    Output_->Flush();
}

const TNetworkAddress& TPeerConnection::PeerAddress() const
{
    return Connection_->RemoteAddress();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NSkynetManager
} // namespace NYT
