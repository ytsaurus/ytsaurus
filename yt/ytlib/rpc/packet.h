#pragma once

#include "common.h"
#include "message.h"

namespace NYT {
namespace NRpc {

////////////////////////////////////////////////////////////////////////////////

#pragma pack(push, 4)

struct TPacketHeader
{
    static const ui32 ExpectedSignature = 0x78616d6f;

    enum EType
    {
        Message,
        Ping,
        Ack
    };

    ui32 Signature;
    i32 Type;
    TSessionId SessionId;

    static int FixedSize;
};

struct TMultipartPacketHeader
    : public TPacketHeader
{
    static const i32 MaxParts = 1 << 14;
    static const i32 MaxPartSize = 1 << 24;

    TSequenceId SequenceId;
    i32 PartCount;
    i32 PartSizes[MaxParts];

    static int FixedSize;
};

#pragma pack(pop)


////////////////////////////////////////////////////////////////////////////////

bool DecodeMessagePacket(
    TBlob& data,
    IMessage::TPtr* message,
    TSequenceId* sequenceId);

bool EncodeMessagePacket(
    IMessage::TPtr message,
    const TSessionId& sessionId,
    TSequenceId sequenceId,
    TBlob* data);

void CreatePacket(const TSessionId& sessionId, TPacketHeader::EType type, TBlob* data);


////////////////////////////////////////////////////////////////////////////////

template <class T>
T* ParsePacketHeader(TBlob& data)
{
    if (data.ysize() < T::FixedSize) {
        //LOG_ERROR("Packet is too short (Size: %d)", data.ysize());
        return NULL;
    }

    T* header = reinterpret_cast<T*>(data.begin());
    if (header->Signature != TPacketHeader::ExpectedSignature) {
        //LOG_ERROR("Invalid packet signature (Signature: %X)", header->Signature);
        return NULL;
    }

    return header;
}

////////////////////////////////////////////////////////////////////////////////


} // namespace NRpc
} // namespace NYT
