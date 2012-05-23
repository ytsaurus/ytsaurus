#pragma once

#include "common.h"
#include "message.h"

namespace NYT {
namespace NBus {

////////////////////////////////////////////////////////////////////////////////

#pragma pack(push, 4)

struct TPacketHeader
{
    static const ui32 ExpectedSignature = 0x78616d6f;

    DECLARE_ENUM(EType,
        (Message)
        (Ping)
        (Ack)
        (BrokenSession)
    );

    ui32 Signature;
    EType Type;
    TSessionId SessionId;
};

struct TMultipartPacketHeader
    : public TPacketHeader
{
    static const i32 MaxParts = 1 << 28;
    static const i32 MaxPartSize = 1 << 28;

    TSequenceId SequenceId;
    i32 PartCount;
    i32 PartSizes[MaxParts];
};

#pragma pack(pop)

////////////////////////////////////////////////////////////////////////////////

template <class T>
struct THeaderTraits
{ };

template<>
struct THeaderTraits<TPacketHeader>
{
    static const int FixedSize;
};

template<>
struct THeaderTraits<TMultipartPacketHeader>
{
    static const int FixedSize;
};

////////////////////////////////////////////////////////////////////////////////

bool DecodeMessagePacket(
    TBlob&& data,
    IMessage::TPtr* message,
    TSequenceId* sequenceId);

bool EncodeMessagePacket(
    IMessage::TPtr message,
    const TSessionId& sessionId,
    TSequenceId sequenceId,
    TBlob* data);

void CreatePacket(
    const TSessionId& sessionId,
    TPacketHeader::EType type,
    TBlob* data);

////////////////////////////////////////////////////////////////////////////////

template <class T>
T* ParsePacketHeader(TBlob& data)
{
    static NLog::TLogger& Logger = BusLogger;

    if (data.ysize() < THeaderTraits<T>::FixedSize) {
        LOG_ERROR("Packet is too short (Size: %d)", data.ysize());
        return NULL;
    }

    T* header = reinterpret_cast<T*>(data.begin());
    if (header->Signature != TPacketHeader::ExpectedSignature) {
        LOG_ERROR("Invalid packet signature (Signature: %X)", header->Signature);
        return NULL;
    }

    return header;
}

////////////////////////////////////////////////////////////////////////////////


} // namespace NBus
} // namespace NYT
