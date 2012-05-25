#include "stdafx.h"
#include "packet.h"
#include "message.h"

#include <ytlib/misc/foreach.h>

namespace NYT {
namespace NBus {

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = BusLogger;

////////////////////////////////////////////////////////////////////////////////

TPacketDecoder::TPacketDecoder()
{
    Restart();
}

void TPacketDecoder::Restart()
{
    Phase = EPacketPhase::Header;
    PacketSize = 0;
    PartSizes.clear();
    Parts.clear();
    PartCount = 0;
    PartIndex = -1;
    Message.Reset();

    BeginPhase(EPacketPhase::Header, &Header, sizeof (TPacketHeader));
}

bool TPacketDecoder::Advance(size_t size)
{
    YASSERT(PendingSize != 0);
    YASSERT(size <= PendingSize);

    PendingSize -= size;
    Chunk += size;
    if (PendingSize == 0) {
        return EndPhase();
    } else {
        return true;
    }
}

EPacketType TPacketDecoder::GetPacketType() const
{
    return Header.Type;
}

const TPacketId& TPacketDecoder::GetPacketId() const
{
    return Header.PacketId;
}

IMessagePtr TPacketDecoder::GetMessage() const
{
    return Message;
}

size_t TPacketDecoder::GetPacketSize() const
{
    return PacketSize;
}

bool TPacketDecoder::EndHeaderPhase()
{
    if (Header.Signature != PacketSignature) {
        LOG_ERROR("Packet header signature mismatch: expected: %X, actual: %X",
            PacketSignature,
            Header.Signature);
        return false;
    }

    switch (Header.Type) {
        case EPacketType::Message:
            BeginPhase(EPacketPhase::PartCount, &PartCount, sizeof (i32));
            return true;
        
        case EPacketType::Ack:
            SetFinished();
            return true;

        default:
            LOG_ERROR("Invalid packet type %d", Header.Type.ToValue());
            return false;
    }
}

bool TPacketDecoder::EndPartCountPhase()
{
    if (PartCount < 0 || PartCount > MaxPacketPartCount) {
        LOG_ERROR("Invalid part count %d", PartCount);
        return false;
    }

    PartSizes.resize(PartCount);
    BeginPhase(EPacketPhase::PartSizes, &*PartSizes.begin(), PartCount * sizeof (i32));
    return true;
}

bool TPacketDecoder::EndPartSizesPhase()
{
    PacketSize =
        sizeof (TPacketHeader) + // header
        sizeof (i32) + // PartCount
        PartCount * sizeof (i32); // PartSizes
    for (int index = 0; index < PartCount; ++index) {
        i32 partSize = PartSizes[index];
        if (partSize < 0 || partSize > MaxPacketPartSize) {
            LOG_ERROR("Invalid size %d of part %d",
                partSize,
                index);
            return false;
        }
        PacketSize += partSize;
    }

    NextMessagePartPhase();
    return true;
}

bool TPacketDecoder::EndMessagePartPhase()
{
    NextMessagePartPhase();
    return true;
}

void TPacketDecoder::NextMessagePartPhase()
{
    YASSERT(PartIndex < PartCount);

    while (true) {
        ++PartIndex;
        if (PartIndex == PartCount) {
            break;
        }

        i32 partSize = PartSizes[PartIndex];
        if (partSize > 0) {
            TBlob blob(partSize);
            TSharedRef part(MoveRV(blob));
            BeginPhase(EPacketPhase::MessagePart, &*part.Begin(), part.Size());
            Parts.push_back(part);
            return;
        }

        Parts.push_back(TSharedRef());
    }

    Message = CreateMessageFromParts(MoveRV(Parts));
    SetFinished();
}

////////////////////////////////////////////////////////////////////////////////

TPacketEncoder::TPacketEncoder()
{
    Phase = EPacketPhase::Unstarted;
    Header.Signature = PacketSignature;
}

size_t TPacketEncoder::GetPacketSize(
    EPacketType type,
    IMessagePtr message)
{
    size_t size = sizeof (TPacketHeader);
    switch (type) {
        case EPacketType::Ack:
            break;

        case EPacketType::Message: {
            size += sizeof (i32); // PartCount
            FOREACH (const auto& part, message->GetParts()) {
                size += sizeof (i32); // PartSize
                size += part.Size();
            }
            break;
        }

        default:
            YUNREACHABLE();
    }
    return size;
}

bool TPacketEncoder::Start(
    EPacketType type,
    const TPacketId& packetId,
    IMessagePtr message)
{
    Header.Type = type;
    Header.PacketId = packetId;

    PartSizes.clear();
    PartCount = 0;
    PartIndex = -1;
    Message = message;

    if (type == EPacketType::Message) {
        const auto& parts = message->GetParts();
        PartCount = static_cast<i32>(parts.size());

        if (PartCount > MaxPacketPartCount) {
            LOG_ERROR("Invalid part count %d", PartCount);
            return false;
        }

        for (int index = 0; index < static_cast<int>(parts.size()); ++index) {
            const auto& part = parts[index];
            int partSize = static_cast<int>(part.Size());
            if (partSize > MaxPacketPartSize) {
                LOG_ERROR("Invalid size %d of part %d",
                    partSize,
                    index);
                return false;
            }

            PartSizes.push_back(partSize);
        }
    }

    BeginPhase(EPacketPhase::Header, &Header, sizeof (TPacketHeader));
    return true;
}

void TPacketEncoder::NextChunk()
{
    EndPhase();
}

bool TPacketEncoder::EndHeaderPhase()
{
    switch (Header.Type) {
        case EPacketType::Message:
            BeginPhase(EPacketPhase::PartCount, &PartCount, sizeof (i32));
            return true;
        
        case EPacketType::Ack:
            SetFinished();
            return true;

        default:
            YUNREACHABLE();
    }
}

bool TPacketEncoder::EndPartCountPhase()
{
    BeginPhase(EPacketPhase::PartSizes, &*PartSizes.begin(), PartCount * sizeof (i32));
    return true;
}

bool TPacketEncoder::EndPartSizesPhase()
{
    NextMessagePartPhase();
    return true;
}

bool TPacketEncoder::EndMessagePartPhase()
{
    NextMessagePartPhase();
    return true;
}

void TPacketEncoder::NextMessagePartPhase()
{
    YASSERT(PartIndex < PartCount);

    const auto& parts = Message->GetParts();

    while (true) {
        ++PartIndex;
        if (PartIndex == PartCount) {
            break;
        }

        const auto& part = parts[PartIndex];
        if (part.Size() != 0) {
            BeginPhase(EPacketPhase::MessagePart, const_cast<char*>(part.Begin()), part.Size());
            return;
        }
    }

    SetFinished();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NBus
} // namespace NYT
