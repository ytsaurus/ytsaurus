#include "stdafx.h"
#include "packet.h"

namespace NYT {
namespace NBus {

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = BusLogger;

static const size_t SmallChunkSize = 16 * 1024;
static const size_t SmallPartSize  =  4 * 1024;

////////////////////////////////////////////////////////////////////////////////

TPacketDecoder::TPacketDecoder()
    : SmallChunkUsed(0)
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
    YASSERT(FragmentRemaining != 0);
    YASSERT(size <= FragmentRemaining);

    FragmentRemaining -= size;
    Fragment += size;
    if (FragmentRemaining == 0) {
        return EndPhase();
    } else {
        return true;
    }
}

EPacketType TPacketDecoder::GetPacketType() const
{
    return EPacketType(Header.Type);
}

EPacketFlags TPacketDecoder::GetPacketFlags() const
{
    return EPacketFlags(Header.Flags);
}

const TPacketId& TPacketDecoder::GetPacketId() const
{
    return Header.PacketId;
}

TSharedRefArray TPacketDecoder::GetMessage() const
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
            LOG_ERROR("Invalid packet type %v", EPacketType(Header.Type));
            return false;
    }
}

bool TPacketDecoder::EndPartCountPhase()
{
    if (PartCount < 0 || PartCount > MaxPacketPartCount) {
        LOG_ERROR("Invalid part count %v", PartCount);
        return false;
    }

    PartSizes.resize(PartCount);
    BeginPhase(EPacketPhase::PartSizes, PartSizes.data(), PartCount * sizeof (i32));
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
            LOG_ERROR("Invalid size %v of part %v",
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

        size_t partSize = PartSizes[PartIndex];
        if (partSize != 0) {
            auto part = AllocatePart(partSize);
            BeginPhase(EPacketPhase::MessagePart, part.Begin(), part.Size());            
            Parts.push_back(std::move(part));
            return;
        }

        Parts.push_back(TSharedRef());
    }

    Message = TSharedRefArray(std::move(Parts));
    SetFinished();
}

TSharedRef TPacketDecoder::AllocatePart(size_t partSize)
{
    if (partSize <= SmallPartSize) {
        if (SmallChunkUsed + partSize > SmallChunk.Size()) {
            struct TSmallReceivedMessagePartTag { };
            SmallChunk = TSharedRef::Allocate<TSmallReceivedMessagePartTag>(SmallChunkSize, false);
            SmallChunkUsed = 0;            
        }
        auto part = SmallChunk.Slice(TRef(SmallChunk.Begin() + SmallChunkUsed, partSize));
        SmallChunkUsed += partSize;
        return part;
    } else {
        struct TLargeReceivedMessagePartTag { };
        return TSharedRef::Allocate<TLargeReceivedMessagePartTag>(partSize, false);
    }
}

////////////////////////////////////////////////////////////////////////////////

TPacketEncoder::TPacketEncoder()
{
    Phase = EPacketPhase::Unstarted;
    Header.Signature = PacketSignature;
}

i64 TPacketEncoder::GetPacketSize(
    EPacketType type,
    const TSharedRefArray& message)
{
    i64 size = sizeof (TPacketHeader);
    switch (type) {
        case EPacketType::Ack:
            break;

        case EPacketType::Message: {
            size += sizeof (i32); // PartCount
            for (const auto& part : message) {
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
    EPacketFlags flags,
    const TPacketId& packetId,
    TSharedRefArray message)
{
    Header.Type = type;
    Header.Flags = flags;
    Header.PacketId = packetId;

    PartSizes.clear();
    PartCount = 0;
    PartIndex = -1;
    Message = message;

    if (type == EPacketType::Message) {
        PartCount = message.Size();

        if (PartCount > MaxPacketPartCount) {
            LOG_ERROR("Invalid part count %v", PartCount);
            return false;
        }

        for (int index = 0; index < PartCount; ++index) {
            const auto& part = message[index];
            int partSize = static_cast<int>(part.Size());
            if (partSize > MaxPacketPartSize) {
                LOG_ERROR("Invalid size %v of part %v",
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

bool TPacketEncoder::IsFragmentOwned() const
{
    return Phase == EPacketPhase::MessagePart;
}

void TPacketEncoder::NextFragment()
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
    BeginPhase(EPacketPhase::PartSizes, PartSizes.data(), PartCount * sizeof (i32));
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

    while (true) {
        ++PartIndex;
        if (PartIndex == PartCount) {
            break;
        }

        const auto& part = Message[PartIndex];
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
