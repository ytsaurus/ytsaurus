#include "stdafx.h"
#include "packet.h"

namespace NYT {
namespace NBus {

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = BusLogger;

////////////////////////////////////////////////////////////////////////////////

#define yt_offset_of(type, member) \
    ( (size_t) ( (char*) &( ((type*)8) -> member ) - 8) )
#define yt_container_of(ptr, type, member) \
    ( (type*) ( (char*)(ptr) - yt_offset_of(type, member) ) )

const int THeaderTraits<TPacketHeader>::FixedSize =
    sizeof (TPacketHeader);
const int THeaderTraits<TMultipartPacketHeader>::FixedSize =
    yt_offset_of(TMultipartPacketHeader, PartSizes);

////////////////////////////////////////////////////////////////////////////////

bool DecodeMessagePacket(
    TBlob&& data,
    IMessage::TPtr* message,
    TSequenceId* sequenceId)
{
    auto* header = ParsePacketHeader<TMultipartPacketHeader>(data);
    if (!header)
        return false;

    if (header->PartCount < 0 || header->PartCount > TMultipartPacketHeader::MaxParts) {
        LOG_ERROR("Invalid part count in a multipart packet (PartCount: %d)", header->PartCount);
        return false;
    }

    yvector<TRef> parts(header->PartCount);

    char* ptr = reinterpret_cast<char*>(&header->PartSizes[header->PartCount]);
    char* dataEnd = data.end();
    for (int partIndex = 0; partIndex < header->PartCount; ++partIndex) {
        i32 partSize = header->PartSizes[partIndex];
        if (partSize < 0 || partSize > TMultipartPacketHeader::MaxPartSize) {
            LOG_ERROR("Invalid part size in a multipart packet (PartIndex: %d, PartSize: %d)",
                partIndex,
                partSize);
            return false;
        }
        if (ptr + partSize > dataEnd) {
            LOG_ERROR("Buffer overrun in a multipart packet (PartIndex: %d)",
                partIndex);
            return false;
        }
        parts[partIndex] = TRef(ptr, partSize);
        ptr += partSize;
    }

    *message = CreateMessageFromParts(MoveRV(data), parts);
    *sequenceId = header->SequenceId;

    return true;
}

bool EncodeMessagePacket(
    IMessage::TPtr message,
    const TSessionId& sessionId,
    TSequenceId sequenceId,
    TBlob* data)
{
    YASSERT(message);

    const auto& parts = message->GetParts();

    if (parts.ysize() > TMultipartPacketHeader::MaxParts) {
        LOG_ERROR("Multipart message contains too many parts (PartCount: %d)",
            parts.ysize());
        return false;
    }

    i64 dataSize = 0;
    dataSize += THeaderTraits<TMultipartPacketHeader>::FixedSize;
    dataSize += sizeof (i32) * parts.ysize();
    for (int partIndex = 0; partIndex < parts.ysize(); ++partIndex)
    {
        const TSharedRef& part = parts[partIndex];
        i32 partSize = static_cast<i32>(part.Size());
        if (partSize > TMultipartPacketHeader::MaxPartSize) {
            LOG_ERROR("Multipart message part is too large (PartIndex: %d, PartSize: %d)",
                partIndex,
                partSize);
            return false;
        }
        dataSize += partSize;
    }

    data->resize(static_cast<size_t>(dataSize));

    auto* header = reinterpret_cast<TMultipartPacketHeader*>(data->begin());
    header->Signature = TPacketHeader::ExpectedSignature;
    header->Type = TPacketHeader::EType::Message;
    header->SessionId = sessionId;
    header->PartCount = parts.ysize();
    header->SequenceId = sequenceId;
    for (int partIndex = 0; partIndex < header->PartCount; ++partIndex) {
        header->PartSizes[partIndex] = static_cast<i32>(parts[partIndex].Size());
    }

    char* current = reinterpret_cast<char*>(&header->PartSizes[parts.ysize()]);
    for (int partIndex = 0; partIndex < header->PartCount; ++partIndex) {
        const TRef& part = parts[partIndex];
        std::copy(part.Begin(), part.End(), current);
        current += part.Size();
    }

    return true;
}

void CreatePacket(const TSessionId& sessionId, TPacketHeader::EType type, TBlob* data)
{
    data->resize(THeaderTraits<TPacketHeader>::FixedSize);
    auto* header = reinterpret_cast<TPacketHeader*>(data->begin());
    header->Signature = TPacketHeader::ExpectedSignature;
    header->Type = type;
    header->SessionId = sessionId;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NBus
} // namespace NYT
