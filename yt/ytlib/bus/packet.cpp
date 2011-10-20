#include "stdafx.h"
#include "packet.h"

namespace NYT {
namespace NBus {

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = BusLogger;

////////////////////////////////////////////////////////////////////////////////

const int THeaderTraits<TPacketHeader>::FixedSize =
    sizeof (TPacketHeader);
const int THeaderTraits<TMultipartPacketHeader>::FixedSize =
    (size_t) &(((TMultipartPacketHeader*) NULL)->PartSizes);

////////////////////////////////////////////////////////////////////////////////

bool DecodeMessagePacket(
    TBlob& data,
    IMessage::TPtr* message,
    TSequenceId* sequenceId)
{
    TMultipartPacketHeader* header = ParsePacketHeader<TMultipartPacketHeader>(data);
    if (header == NULL)
        return false;

    if (header->PartCount < 0 || header->PartCount > TMultipartPacketHeader::MaxParts) {
        LOG_ERROR("Invalid part count in a multipart packet (PartCount: %d)", header->PartCount);
        return false;
    }

    yvector<TRef> parts(header->PartCount);

    char* ptr = reinterpret_cast<char*>(&header->PartSizes[header->PartCount]);
    char* dataEnd = data.end();
    for (int i = 0; i < header->PartCount; ++i) {
        i32 partSize = header->PartSizes[i];
        if (partSize < 0 || partSize > TMultipartPacketHeader::MaxPartSize) {
            LOG_ERROR("Invalid part size in a multipart packet (PartIndex: %d, PartSize: %d)",
                i,
                partSize);
            return false;
        }
        if (ptr + partSize > dataEnd) {
            LOG_ERROR("Buffer overrun in a multipart packet (PartIndex: %d)",
                i);
            return false;
        }
        parts[i] = TRef(ptr, partSize);
        ptr += partSize;
    }

    *message = ~New<TBlobMessage>(&data, parts);
    *sequenceId = header->SequenceId;

    return true;
}

bool EncodeMessagePacket(
    IMessage::TPtr message,
    const TSessionId& sessionId,
    TSequenceId sequenceId,
    TBlob* data)
{
    const yvector<TSharedRef>& parts = message->GetParts();

    if (parts.ysize() > TMultipartPacketHeader::MaxParts) {
        LOG_ERROR("Multipart message contains too many parts (PartCount: %d)",
            parts.ysize());
        return false;
    }

    i64 dataSize = 0;
    dataSize += THeaderTraits<TMultipartPacketHeader>::FixedSize;
    dataSize += sizeof (i32) * parts.ysize();
    for (int index = 0; index < parts.ysize(); ++index)
    {
        const TSharedRef& part = parts[index];
        i32 partSize = static_cast<i32>(part.Size());
        if (partSize > TMultipartPacketHeader::MaxPartSize) {
            LOG_ERROR("Multipart message part is too large (PartIndex: %d, PartSize: %d)",
                index,
                partSize);
            return false;
        }
        dataSize += partSize;
    }

    data->resize(static_cast<size_t>(dataSize));

    TMultipartPacketHeader* header = reinterpret_cast<TMultipartPacketHeader*>(data->begin());
    header->Signature = TPacketHeader::ExpectedSignature;
    header->Type = TPacketHeader::EType::Message;
    header->SessionId = sessionId;
    header->PartCount = parts.ysize();
    header->SequenceId = sequenceId;
    for (int i = 0; i < header->PartCount; ++i) {
        header->PartSizes[i] = static_cast<i32>(parts[i].Size());
    }

    char* current = reinterpret_cast<char*>(&header->PartSizes[parts.ysize()]);
    for (int i = 0; i < header->PartCount; ++i) {
        const TRef& part = parts[i];
        NStl::copy(part.Begin(), part.End(), current);
        current += part.Size();
    }

    return true;
}

void CreatePacket(const TSessionId& sessionId, TPacketHeader::EType type, TBlob* data)
{
    data->resize(THeaderTraits<TPacketHeader>::FixedSize);
    TPacketHeader* header = reinterpret_cast<TPacketHeader*>(data->begin());
    header->Signature = TPacketHeader::ExpectedSignature;
    header->Type = type;
    header->SessionId = sessionId;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NBus
} // namespace NYT
