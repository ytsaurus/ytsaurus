#include "stdafx.h"
#include "protobuf_helpers.h"
#include "mpl.h"

#include <core/compression/codec.h>

#include <contrib/libs/protobuf/io/zero_copy_stream.h>
#include <contrib/libs/protobuf/io/zero_copy_stream_impl_lite.h>

namespace NYT {

using namespace google::protobuf::io;

////////////////////////////////////////////////////////////////////////////////

bool SerializeToProto(const google::protobuf::MessageLite& message, TSharedRef* data)
{
    size_t size = message.ByteSize();
    struct TSerializedMessageTag { };
    *data = TSharedRef::Allocate<TSerializedMessageTag>(size, false);
    return message.SerializePartialToArray(data->Begin(), size);
}

bool DeserializeFromProto(google::protobuf::MessageLite* message, const TRef& data)
{
    return message->ParsePartialFromArray(data.Begin(), data.Size());
}

////////////////////////////////////////////////////////////////////////////////

#pragma pack(push, 4)

struct TSerializedMessageFixedHeader
{
    i32 HeaderSize;
    i32 MessageSize;
};

#pragma pack(pop)

bool SerializeToProtoWithEnvelope(
    const google::protobuf::MessageLite& message,
    TSharedRef* data,
    NCompression::ECodec codecId)
{
    NProto::TSerializedMessageEnvelope envelope;
    if (codecId != NCompression::ECodec::None) {
        envelope.set_codec(static_cast<int>(codecId));
    }

    size_t messageSize = message.ByteSize();
    struct TSerializedMessageTag { };
    auto serializedMessage = TSharedRef::Allocate<TSerializedMessageTag>(messageSize, false);
    if (!message.SerializePartialToArray(serializedMessage.Begin(), messageSize)) {
        return false;
    }

    auto codec = NCompression::GetCodec(codecId);
    auto compressedMessage = codec->Compress(serializedMessage);

    TSerializedMessageFixedHeader fixedHeader;
    fixedHeader.HeaderSize = envelope.ByteSize();
    fixedHeader.MessageSize = compressedMessage.Size();

    size_t totalSize =
        sizeof (TSerializedMessageFixedHeader) +
        fixedHeader.HeaderSize +
        fixedHeader.MessageSize;

    *data = TSharedRef::Allocate<TSerializedMessageTag>(totalSize, false);

    char* targetFixedHeader = data->Begin();
    char* targetHeader = targetFixedHeader + sizeof (TSerializedMessageFixedHeader);
    char* targetMessage = targetHeader + fixedHeader.HeaderSize;

    memcpy(targetFixedHeader, &fixedHeader, sizeof (fixedHeader));
    YCHECK(envelope.SerializeToArray(targetHeader, fixedHeader.HeaderSize));
    memcpy(targetMessage, compressedMessage.Begin(), fixedHeader.MessageSize);

    return true;
}

bool DeserializeFromProtoWithEnvelope(
    google::protobuf::MessageLite* message,
    const TRef& data)
{
    if (data.Size() < sizeof (TSerializedMessageFixedHeader)) {
        return false;
    }

    const auto* fixedHeader = reinterpret_cast<const TSerializedMessageFixedHeader*>(data.Begin());
    if (fixedHeader->HeaderSize < 0 || fixedHeader->MessageSize < 0) {
        return false;
    }

    const char* sourceHeader = data.Begin() + sizeof (TSerializedMessageFixedHeader);
    const char* sourceMessage = sourceHeader + fixedHeader->HeaderSize;

    NProto::TSerializedMessageEnvelope envelope;
    if (!envelope.ParseFromArray(sourceHeader, fixedHeader->HeaderSize)) {
        return false;
    }

    // TODO(babenko): get rid of const_cast here
    auto compressedMessage = TSharedRef::FromRefNonOwning(TRef(
        const_cast<char*>(sourceMessage),
        fixedHeader->MessageSize));

    auto codecId = NCompression::ECodec(envelope.codec());
    auto codec = NCompression::GetCodec(codecId);
    auto serializedMessage = codec->Decompress(compressedMessage);

    // See comments to CodedInputStream::SetTotalBytesLimit (libs/protobuf/io/coded_stream.h)
    // to find out more about protobuf message size limits.
    CodedInputStream codedInputStream(
        reinterpret_cast<const ui8*>(serializedMessage.Begin()),
        static_cast<int>(serializedMessage.Size()));
    codedInputStream.SetTotalBytesLimit(
        serializedMessage.Size() + 1,
        serializedMessage.Size() + 1);

    // Raise recursion limit.
    codedInputStream.SetRecursionLimit(1024);

    if (!message->ParsePartialFromCodedStream(&codedInputStream)) {
        return false;
    }

    return true;
}

////////////////////////////////////////////////////////////////////////////////

void TBinaryProtoSerializer::Save(TStreamSaveContext& context, const ::google::protobuf::MessageLite& message)
{
    TSharedRef data;
    YCHECK(SerializeToProtoWithEnvelope(message, &data));
    TSizeSerializer::Save(context, data.Size());
    TRangeSerializer::Save(context, data);
}

void TBinaryProtoSerializer::Load(TStreamLoadContext& context, ::google::protobuf::MessageLite& message)
{
    size_t size = TSizeSerializer::Load(context);
    auto data = TSharedRef::Allocate(size, false);
    TRangeSerializer::Load(context, data);
    YCHECK(DeserializeFromProtoWithEnvelope(&message, data));
}

////////////////////////////////////////////////////////////////////////////////

void FilterProtoExtensions(
    NProto::TExtensionSet* target,
    const NProto::TExtensionSet& source,
    const yhash_set<int>& tags)
{
    target->Clear();
    for (const auto& extension : source.extensions()) {
        if (tags.find(extension.tag()) != tags.end()) {
            *target->add_extensions() = extension;
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
