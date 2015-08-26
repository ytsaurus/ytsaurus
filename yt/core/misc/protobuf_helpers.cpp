#include "stdafx.h"
#include "protobuf_helpers.h"
#include "mpl.h"

#include <core/compression/codec.h>

#include <contrib/libs/protobuf/text_format.h>
#include <contrib/libs/protobuf/io/coded_stream.h>
#include <contrib/libs/protobuf/io/zero_copy_stream.h>
#include <contrib/libs/protobuf/io/zero_copy_stream_impl_lite.h>

namespace NYT {

using namespace google::protobuf::io;

////////////////////////////////////////////////////////////////////////////////

namespace {

bool SerializeMessage(
    const google::protobuf::MessageLite& message,
    const TSharedMutableRef& mutableData,
    bool partial)
{
    return partial
        ? message.SerializePartialToArray(mutableData.Begin(), mutableData.Size())
        : message.SerializeToArray(mutableData.Begin(), mutableData.Size());
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

bool TrySerializeToProto(
    const google::protobuf::MessageLite& message,
    TSharedRef* data,
    bool partial)
{
    size_t size = message.ByteSize();
    struct TSerializedMessageTag { };
    auto mutableData = TSharedMutableRef::Allocate<TSerializedMessageTag>(size, false);
    if (!SerializeMessage(message, mutableData, partial)) {
        return false;
    }
    *data = mutableData;
    return true;
}

TSharedRef SerializeToProto(
    const google::protobuf::MessageLite& message,
    bool partial)
{
    TSharedRef data;
    YCHECK(TrySerializeToProto(message, &data, partial));
    return data;
}

bool TryDeserializeFromProto(google::protobuf::MessageLite* message, const TRef& data)
{
    // See comments to CodedInputStream::SetTotalBytesLimit (libs/protobuf/io/coded_stream.h)
    // to find out more about protobuf message size limits.
    CodedInputStream codedInputStream(
        reinterpret_cast<const ui8*>(data.Begin()),
        static_cast<int>(data.Size()));
    codedInputStream.SetTotalBytesLimit(
        data.Size() + 1,
        data.Size() + 1);

    // Raise recursion limit.
    codedInputStream.SetRecursionLimit(1024);

    return message->ParsePartialFromCodedStream(&codedInputStream);
}

void DeserializeFromProto(google::protobuf::MessageLite* message, const TRef& data)
{
    YCHECK(TryDeserializeFromProto(message, data));
}

////////////////////////////////////////////////////////////////////////////////

#pragma pack(push, 4)

struct TSerializedMessageFixedHeader
{
    i32 HeaderSize;
    i32 MessageSize;
};

#pragma pack(pop)

bool TrySerializeToProtoWithEnvelope(
    const google::protobuf::MessageLite& message,
    TSharedRef* data,
    NCompression::ECodec codecId,
    bool partial)
{
    NProto::TSerializedMessageEnvelope envelope;
    if (codecId != NCompression::ECodec::None) {
        envelope.set_codec(static_cast<int>(codecId));
    }

    size_t messageSize = message.ByteSize();
    struct TSerializedMessageTag { };
    auto serializedMessage = TSharedMutableRef::Allocate<TSerializedMessageTag>(messageSize, false);
    if (!SerializeMessage(message, serializedMessage, partial)) {
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

    auto mutableData = TSharedMutableRef::Allocate<TSerializedMessageTag>(totalSize, false);

    char* targetFixedHeader = mutableData.Begin();
    char* targetHeader = targetFixedHeader + sizeof (TSerializedMessageFixedHeader);
    char* targetMessage = targetHeader + fixedHeader.HeaderSize;

    memcpy(targetFixedHeader, &fixedHeader, sizeof (fixedHeader));
    YCHECK(envelope.SerializeToArray(targetHeader, fixedHeader.HeaderSize));
    memcpy(targetMessage, compressedMessage.Begin(), fixedHeader.MessageSize);

    *data = mutableData;
    return true;
}

TSharedRef SerializeToProtoWithEnvelope(
    const google::protobuf::MessageLite& message,
    NCompression::ECodec codecId,
    bool partial)
{
    TSharedRef data;
    YCHECK(TrySerializeToProtoWithEnvelope(message, &data, codecId, partial));
    return data;
}

bool TryDeserializeFromProtoWithEnvelope(
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

    auto compressedMessage = TSharedRef(sourceMessage, fixedHeader->MessageSize, nullptr);

    auto codecId = NCompression::ECodec(envelope.codec());
    auto codec = NCompression::GetCodec(codecId);
    auto serializedMessage = codec->Decompress(compressedMessage);

    return TryDeserializeFromProto(message, serializedMessage);
}

void DeserializeFromProtoWithEnvelope(
    google::protobuf::MessageLite* message,
    const TRef& data)
{
    YCHECK(TryDeserializeFromProtoWithEnvelope(message, data));
}

////////////////////////////////////////////////////////////////////////////////

void TBinaryProtoSerializer::Save(TStreamSaveContext& context, const ::google::protobuf::Message& message)
{
    auto data = SerializeToProtoWithEnvelope(message);
    TSizeSerializer::Save(context, data.Size());
    TRangeSerializer::Save(context, data);
}

namespace {

Stroka DumpProto(::google::protobuf::Message& message)
{
    ::google::protobuf::TextFormat::Printer printer;
    printer.SetSingleLineMode(true);
    Stroka result;
    YCHECK(printer.PrintToString(message, &result));
    return result;
}

} // namespace

void TBinaryProtoSerializer::Load(TStreamLoadContext& context, ::google::protobuf::Message& message)
{
    size_t size = TSizeSerializer::LoadSuspended(context);
    auto data = TSharedMutableRef::Allocate(size, false);

    SERIALIZATION_DUMP_SUSPEND(context) {
        TRangeSerializer::Load(context, data);
    }

    DeserializeFromProtoWithEnvelope(&message, data);

    SERIALIZATION_DUMP_WRITE(context, "proto[%v] %v", size, DumpProto(message));
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
