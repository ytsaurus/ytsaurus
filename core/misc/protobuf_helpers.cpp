#include "protobuf_helpers.h"
#include "mpl.h"

#include <yt/core/compression/codec.h>

#include <yt/core/logging/log.h>

#include <contrib/libs/protobuf/io/coded_stream.h>
#include <contrib/libs/protobuf/io/zero_copy_stream.h>
#include <contrib/libs/protobuf/io/zero_copy_stream_impl_lite.h>
#include <contrib/libs/protobuf/text_format.h>

namespace NYT {

using namespace google::protobuf::io;

////////////////////////////////////////////////////////////////////////////////

static const NLogging::TLogger Logger("Serialize");
struct TSerializedMessageTag { };

////////////////////////////////////////////////////////////////////////////////

TSharedRef SerializeToProto(
    const google::protobuf::MessageLite& message,
    bool partial)
{
#ifdef YT_VALIDATE_REQUIRED_PROTO_FIELDS
    if (!partial && !message.IsInitialized()) {
        LOG_FATAL("Missing required fields: %v", message.InitializationErrorString());
    }
#endif
    auto size = message.ByteSize();
    auto data = TSharedMutableRef::Allocate<TSerializedMessageTag>(size, false);
    auto* begin = reinterpret_cast<google::protobuf::uint8*>(data.Begin());
    auto* end = message.SerializeWithCachedSizesToArray(begin);
    YCHECK(end - begin == data.Size());
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

TSharedRef SerializeToProtoWithEnvelope(
    const google::protobuf::MessageLite& message,
    NCompression::ECodec codecId,
    bool partial)
{
    NProto::TSerializedMessageEnvelope envelope;
    if (codecId != NCompression::ECodec::None) {
        envelope.set_codec(static_cast<int>(codecId));
    }

    auto serializedMessage = SerializeToProto(message, partial);

    auto codec = NCompression::GetCodec(codecId);
    auto compressedMessage = codec->Compress(serializedMessage);

    TEnvelopeFixedHeader fixedHeader;
    fixedHeader.HeaderSize = envelope.ByteSize();
    fixedHeader.MessageSize = compressedMessage.Size();

    size_t totalSize =
        sizeof (TEnvelopeFixedHeader) +
        fixedHeader.HeaderSize +
        fixedHeader.MessageSize;

    auto data = TSharedMutableRef::Allocate<TSerializedMessageTag>(totalSize, false);

    char* targetFixedHeader = data.Begin();
    char* targetHeader = targetFixedHeader + sizeof (TEnvelopeFixedHeader);
    char* targetMessage = targetHeader + fixedHeader.HeaderSize;

    memcpy(targetFixedHeader, &fixedHeader, sizeof (fixedHeader));
    YCHECK(envelope.SerializeToArray(targetHeader, fixedHeader.HeaderSize));
    memcpy(targetMessage, compressedMessage.Begin(), fixedHeader.MessageSize);

    return data;
}

bool TryDeserializeFromProtoWithEnvelope(
    google::protobuf::MessageLite* message,
    const TRef& data)
{
    if (data.Size() < sizeof (TEnvelopeFixedHeader)) {
        return false;
    }

    const auto* fixedHeader = reinterpret_cast<const TEnvelopeFixedHeader*>(data.Begin());
    const char* sourceHeader = data.Begin() + sizeof (TEnvelopeFixedHeader);
    const char* sourceMessage = sourceHeader + fixedHeader->HeaderSize;

    NProto::TSerializedMessageEnvelope envelope;
    if (!envelope.ParseFromArray(sourceHeader, fixedHeader->HeaderSize)) {
        return false;
    }

    auto compressedMessage = TSharedRef(sourceMessage, fixedHeader->MessageSize, nullptr);

    auto codecId = NCompression::ECodec(envelope.codec());
    auto* codec = NCompression::GetCodec(codecId);
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

TString DumpProto(::google::protobuf::Message& message)
{
    ::google::protobuf::TextFormat::Printer printer;
    printer.SetSingleLineMode(true);
    TString result;
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
    const THashSet<int>& tags)
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
