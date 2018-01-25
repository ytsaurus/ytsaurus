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

namespace {

void SerializeProtoToRefImpl(
    const google::protobuf::MessageLite& message,
    bool partial,
    const TMutableRef& ref)
{
#ifdef YT_VALIDATE_REQUIRED_PROTO_FIELDS
    if (!partial && !message.IsInitialized()) {
        LOG_FATAL("Missing required fields: %v", message.InitializationErrorString());
    }
#endif
    auto* begin = reinterpret_cast<google::protobuf::uint8*>(ref.begin());
    auto* end = reinterpret_cast<google::protobuf::uint8*>(ref.end());
    YCHECK(message.SerializeWithCachedSizesToArray(begin) == end);
}

} // namespace

TSharedRef SerializeProtoToRef(
    const google::protobuf::MessageLite& message,
    bool partial)
{
    auto size = message.ByteSize();
    auto data = TSharedMutableRef::Allocate<TSerializedMessageTag>(size, false);
    SerializeProtoToRefImpl(message, partial, data);
    return data;
}

TString SerializeProtoToString(
    const google::protobuf::MessageLite& message,
    bool partial)
{
    auto size = message.ByteSize();
    auto data = TString::Uninitialized(size);
    SerializeProtoToRefImpl(message, partial, TMutableRef(data.begin(), size));
    return data;
}

bool TryDeserializeProto(google::protobuf::MessageLite* message, const TRef& data)
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

void DeserializeProto(google::protobuf::MessageLite* message, const TRef& data)
{
    YCHECK(TryDeserializeProto(message, data));
}

////////////////////////////////////////////////////////////////////////////////

TSharedRef SerializeProtoToRefWithEnvelope(
    const google::protobuf::MessageLite& message,
    NCompression::ECodec codecId,
    bool partial)
{
    NProto::TSerializedMessageEnvelope envelope;
    if (codecId != NCompression::ECodec::None) {
        envelope.set_codec(static_cast<int>(codecId));
    }

    auto serializedMessage = SerializeProtoToRef(message, partial);

    auto codec = NCompression::GetCodec(codecId);
    auto compressedMessage = codec->Compress(serializedMessage);

    TEnvelopeFixedHeader fixedHeader;
    fixedHeader.EnvelopeSize = static_cast<ui32>(envelope.ByteSize());
    fixedHeader.MessageSize = static_cast<ui32>(compressedMessage.Size());

    size_t totalSize =
        sizeof (TEnvelopeFixedHeader) +
        fixedHeader.EnvelopeSize +
        fixedHeader.MessageSize;

    auto data = TSharedMutableRef::Allocate<TSerializedMessageTag>(totalSize, false);

    char* targetFixedHeader = data.Begin();
    char* targetHeader = targetFixedHeader + sizeof (TEnvelopeFixedHeader);
    char* targetMessage = targetHeader + fixedHeader.EnvelopeSize;

    memcpy(targetFixedHeader, &fixedHeader, sizeof (fixedHeader));
    YCHECK(envelope.SerializeToArray(targetHeader, fixedHeader.EnvelopeSize));
    memcpy(targetMessage, compressedMessage.Begin(), fixedHeader.MessageSize);

    return data;
}

TString SerializeProtoToStringWithEnvelope(
    const google::protobuf::MessageLite& message,
    NCompression::ECodec codecId,
    bool partial)
{
    if (codecId != NCompression::ECodec::None) {
        // TODO(babenko): see YT-7865 for a related issue
        return ToString(SerializeProtoToRefWithEnvelope(message, codecId, partial));
    }

    NProto::TSerializedMessageEnvelope envelope;

    TEnvelopeFixedHeader fixedHeader;
    fixedHeader.EnvelopeSize = static_cast<ui32>(envelope.ByteSize());
    fixedHeader.MessageSize = static_cast<ui32>(message.ByteSize());

    auto totalSize =
        sizeof (fixedHeader) +
        fixedHeader.EnvelopeSize +
        fixedHeader.MessageSize;

    auto data = TString::Uninitialized(totalSize);
    char* ptr = data.begin();
    ::memcpy(ptr, &fixedHeader, sizeof (fixedHeader));
    ptr += sizeof (fixedHeader);
    ptr = reinterpret_cast<char*>(envelope.SerializeWithCachedSizesToArray(reinterpret_cast<ui8*>(ptr)));
    ptr = reinterpret_cast<char*>(message.SerializeWithCachedSizesToArray(reinterpret_cast<ui8*>(ptr)));
    Y_ASSERT(ptr == data.end());

    return data;
}

bool TryDeserializeProtoWithEnvelope(
    google::protobuf::MessageLite* message,
    const TRef& data)
{
    if (data.Size() < sizeof (TEnvelopeFixedHeader)) {
        return false;
    }

    const auto* fixedHeader = reinterpret_cast<const TEnvelopeFixedHeader*>(data.Begin());
    const char* sourceHeader = data.Begin() + sizeof (TEnvelopeFixedHeader);
    const char* sourceMessage = sourceHeader + fixedHeader->EnvelopeSize;

    NProto::TSerializedMessageEnvelope envelope;
    if (!envelope.ParseFromArray(sourceHeader, fixedHeader->EnvelopeSize)) {
        return false;
    }

    auto codecId = NCompression::ECodec(envelope.codec());
    auto compressedMessage = TSharedRef(sourceMessage, fixedHeader->MessageSize, nullptr);

    auto* codec = NCompression::GetCodec(codecId);
    auto serializedMessage = codec->Decompress(compressedMessage);

    return TryDeserializeProto(message, serializedMessage);
}

void DeserializeProtoWithEnvelope(
    google::protobuf::MessageLite* message,
    const TRef& data)
{
    YCHECK(TryDeserializeProtoWithEnvelope(message, data));
}

////////////////////////////////////////////////////////////////////////////////

void TBinaryProtoSerializer::Save(TStreamSaveContext& context, const ::google::protobuf::Message& message)
{
    auto data = SerializeProtoToRefWithEnvelope(message);
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

    DeserializeProtoWithEnvelope(&message, data);

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
