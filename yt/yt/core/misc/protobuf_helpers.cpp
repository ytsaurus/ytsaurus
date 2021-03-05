#include "protobuf_helpers.h"
#include "mpl.h"

#include <yt/yt/core/compression/codec.h>

#include <yt/yt/core/logging/log.h>

#include <yt/yt/core/misc/cast.h>

#include <yt/yt/core/yson/protobuf_interop.h>

#include <yt/yt/core/ytree/fluent.h>

#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/io/zero_copy_stream.h>
#include <google/protobuf/io/zero_copy_stream_impl_lite.h>
#include <google/protobuf/text_format.h>
#include <google/protobuf/util/time_util.h>

namespace NYT {

using namespace google::protobuf::io;
using namespace NYTree;
using namespace NYson;

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
        YT_LOG_FATAL("Missing required fields: %v", message.InitializationErrorString());
    }
#endif
    auto* begin = reinterpret_cast<google::protobuf::uint8*>(ref.begin());
    auto* end = reinterpret_cast<google::protobuf::uint8*>(ref.end());
    YT_VERIFY(message.SerializeWithCachedSizesToArray(begin) == end);
}

i32 CheckedCastToI32(ui64 length)
{
    if (length >= std::numeric_limits<i32>::max()) {
        THROW_ERROR_EXCEPTION("Protobuf message size exceeds 2GB")
            << TErrorAttribute("length", length);
    }
    return static_cast<i32>(length);
}

} // namespace

TSharedRef SerializeProtoToRef(
    const google::protobuf::MessageLite& message,
    bool partial)
{
    auto size = CheckedCastToI32(message.ByteSizeLong());
    auto data = TSharedMutableRef::Allocate<TSerializedMessageTag>(size, false);
    SerializeProtoToRefImpl(message, partial, data);
    return data;
}

TString SerializeProtoToString(
    const google::protobuf::MessageLite& message,
    bool partial)
{
    auto size = CheckedCastToI32(message.ByteSizeLong());
    auto data = TString::Uninitialized(size);
    SerializeProtoToRefImpl(message, partial, TMutableRef(data.begin(), size));
    return data;
}

bool TryDeserializeProto(google::protobuf::MessageLite* message, TRef data)
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

void DeserializeProto(google::protobuf::MessageLite* message, TRef data)
{
    YT_VERIFY(TryDeserializeProto(message, data));
}

////////////////////////////////////////////////////////////////////////////////

TSharedRef SerializeProtoToRefWithEnvelope(
    const google::protobuf::MessageLite& message,
    NCompression::ECodec codecId,
    bool partial)
{
    NYT::NProto::TSerializedMessageEnvelope envelope;
    if (codecId != NCompression::ECodec::None) {
        envelope.set_codec(static_cast<int>(codecId));
    }

    auto serializedMessage = SerializeProtoToRef(message, partial);

    auto codec = NCompression::GetCodec(codecId);
    auto compressedMessage = codec->Compress(serializedMessage);

    TEnvelopeFixedHeader fixedHeader;
    fixedHeader.EnvelopeSize = CheckedCastToI32(envelope.ByteSizeLong());
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
    YT_VERIFY(envelope.SerializeToArray(targetHeader, fixedHeader.EnvelopeSize));
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

    NYT::NProto::TSerializedMessageEnvelope envelope;

    TEnvelopeFixedHeader fixedHeader;
    fixedHeader.EnvelopeSize = CheckedCastToI32(envelope.ByteSizeLong());
    fixedHeader.MessageSize = CheckedCastToI32(message.ByteSizeLong());

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
    YT_ASSERT(ptr == data.end());

    return data;
}

bool TryDeserializeProtoWithEnvelope(
    google::protobuf::MessageLite* message,
    TRef data)
{
    if (data.Size() < sizeof (TEnvelopeFixedHeader)) {
        return false;
    }

    const auto* fixedHeader = reinterpret_cast<const TEnvelopeFixedHeader*>(data.Begin());
    const char* sourceHeader = data.Begin() + sizeof (TEnvelopeFixedHeader);
    if (fixedHeader->EnvelopeSize + sizeof (*fixedHeader) > data.Size()) {
        return false;
    }

    const char* sourceMessage = sourceHeader + fixedHeader->EnvelopeSize;

    NYT::NProto::TSerializedMessageEnvelope envelope;
    if (!envelope.ParseFromArray(sourceHeader, fixedHeader->EnvelopeSize)) {
        return false;
    }

    NCompression::ECodec codecId;
    if (!TryEnumCast(envelope.codec(), &codecId)) {
        return false;
    }

    if (fixedHeader->MessageSize + fixedHeader->EnvelopeSize + sizeof (*fixedHeader) > data.Size()) {
        return false;
    }

    auto compressedMessage = TSharedRef(sourceMessage, fixedHeader->MessageSize, nullptr);

    auto* codec = NCompression::GetCodec(codecId);
    try {
        auto serializedMessage = codec->Decompress(compressedMessage);

        return TryDeserializeProto(message, serializedMessage);
    } catch (const std::exception& ex) {
        return false;
    }
}

void DeserializeProtoWithEnvelope(
    google::protobuf::MessageLite* message,
    TRef data)
{
    YT_VERIFY(TryDeserializeProtoWithEnvelope(message, data));
}

TSharedRef SerializeProtoToRefWithCompression(
    const google::protobuf::MessageLite& message,
    NCompression::ECodec codecId,
    bool partial)
{
    auto serializedMessage = SerializeProtoToRef(message, partial);
    auto codec = NCompression::GetCodec(codecId);
    return codec->Compress(serializedMessage);
}

bool TryDeserializeProtoWithCompression(
    google::protobuf::MessageLite* message,
    TRef data,
    NCompression::ECodec codecId)
{
    auto compressedMessage = TSharedRef(data.Begin(), data.Size(), nullptr);
    auto* codec = NCompression::GetCodec(codecId);
    try {
        auto serializedMessage = codec->Decompress(compressedMessage);
        return TryDeserializeProto(message, serializedMessage);
    } catch (const std::exception& ex) {
        return false;
    }
}

void DeserializeProtoWithCompression(
    google::protobuf::MessageLite* message,
    TRef data,
    NCompression::ECodec codecId)
{
    YT_VERIFY(TryDeserializeProtoWithCompression(message, data, codecId));
}

TSharedRef PopEnvelope(const TSharedRef& data)
{
    TEnvelopeFixedHeader header;
    if (data.Size() < sizeof(header)) {
        THROW_ERROR_EXCEPTION("Fixed header is missing");
    }

    memcpy(&header, data.Begin(), sizeof(header));
    if (header.EnvelopeSize != 0) {
        THROW_ERROR_EXCEPTION("Envelope is not empty");
    }

    return data.Slice(sizeof(TEnvelopeFixedHeader), data.Size());
}

TSharedRef PushEnvelope(const TSharedRef& data)
{
    TEnvelopeFixedHeader header;
    header.EnvelopeSize = 0;
    header.MessageSize = data.Size();

    auto headerRef = TSharedMutableRef::Allocate(sizeof(header));
    memcpy(headerRef.Begin(), &header, sizeof(header));

    return MergeRefsToRef<TDefaultSharedBlobTag>(std::vector<TSharedRef>{headerRef, data});
}

////////////////////////////////////////////////////////////////////////////////

struct TProtobufExtensionDescriptor
{
    const google::protobuf::Descriptor* MessageDescriptor;
    const int Tag;
    const TString Name;
};

class TProtobufExtensionRegistry
{
public:
    void RegisterProtobufExtension(
        const google::protobuf::Descriptor* descriptor,
        int tag,
        const TString& name)
    {
        TProtobufExtensionDescriptor extensionDescriptor{
            .MessageDescriptor = descriptor,
            .Tag = tag,
            .Name = name
        };

        YT_VERIFY(ExtensionTagToExtensionDescriptor_.emplace(tag, extensionDescriptor).second);
        YT_VERIFY(ExtensionNameToExtensionDescriptor_.emplace(name, extensionDescriptor).second);
    }

    std::optional<TProtobufExtensionDescriptor> FindProtobufExtension(int tag) const
    {
        auto it = ExtensionTagToExtensionDescriptor_.find(tag);
        if (it == ExtensionTagToExtensionDescriptor_.end()) {
            return std::nullopt;
        } else {
            return it->second;
        }
    }

    std::optional<TProtobufExtensionDescriptor> FindProtobufExtension(const TString& name) const
    {
        auto it = ExtensionNameToExtensionDescriptor_.find(name);
        if (it == ExtensionNameToExtensionDescriptor_.end()) {
            return std::nullopt;
        } else {
            return it->second;
        }
    }

    static TProtobufExtensionRegistry* Get()
    {
        return Singleton<TProtobufExtensionRegistry>();
    }

private:
    Y_DECLARE_SINGLETON_FRIEND();
    TProtobufExtensionRegistry() = default;

    THashMap<int, TProtobufExtensionDescriptor> ExtensionTagToExtensionDescriptor_;
    THashMap<TString, TProtobufExtensionDescriptor> ExtensionNameToExtensionDescriptor_;
};

void RegisterProtobufExtension(
    const google::protobuf::Descriptor* descriptor,
    int tag,
    const TString& name)
{
    TProtobufExtensionRegistry::Get()->RegisterProtobufExtension(descriptor, tag, name);
}

////////////////////////////////////////////////////////////////////////////////

//! Intermediate extension representation for proto<->yson converter.
struct TExtension
{
    //! Extension tag.
    int Tag;

    //! Serialized extension message.
    TString Data;
};

//! Intermediate extension set representation for proto<->yson converter.
struct TExtensionSet
{
    std::vector<TExtension> Extensions;
};

void FromProto(TExtensionSet* extensionSet, const NYT::NProto::TExtensionSet& protoExtensionSet)
{
    const auto* extensionRegistry = TProtobufExtensionRegistry::Get();

    for (const auto& protoExtension : protoExtensionSet.extensions()) {
        // Do not parse unknown extensions.
        if (extensionRegistry->FindProtobufExtension(protoExtension.tag())) {
            TExtension extension{
                .Tag = protoExtension.tag(),
                .Data = protoExtension.data()
            };
            extensionSet->Extensions.push_back(std::move(extension));
        }
    }
}

void ToProto(NYT::NProto::TExtensionSet* protoExtensionSet, const TExtensionSet& extensionSet)
{
    for (const auto& extension : extensionSet.Extensions) {
        auto* protoExtension = protoExtensionSet->add_extensions();
        protoExtension->set_tag(extension.Tag);
        protoExtension->set_data(extension.Data);
    }
}

void Serialize(const TExtensionSet& extensionSet, NYson::IYsonConsumer* consumer)
{
    const auto* extensionRegistry = TProtobufExtensionRegistry::Get();

    BuildYsonFluently(consumer)
        .DoMapFor(extensionSet.Extensions, [&] (TFluentMap fluent, const TExtension& extension) {
            auto extensionDescriptor = extensionRegistry->FindProtobufExtension(extension.Tag);
            YT_VERIFY(extensionDescriptor);

            fluent
                .Item(extensionDescriptor->Name)
                .Do([&] (TFluentAny fluent) {
                    const auto& data = extension.Data;
                    ArrayInputStream inputStream(data.data(), data.size());
                    ParseProtobuf(
                        fluent.GetConsumer(),
                        &inputStream,
                        ReflectProtobufMessageType(extensionDescriptor->MessageDescriptor));
                });
        });
}

void Deserialize(TExtensionSet& extensionSet, NYTree::INodePtr node)
{
    const auto* extensionRegistry = TProtobufExtensionRegistry::Get();

    auto mapNode = node->AsMap();
    for (const auto& [name, value] : mapNode->GetChildren()) {
        auto extensionDescriptor = extensionRegistry->FindProtobufExtension(name);
        // Do not parse unknown extensions.
        if (!extensionDescriptor) {
            continue;
        }
        auto& extension = extensionSet.Extensions.emplace_back();
        extension.Tag = extensionDescriptor->Tag;

        StringOutputStream stream(&extension.Data);
        auto writer = CreateProtobufWriter(
            &stream,
            ReflectProtobufMessageType(extensionDescriptor->MessageDescriptor));
        VisitTree(value, writer.get(), /*stable=*/false);
    }
}

REGISTER_INTERMEDIATE_PROTO_INTEROP_REPRESENTATION(NYT::NProto::TExtensionSet, TExtensionSet)

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
    YT_VERIFY(printer.PrintToString(message, &result));
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
    NYT::NProto::TExtensionSet* target,
    const NYT::NProto::TExtensionSet& source,
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

google::protobuf::Timestamp GetProtoNow()
{
    // Unfortunately TimeUtil::GetCurrentTime provides only one second accuracy, so we use TInstant::Now.
    return google::protobuf::util::TimeUtil::MicrosecondsToTimestamp(TInstant::Now().MicroSeconds());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
