#include "stdafx.h"
#include "protobuf_helpers.h"

#include <contrib/libs/protobuf/io/zero_copy_stream.h>
#include <contrib/libs/protobuf/io/zero_copy_stream_impl_lite.h>

namespace NYT {

using namespace google::protobuf::io;

////////////////////////////////////////////////////////////////////////////////

bool SerializeToProto(const google::protobuf::Message& message, TSharedRef* data)
{
    size_t size = message.ByteSize();
    *data = TSharedRef(size);
    return message.SerializeToArray(data->Begin(), size);
}

bool DeserializeFromProto(google::protobuf::Message* message, const TRef& data)
{
    return message->ParseFromArray(data.Begin(), data.Size());
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
    const google::protobuf::Message& message,
    TSharedRef* data,
    ECodecId codecId)
{
    NProto::TSerializedMessageEnvelope envelope;
    if (codecId != ECodecId::None) {
        envelope.set_codec_id(codecId);   
    }

    size_t messageSize = message.ByteSize();
    TSharedRef serializedMessage(messageSize);
    if (!message.SerializeToArray(serializedMessage.Begin(), messageSize)) {
        return false;
    }

    auto codec = GetCodec(codecId);
    auto compressedMessage = codec->Compress(serializedMessage);

    TSerializedMessageFixedHeader fixedHeader;
    fixedHeader.HeaderSize = envelope.ByteSize();
    fixedHeader.MessageSize = compressedMessage.Size();

    size_t totalSize =
        sizeof (TSerializedMessageFixedHeader) +
        fixedHeader.HeaderSize +
        fixedHeader.MessageSize;
        
    *data = TSharedRef(totalSize);

    char* targetFixedHeader = data->GetRef().Begin();
    char* targetHeader = targetFixedHeader + sizeof (TSerializedMessageFixedHeader);
    char* targetMessage = targetHeader + fixedHeader.HeaderSize;

    memcpy(targetFixedHeader, &fixedHeader, sizeof (fixedHeader));
    YCHECK(envelope.SerializeToArray(targetHeader, fixedHeader.HeaderSize));
    memcpy(targetMessage, compressedMessage.Begin(), fixedHeader.MessageSize);

    return true;
}

bool DeserializeFromProtoWithEnvelope(
    google::protobuf::Message* message,
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

    auto codecId = ECodecId(envelope.codec_id());
    auto codec = GetCodec(codecId);
    auto serializedMessage = codec->Decompress(compressedMessage);

    // Read comments to CodedInputStream::SetTotalBytesLimit (libs/protobuf/io/coded_stream.h)
    // to find out more about protobuf message size limits.
    ArrayInputStream arrayInputStream(serializedMessage.Begin(), serializedMessage.Size());
    CodedInputStream codedInputStream(arrayInputStream);
    codedInputStream.SetTotalBytesLimit(
        serializedMessage.Size() + 1, 
        serializedMessage.Size() + 1);

    if (!message->ParseFromCodedStream(&codedInputStream) {
        return false;
    }

    return true;
}

////////////////////////////////////////////////////////////////////////////////

void SaveProto(TOutputStream* output, const ::google::protobuf::Message& message)
{
    TSharedRef data;
    YCHECK(SerializeToProtoWithEnvelope(message, &data));
    ::SaveSize(output, data.Size());
    output->Write(data.Begin(), data.Size());
}

void LoadProto(TInputStream* input, ::google::protobuf::Message& message)
{
    size_t size = ::LoadSize(input);
    TSharedRef data(size);
    YCHECK(input->Load(data.Begin(), size) == size);
    YCHECK(DeserializeFromProtoWithEnvelope(&message, data));
}

void FilterProtoExtensions(
    NProto::TExtensionSet* target,
    const NProto::TExtensionSet& source,
    const yhash_set<int>& tags)
{
    target->Clear();
    FOREACH (const auto& extension, source.extensions()) {
        if (tags.find(extension.tag()) != tags.end()) {
            *target->add_extensions() = extension;
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

////////////////////////////////////////////////////////////////////////////////

namespace google {
namespace protobuf {

void CleanPooledObject(google::protobuf::MessageLite* obj)
{
    obj->Clear();
}

} // namespace protobuf
} // namespace google

////////////////////////////////////////////////////////////////////////////////


