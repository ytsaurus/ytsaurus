#include "helpers.h"

#include <yt/core/misc/protobuf_helpers.h>
#include <yt/core/misc/protobuf_helpers.pb.h>
#include <yt/core/misc/error.pb.h>

#include <yt/core/compression/codec.h>

#include <contrib/libs/grpc/include/grpc/grpc.h>
#include <contrib/libs/grpc/include/grpc/byte_buffer.h>
#include <contrib/libs/grpc/include/grpc/byte_buffer_reader.h>

#include <contrib/libs/protobuf/io/zero_copy_stream_impl_lite.h>

namespace NYT {
namespace NGrpc {

////////////////////////////////////////////////////////////////////////////////

TGrpcMetadataArray::TGrpcMetadataArray()
{
    grpc_metadata_array_init(&Native_);
}

TGrpcMetadataArray::~TGrpcMetadataArray()
{
    grpc_metadata_array_destroy(&Native_);
}

grpc_metadata_array* TGrpcMetadataArray::Unwrap()
{
    return &Native_;
}

TStringBuf TGrpcMetadataArray::Find(const char* key) const
{
    for (size_t index = 0; index < Native_.count; ++index) {
        const auto& metadata = Native_.metadata[index];
        if (strcmp(metadata.key, key) == 0) {
            return TStringBuf(metadata.value, metadata.value_length);
        }
    }

    return TStringBuf();
}

////////////////////////////////////////////////////////////////////////////////

void TGrpcMetadataArrayBuilder::Add(const char* key, TString value)
{
    grpc_metadata metadata;
    metadata.key = key;
    metadata.value = value.c_str();
    metadata.value_length = value.length();
    metadata.flags = 0;
    NativeMetadata_.push_back(metadata);
    Strings_.emplace_back(std::move(value));
}

size_t TGrpcMetadataArrayBuilder::GetSize() const
{
    return NativeMetadata_.size();
}

grpc_metadata* TGrpcMetadataArrayBuilder::Unwrap()
{
    return NativeMetadata_.data();
}

////////////////////////////////////////////////////////////////////////////////

TGrpcCallDetails::TGrpcCallDetails()
{
    grpc_call_details_init(&Native_);
}

TGrpcCallDetails::~TGrpcCallDetails()
{
    grpc_call_details_destroy(&Native_);
}

grpc_call_details* TGrpcCallDetails::Unwrap()
{
    return &Native_;
}

grpc_call_details* TGrpcCallDetails::operator->()
{
    return &Native_;
}

////////////////////////////////////////////////////////////////////////////////

struct TMessageTag
{ };

TSharedRef ByteBufferToEnvelopedMessage(grpc_byte_buffer* buffer)
{
    NProto::TSerializedMessageEnvelope envelope;
    // Codec remains "none".

    TEnvelopeFixedHeader fixedHeader;
    fixedHeader.HeaderSize = envelope.ByteSize();
    fixedHeader.MessageSize = static_cast<ui32>(grpc_byte_buffer_length(buffer));

    size_t totalSize =
        sizeof (TEnvelopeFixedHeader) +
        fixedHeader.HeaderSize +
        fixedHeader.MessageSize;

    auto data = TSharedMutableRef::Allocate<TMessageTag>(totalSize, false);

    char* targetFixedHeader = data.Begin();
    char* targetHeader = targetFixedHeader + sizeof (TEnvelopeFixedHeader);
    char* targetMessage = targetHeader + fixedHeader.HeaderSize;

    memcpy(targetFixedHeader, &fixedHeader, sizeof (fixedHeader));
    YCHECK(envelope.SerializeToArray(targetHeader, fixedHeader.HeaderSize));

    grpc_byte_buffer_reader reader;
    YCHECK(grpc_byte_buffer_reader_init(&reader, buffer) == 1);

    char* currentMessage = targetMessage;
    while (true) {
        grpc_slice slice;
        if (grpc_byte_buffer_reader_next(&reader, &slice) == 0) {
            break;
        }
        const auto* sliceData = GRPC_SLICE_START_PTR(slice);
        auto sliceSize = GRPC_SLICE_LENGTH(slice);
        ::memcpy(currentMessage, sliceData, sliceSize);
        currentMessage += sliceSize;
        grpc_slice_unref(slice);
    }
    grpc_byte_buffer_reader_destroy(&reader);

    return data;
}

TGrpcByteBufferPtr EnvelopedMessageToByteBuffer(const TSharedRef& data)
{
    YCHECK(data.Size() >= sizeof (TEnvelopeFixedHeader));
    const auto* fixedHeader = reinterpret_cast<const TEnvelopeFixedHeader*>(data.Begin());
    const char* sourceHeader = data.Begin() + sizeof (TEnvelopeFixedHeader);
    const char* sourceMessage = sourceHeader + fixedHeader->HeaderSize;

    NProto::TSerializedMessageEnvelope envelope;
    YCHECK(envelope.ParseFromArray(sourceHeader, fixedHeader->HeaderSize));

    auto compressedMessage = data.Slice(sourceMessage, sourceMessage + fixedHeader->MessageSize);

    auto codecId = NCompression::ECodec(envelope.codec());
    auto* codec = NCompression::GetCodec(codecId);
    auto uncompressedMessage = codec->Decompress(compressedMessage);

    struct THolder
    {
        TSharedRef Message;
    };

    auto* holder = new THolder();
    holder->Message = uncompressedMessage;

    auto slice = grpc_slice_new_with_user_data(
        const_cast<char*>(uncompressedMessage.Begin()),
        uncompressedMessage.Size(),
        [] (void* untypedHolder) {
            delete static_cast<THolder*>(untypedHolder);
        },
        holder);

    auto* buffer = grpc_raw_byte_buffer_create(&slice, 1);

    grpc_slice_unref(slice);

    return TGrpcByteBufferPtr(buffer);
}

TString SerializeError(const TError& error)
{
    TString serializedError;
    google::protobuf::io::StringOutputStream output(&serializedError);
    NProto::TError protoError;
    ToProto(&protoError, error);
    YCHECK(protoError.SerializeToZeroCopyStream(&output));
    return serializedError;
}

TError DeserializeError(const TStringBuf& serializedError)
{
    NProto::TError protoError;
    google::protobuf::io::ArrayInputStream input(serializedError.data(), serializedError.size());
    if (!protoError.ParseFromZeroCopyStream(&input)) {
        THROW_ERROR_EXCEPTION("Error deserializing error");
    }
    return FromProto<TError>(protoError);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NGrpc
} // namespace NYT
