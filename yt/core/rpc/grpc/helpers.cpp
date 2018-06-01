#include "helpers.h"
#include "config.h"

#include <yt/core/misc/protobuf_helpers.h>
#include <yt/core/misc/proto/protobuf_helpers.pb.h>
#include <yt/core/misc/proto/error.pb.h>

#include <yt/core/ytree/node.h>

#include <yt/core/compression/codec.h>

#include <contrib/libs/grpc/include/grpc/grpc.h>
#include <contrib/libs/grpc/include/grpc/byte_buffer.h>
#include <contrib/libs/grpc/include/grpc/byte_buffer_reader.h>

#include <contrib/libs/protobuf/io/zero_copy_stream_impl_lite.h>
#include <contrib/libs/grpc/include/grpc/support/alloc.h>

namespace NYT {
namespace NRpc {
namespace NGrpc {

using NYTree::ENodeType;

////////////////////////////////////////////////////////////////////////////////

TGprString MakeGprString(char* str)
{
    return TGprString(str, gpr_free);
}

TStringBuf ToStringBuf(grpc_slice slice)
{
    return TStringBuf(
        reinterpret_cast<const char*>(GRPC_SLICE_START_PTR(slice)),
        GRPC_SLICE_LENGTH(slice));
}

TString ToString(grpc_slice slice)
{
    return TString(ToStringBuf(slice));
}

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
        if (ToStringBuf(metadata.key) == key) {
            return ToStringBuf(metadata.value);
        }
    }

    return TStringBuf();
}

////////////////////////////////////////////////////////////////////////////////

void TGrpcMetadataArrayBuilder::Add(const char* key, TString value)
{
    grpc_metadata metadata;
    metadata.key = grpc_slice_from_static_string(key);
    metadata.value = grpc_slice_from_static_buffer(value.c_str(), value.length());
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

TGrpcChannelArgs::TGrpcChannelArgs(const THashMap<TString, NYTree::INodePtr>& args)
{
    for (const auto& pair : args) {
        Items_.emplace_back();
        auto& item = Items_.back();
        const auto& key = pair.first;
        const auto& node = pair.second;
        item.key = const_cast<char*>(key.c_str());

        auto setIntegerValue = [&] (auto value) {
            item.type = GRPC_ARG_INTEGER;
            if (value < std::numeric_limits<int>::min() || value > std::numeric_limits<int>::max()) {
                THROW_ERROR_EXCEPTION("Value %v of GRPC argument %Qv is out of range",
                    value,
                    node->GetType(),
                    key);
            }
            item.value.integer = static_cast<int>(value);
        };

        auto setStringValue = [&] (const auto& value) {
            item.type = GRPC_ARG_STRING;
            item.value.string = const_cast<char*>(value.c_str());
        };

        switch (node->GetType()) {
            case ENodeType::Int64:
                setIntegerValue(node->GetValue<i64>());
                break;
            case ENodeType::Uint64:
                setIntegerValue(node->GetValue<ui64>());
                break;
            case ENodeType::String:
                setStringValue(node->GetValue<TString>());
                break;
            default:
                THROW_ERROR_EXCEPTION("Invalid type %Qlv of GRPC argument %Qv in channel configuration",
                    node->GetType(),
                    key);
        }
    }

    Native_.num_args = args.size();
    Native_.args = Items_.data();
}

grpc_channel_args* TGrpcChannelArgs::Unwrap()
{
    return &Native_;
}

////////////////////////////////////////////////////////////////////////////////

TGrpcPemKeyCertPair::TGrpcPemKeyCertPair(TString privateKey, TString certChain)
    : Native_({
        privateKey.c_str(),
        certChain.c_str()
    })
    , PrivateKey_(std::move(privateKey))
    , CertChain_(std::move(certChain))
{ }

grpc_ssl_pem_key_cert_pair* TGrpcPemKeyCertPair::Unwrap()
{
    return &Native_;
}

////////////////////////////////////////////////////////////////////////////////

struct TMessageTag
{ };

TSharedRef ByteBufferToEnvelopedMessage(grpc_byte_buffer* buffer)
{
    NYT::NProto::TSerializedMessageEnvelope envelope;
    // Codec remains "none".

    TEnvelopeFixedHeader fixedHeader;
    fixedHeader.EnvelopeSize = envelope.ByteSize();
    fixedHeader.MessageSize = static_cast<ui32>(grpc_byte_buffer_length(buffer));

    size_t totalSize =
        sizeof (TEnvelopeFixedHeader) +
        fixedHeader.EnvelopeSize +
        fixedHeader.MessageSize;

    auto data = TSharedMutableRef::Allocate<TMessageTag>(totalSize, false);

    char* targetFixedHeader = data.Begin();
    char* targetHeader = targetFixedHeader + sizeof (TEnvelopeFixedHeader);
    char* targetMessage = targetHeader + fixedHeader.EnvelopeSize;

    memcpy(targetFixedHeader, &fixedHeader, sizeof (fixedHeader));
    YCHECK(envelope.SerializeToArray(targetHeader, fixedHeader.EnvelopeSize));

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
    const char* sourceMessage = sourceHeader + fixedHeader->EnvelopeSize;

    NYT::NProto::TSerializedMessageEnvelope envelope;
    YCHECK(envelope.ParseFromArray(sourceHeader, fixedHeader->EnvelopeSize));

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
    NYT::NProto::TError protoError;
    ToProto(&protoError, error);
    YCHECK(protoError.SerializeToZeroCopyStream(&output));
    return serializedError;
}

TError DeserializeError(TStringBuf serializedError)
{
    NYT::NProto::TError protoError;
    google::protobuf::io::ArrayInputStream input(serializedError.data(), serializedError.size());
    if (!protoError.ParseFromZeroCopyStream(&input)) {
        THROW_ERROR_EXCEPTION("Error deserializing error");
    }
    return FromProto<TError>(protoError);
}

TGrpcPemKeyCertPair LoadPemKeyCertPair(const TSslPemKeyCertPairConfigPtr& config)
{
    return TGrpcPemKeyCertPair(
        config->PrivateKey->LoadBlob(),
        config->CertChain->LoadBlob());
}

TGrpcChannelCredentialsPtr LoadChannelCredentials(const TChannelCredentialsConfigPtr& config)
{
    auto rootCerts = config->PemRootCerts ? config->PemRootCerts->LoadBlob() : TString();
    auto keyCertPair = LoadPemKeyCertPair(config->PemKeyCertPair);
    return TGrpcChannelCredentialsPtr(grpc_ssl_credentials_create(
        rootCerts ? rootCerts.c_str() : nullptr,
        keyCertPair.Unwrap(),
        nullptr));
}

TGrpcServerCredentialsPtr LoadServerCredentials(const TServerCredentialsConfigPtr& config)
{
    auto rootCerts = config->PemRootCerts? config->PemRootCerts->LoadBlob() : TString();
    std::vector<TGrpcPemKeyCertPair> keyCertPairs;
    std::vector<grpc_ssl_pem_key_cert_pair> nativeKeyCertPairs;
    for (const auto& pairConfig : config->PemKeyCertPairs) {
        keyCertPairs.push_back(LoadPemKeyCertPair(pairConfig));
        nativeKeyCertPairs.push_back(*keyCertPairs.back().Unwrap());
    }
    return TGrpcServerCredentialsPtr(grpc_ssl_server_credentials_create_ex(
        rootCerts ? rootCerts.c_str() : nullptr,
        nativeKeyCertPairs.data(),
        nativeKeyCertPairs.size(),
        static_cast<grpc_ssl_client_certificate_request_type>(config->ClientCertificateRequest),
        nullptr));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NGrpc
} // namespace NRpc
} // namespace NYT
