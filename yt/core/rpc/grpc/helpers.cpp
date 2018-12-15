#include "helpers.h"
#include "config.h"

#include <yt/core/misc/protobuf_helpers.h>
#include <yt/core/misc/finally.h>

#include <yt/core/misc/proto/protobuf_helpers.pb.h>
#include <yt/core/misc/proto/error.pb.h>

#include <yt/core/ytree/node.h>

#include <yt/core/compression/codec.h>

#include <contrib/libs/grpc/include/grpc/grpc.h>
#include <contrib/libs/grpc/include/grpc/byte_buffer.h>

#include <contrib/libs/protobuf/io/zero_copy_stream_impl_lite.h>

#include <contrib/libs/grpc/include/grpc/support/alloc.h>

#include <contrib/libs/openssl/include/openssl/pem.h>

#include <array>

namespace NYT::NRpc::NGrpc {

using NYTree::ENodeType;

////////////////////////////////////////////////////////////////////////////////

static constexpr ui32 MinusOne = static_cast<ui32>(-1);

////////////////////////////////////////////////////////////////////////////////

TGprString MakeGprString(char* str)
{
    return TGprString(str, gpr_free);
}

TStringBuf ToStringBuf(const grpc_slice& slice)
{
    return TStringBuf(
        reinterpret_cast<const char*>(GRPC_SLICE_START_PTR(slice)),
        GRPC_SLICE_LENGTH(slice));
}

TString ToString(const grpc_slice& slice)
{
    return TString(ToStringBuf(slice));
}

////////////////////////////////////////////////////////////////////////////////

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

TGrpcByteBufferStream::TGrpcByteBufferStream(grpc_byte_buffer* buffer)
    : CurrentSlice_(grpc_empty_slice())
    , RemainingBytes_(grpc_byte_buffer_length(buffer))
{
    grpc_byte_buffer_reader_init(&Reader_, buffer);
}

TGrpcByteBufferStream::~TGrpcByteBufferStream()
{
    grpc_byte_buffer_reader_destroy(&Reader_);
    grpc_slice_unref(CurrentSlice_);
}

bool TGrpcByteBufferStream::ReadNextSlice()
{
    grpc_slice_unref(CurrentSlice_);
    CurrentSlice_ = grpc_empty_slice();

    if (grpc_byte_buffer_reader_next(&Reader_, &CurrentSlice_) == 0) {
        return false;
    }

    AvailableBytes_ = GRPC_SLICE_LENGTH(CurrentSlice_);
    return true;
}

size_t TGrpcByteBufferStream::DoRead(void* buf, size_t len)
{
    // NB: Theoretically empty slice can be read, skipping such
    // slices to avoid early EOS.
    while (AvailableBytes_ == 0) {
        if (!ReadNextSlice()) {
            return 0;
        }
    }

    const auto* sliceData = GRPC_SLICE_START_PTR(CurrentSlice_);
    auto offset = GRPC_SLICE_LENGTH(CurrentSlice_) - AvailableBytes_;

    ui32 toRead = std::min(static_cast<ui32>(len), AvailableBytes_);
    ::memcpy(buf, sliceData + offset, toRead);
    AvailableBytes_ -= toRead;
    RemainingBytes_ -= toRead;

    return toRead;
}

bool TGrpcByteBufferStream::IsExhausted() const
{
    return RemainingBytes_ == 0;
}

////////////////////////////////////////////////////////////////////////////////

struct TMessageTag
{ };

TMessageWithAttachments ByteBufferToMessageWithAttachments(
    grpc_byte_buffer* buffer,
    std::optional<ui32> messageBodySize)
{
    TMessageWithAttachments result;

    ui32 bufferSize = grpc_byte_buffer_length(buffer);

    // NB: Message body size is not specified, assuming that
    // the whole data is message body.
    if (!messageBodySize) {
        messageBodySize = bufferSize;
    }

    NYT::NProto::TSerializedMessageEnvelope envelope;
    // Codec remains "none".

    TEnvelopeFixedHeader fixedHeader;
    fixedHeader.EnvelopeSize = envelope.ByteSize();
    fixedHeader.MessageSize = *messageBodySize;

    size_t totalMessageSize =
        sizeof (TEnvelopeFixedHeader) +
        fixedHeader.EnvelopeSize +
        fixedHeader.MessageSize;

    auto data = TSharedMutableRef::Allocate<TMessageTag>(
        totalMessageSize,
        /* initializeStorage */ false);

    char* targetFixedHeader = data.Begin();
    char* targetHeader = targetFixedHeader + sizeof (TEnvelopeFixedHeader);
    char* targetMessage = targetHeader + fixedHeader.EnvelopeSize;

    memcpy(targetFixedHeader, &fixedHeader, sizeof (fixedHeader));
    YCHECK(envelope.SerializeToArray(targetHeader, fixedHeader.EnvelopeSize));

    TGrpcByteBufferStream stream(buffer);

    if (stream.Load(targetMessage, *messageBodySize) != *messageBodySize) {
        THROW_ERROR_EXCEPTION("Unexpected end of stream while reading message body");
    }

    result.Message = data;

    ui32 attachmentsSize = bufferSize - *messageBodySize;

    if (attachmentsSize == 0) {
        return result;
    }

    auto attachmentsData = TSharedMutableRef::Allocate<TMessageTag>(
        attachmentsSize,
        /* initializeStorage */ false);

    char* attachmentsBuffer = attachmentsData.Begin();

    while (!stream.IsExhausted()) {
        ui32 attachmentSize;

        if (stream.Load(&attachmentSize, sizeof(attachmentSize)) != sizeof(attachmentSize)) {
            THROW_ERROR_EXCEPTION("Unexpected end of stream while reading attachment size");
        }

        if (attachmentSize == MinusOne) {
            result.Attachments.push_back(TSharedRef());
        } else if (attachmentSize == 0) {
            result.Attachments.push_back(EmptySharedRef);
        } else {
            if (stream.Load(attachmentsBuffer, attachmentSize) != attachmentSize) {
                THROW_ERROR_EXCEPTION("Unexpected end of stream while reading message attachment");
            }

            result.Attachments.push_back(
                attachmentsData.Slice(attachmentsBuffer, attachmentsBuffer + attachmentSize));

            attachmentsBuffer += attachmentSize;
        }
    }

    return result;
}

TGrpcByteBufferPtr MessageWithAttachmentsToByteBuffer(const TMessageWithAttachments& messageWithAttachments)
{
    struct THolder
    {
        TSharedRef Data;
    };

    auto sliceFromRef = [] (TSharedRef ref) {
        auto* holder = new THolder();
        holder->Data = std::move(ref);
        return grpc_slice_new_with_user_data(
            const_cast<char*>(holder->Data.Begin()),
            holder->Data.Size(),
            [] (void* untypedHolder) {
                delete static_cast<THolder*>(untypedHolder);
            },
            holder);
    };

    std::vector<grpc_slice> slices;
    slices.reserve(1 + 2 * messageWithAttachments.Attachments.size());

    slices.push_back(sliceFromRef(messageWithAttachments.Message));

    for (const auto& attachment : messageWithAttachments.Attachments) {
        ui32 size = attachment ? attachment.Size() : MinusOne;
        slices.push_back(grpc_slice_from_copied_buffer(reinterpret_cast<const char*>(&size), sizeof(size)));
        if (attachment) {
            slices.push_back(sliceFromRef(attachment));
        }
    }

    auto* buffer = grpc_raw_byte_buffer_create(slices.data(), slices.size());

    for (const auto& slice : slices) {
        grpc_slice_unref(slice);
    }

    return TGrpcByteBufferPtr(buffer);
}

TSharedRef ExtractMessageFromEnvelopedMessage(const TSharedRef& data)
{
    YCHECK(data.Size() >= sizeof(TEnvelopeFixedHeader));
    const auto* fixedHeader = reinterpret_cast<const TEnvelopeFixedHeader*>(data.Begin());
    const char* sourceHeader = data.Begin() + sizeof(TEnvelopeFixedHeader);
    const char* sourceMessage = sourceHeader + fixedHeader->EnvelopeSize;

    NYT::NProto::TSerializedMessageEnvelope envelope;
    YCHECK(envelope.ParseFromArray(sourceHeader, fixedHeader->EnvelopeSize));

    auto compressedMessage = data.Slice(sourceMessage, sourceMessage + fixedHeader->MessageSize);

    auto codecId = NCompression::ECodec(envelope.codec());
    auto* codec = NCompression::GetCodec(codecId);
    return codec->Decompress(compressedMessage);
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

std::optional<TString> ParseIssuerFromX509(TStringBuf x509String)
{
    auto* bio = BIO_new(BIO_s_mem());
    auto bioGuard = Finally([&] {
        BIO_free(bio);
    });

    BIO_write(bio, x509String.data(), x509String.length());

    auto* x509 = PEM_read_bio_X509(bio, nullptr, nullptr, nullptr);
    auto x509Guard = Finally([&] {
        X509_free(x509);
    });

    if (!x509) {
        return std::nullopt;
    }

    auto* issuerName = X509_get_issuer_name(x509);

    std::array<char, 1024> buf;
    auto* issuerString = X509_NAME_oneline(issuerName, buf.data(), buf.size());
    if (!issuerString) {
        return std::nullopt;
    }

    return TString(issuerString);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc::NGrpc
