#pragma once

#include "private.h"

#include <yt/core/misc/small_vector.h>
#include <yt/core/misc/ref.h>

#include <yt/core/crypto/public.h>

#include <contrib/libs/grpc/include/grpc/grpc.h>
#include <contrib/libs/grpc/include/grpc/grpc_security.h>
#include <contrib/libs/grpc/include/grpc/impl/codegen/grpc_types.h>
#include <contrib/libs/grpc/include/grpc/byte_buffer_reader.h>

namespace NYT::NRpc::NGrpc {

////////////////////////////////////////////////////////////////////////////////

using TGprString = std::unique_ptr<char, void(*)(void*)>;
TGprString MakeGprString(char* str);

TStringBuf ToStringBuf(const grpc_slice& slice);
TString ToString(const grpc_slice& slice);

////////////////////////////////////////////////////////////////////////////////

template <class T, void(*Deletor)(T*)>
class TGrpcObjectPtr
{
public:
    TGrpcObjectPtr();
    explicit TGrpcObjectPtr(T* obj);
    TGrpcObjectPtr(TGrpcObjectPtr&& other);
    ~TGrpcObjectPtr();

    TGrpcObjectPtr& operator=(TGrpcObjectPtr&& other);

    TGrpcObjectPtr& operator=(const TGrpcObjectPtr&) = delete;
    TGrpcObjectPtr(const TGrpcObjectPtr& other) = delete;

    T* Unwrap();
    const T* Unwrap() const;
    explicit operator bool() const;

    T** GetPtr();
    void Reset();

private:
    T* Ptr_;
};

using TGrpcByteBufferPtr = TGrpcObjectPtr<grpc_byte_buffer, grpc_byte_buffer_destroy>;
using TGrpcCallPtr = TGrpcObjectPtr<grpc_call, grpc_call_unref>;
using TGrpcChannelPtr = TGrpcObjectPtr<grpc_channel, grpc_channel_destroy>;
using TGrpcCompletionQueuePtr = TGrpcObjectPtr<grpc_completion_queue, grpc_completion_queue_destroy>;
using TGrpcServerPtr = TGrpcObjectPtr<grpc_server, grpc_server_destroy>;
using TGrpcChannelCredentialsPtr = TGrpcObjectPtr<grpc_channel_credentials, grpc_channel_credentials_release>;
using TGrpcServerCredentialsPtr = TGrpcObjectPtr<grpc_server_credentials, grpc_server_credentials_release>;
using TGrpcAuthContextPtr = TGrpcObjectPtr<grpc_auth_context, grpc_auth_context_release>;

////////////////////////////////////////////////////////////////////////////////

class TGrpcMetadataArray
{
public:
    TGrpcMetadataArray();
    ~TGrpcMetadataArray();

    TGrpcMetadataArray(const TGrpcMetadataArray&) = delete;
    TGrpcMetadataArray(TGrpcMetadataArray&&) = delete;
    TGrpcMetadataArray& operator =(const TGrpcMetadataArray& other) = delete;
    TGrpcMetadataArray& operator =(TGrpcMetadataArray&& other) = delete;

    grpc_metadata_array* Unwrap();

    TStringBuf Find(const char* key) const;

private:
    grpc_metadata_array Native_;
};

////////////////////////////////////////////////////////////////////////////////

class TGrpcMetadataArrayBuilder
{
public:
    void Add(const char* key, TString value);
    size_t GetSize() const;

    grpc_metadata* Unwrap();

private:
    static constexpr size_t TypicalSize = 4;
    SmallVector<grpc_metadata, TypicalSize> NativeMetadata_;
    SmallVector<TString, TypicalSize> Strings_;
};

////////////////////////////////////////////////////////////////////////////////

class TGrpcCallDetails
{
public:
    TGrpcCallDetails();
    ~TGrpcCallDetails();

    TGrpcCallDetails(const TGrpcCallDetails&) = delete;
    TGrpcCallDetails(TGrpcCallDetails&&) = delete;

    TGrpcCallDetails& operator=(const TGrpcCallDetails&) = delete;
    TGrpcCallDetails& operator=(TGrpcCallDetails&&) = delete;

    grpc_call_details* Unwrap();
    grpc_call_details* operator->();

private:
    grpc_call_details Native_;
};

////////////////////////////////////////////////////////////////////////////////

class TGrpcChannelArgs
{
public:
    explicit TGrpcChannelArgs(const THashMap<TString, NYTree::INodePtr>& args);

    grpc_channel_args* Unwrap();

private:
    grpc_channel_args Native_;
    std::vector<grpc_arg> Items_;
};

////////////////////////////////////////////////////////////////////////////////

class TGrpcByteBufferStream
    : public IInputStream
{
public:
    explicit TGrpcByteBufferStream(grpc_byte_buffer* buffer);
    ~TGrpcByteBufferStream();

    bool IsExhausted() const;

private:
    grpc_byte_buffer_reader Reader_;

    grpc_slice Slice_;
    bool Started_ = false;
    ui32 AvailableBytes_ = 0;
    ui32 RemainingBytes_;

    virtual size_t DoRead(void* buf, size_t len) override;

    bool ReadNextSlice();
};

////////////////////////////////////////////////////////////////////////////////

class TGrpcPemKeyCertPair
{
public:
    TGrpcPemKeyCertPair(
        TString privateKey,
        TString certChain);

    grpc_ssl_pem_key_cert_pair* Unwrap();

private:
    grpc_ssl_pem_key_cert_pair Native_;
    TString PrivateKey_;
    TString CertChain_;
};

////////////////////////////////////////////////////////////////////////////////

struct TMessageWithAttachments
{
    TSharedRef Message;
    std::vector<TSharedRef> Attachments;
};

TMessageWithAttachments ByteBufferToMessageWithAttachments(
    grpc_byte_buffer* buffer,
    std::optional<ui32> messageBodySize);

TGrpcByteBufferPtr MessageWithAttachmentsToByteBuffer(
    const TMessageWithAttachments& messageWithAttachments);

TSharedRef ExtractMessageFromEnvelopedMessage(const TSharedRef& data);

////////////////////////////////////////////////////////////////////////////////

TString SerializeError(const TError& error);
TError DeserializeError(TStringBuf serializedError);

TGrpcPemKeyCertPair LoadPemKeyCertPair(const TSslPemKeyCertPairConfigPtr& config);
TGrpcChannelCredentialsPtr LoadChannelCredentials(const TChannelCredentialsConfigPtr& config);
TGrpcServerCredentialsPtr LoadServerCredentials(const TServerCredentialsConfigPtr& config);
std::optional<TString> ParseIssuerFromX509(TStringBuf x509String);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc::NGrpc

#define HELPERS_INL_H_
#include "helpers-inl.h"
#undef HELPERS_INL_H_
