#pragma once

#include "private.h"

#include <yt/core/misc/small_vector.h>
#include <yt/core/misc/ref.h>

#include <contrib/libs/grpc/include/grpc/grpc.h>
#include <contrib/libs/grpc/include/grpc/impl/codegen/grpc_types.h>

namespace NYT {
namespace NRpc {
namespace NGrpc {

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
    explicit operator bool() const;

    T** GetPtr();
    void Reset();

private:
    T* Ptr_;
};

using TGrpcByteBufferPtr = TGrpcObjectPtr<grpc_byte_buffer, grpc_byte_buffer_destroy>;
using TGrpcCallPtr = TGrpcObjectPtr<grpc_call, grpc_call_destroy>;
using TGrpcChannelPtr = TGrpcObjectPtr<grpc_channel, grpc_channel_destroy>;
using TGrpcCompletionQueuePtr = TGrpcObjectPtr<grpc_completion_queue, grpc_completion_queue_destroy>;
using TGrpcServerPtr = TGrpcObjectPtr<grpc_server, grpc_server_destroy>;

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

TSharedRef ByteBufferToEnvelopedMessage(grpc_byte_buffer* buffer);
TGrpcByteBufferPtr EnvelopedMessageToByteBuffer(const TSharedRef& data);

TString SerializeError(const TError& error);
TError DeserializeError(const TStringBuf& serializedError);

////////////////////////////////////////////////////////////////////////////////

} // namespace NGrpc
} // namespace NRpc
} // namespace NYT

#define HELPERS_INL_H_
#include "helpers-inl.h"
#undef HELPERS_INL_H_
