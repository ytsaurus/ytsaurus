#include "message.h"
#include "private.h"
#include "service.h"

#include <yt/core/misc/protobuf_helpers.h>

#include <yt/core/rpc/proto/rpc.pb.h>

namespace NYT {
namespace NRpc {

using namespace NBus;
using namespace NRpc::NProto;

////////////////////////////////////////////////////////////////////////////////

#pragma pack(push, 1)

struct TFixedMessageHeader
{
    EMessageType Type;
};

#pragma pack(pop)

////////////////////////////////////////////////////////////////////////////////

TSharedRef SerializeToProtoWithHeader(
    const TFixedMessageHeader& fixedHeader,
    const google::protobuf::MessageLite& message)
{
    size_t messageSize = message.ByteSize();
    size_t totalSize = sizeof(fixedHeader) + messageSize;
    struct TSerializedMessageTag { };
    auto data = TSharedMutableRef::Allocate<TSerializedMessageTag>(totalSize, false);
    ::memcpy(data.Begin(), &fixedHeader, sizeof(fixedHeader));
    YCHECK(message.SerializeToArray(data.Begin() + sizeof(fixedHeader), messageSize));
    return data;
}

bool DeserializeFromProtoWithHeader(
    google::protobuf::MessageLite* message,
    const TRef& data)
{
    if (data.Size() < sizeof(TFixedMessageHeader)) {
        return false;
    }
    return message->ParsePartialFromArray(
        data.Begin() + sizeof(TFixedMessageHeader),
        data.Size() - sizeof(TFixedMessageHeader));
}

////////////////////////////////////////////////////////////////////////////////

TSharedRefArray CreateRequestMessage(
    const TRequestHeader& header,
    const TSharedRef& body,
    const std::vector<TSharedRef>& attachments)
{
    std::vector<TSharedRef> parts;
    parts.reserve(2 + attachments.size());

    parts.push_back(SerializeToProtoWithHeader(TFixedMessageHeader{EMessageType::Request}, header));

    parts.push_back(body);

    for (const auto& attachment : attachments) {
        parts.push_back(attachment);
    }

    return TSharedRefArray(std::move(parts));
}

TSharedRefArray CreateRequestCancelationMessage(
    const TRequestCancelationHeader& header)
{
    auto headerData = SerializeToProtoWithHeader(TFixedMessageHeader{EMessageType::RequestCancelation}, header);
    return TSharedRefArray(std::move(headerData));
}

TSharedRefArray CreateResponseMessage(
    const TResponseHeader& header,
    const TSharedRef& body,
    const std::vector<TSharedRef>& attachments)
{
    std::vector<TSharedRef> parts;
    parts.reserve(2 + attachments.size());

    parts.push_back(SerializeToProtoWithHeader(TFixedMessageHeader{EMessageType::Response}, header));

    parts.push_back(body);

    for (const auto& attachment : attachments) {
        parts.push_back(attachment);
    }

    return TSharedRefArray(std::move(parts));
}

TSharedRefArray CreateResponseMessage(
    const ::google::protobuf::MessageLite& body,
    const std::vector<TSharedRef>& attachments)
{
    auto serializedBody = SerializeProtoToRefWithEnvelope(body, NCompression::ECodec::None, false);

    return CreateResponseMessage(
        TResponseHeader(),
        serializedBody,
        attachments);
}

TSharedRefArray CreateErrorResponseMessage(
    const TResponseHeader& header)
{
    auto headerData = SerializeToProtoWithHeader(TFixedMessageHeader{EMessageType::Response}, header);
    return TSharedRefArray(std::move(headerData));
}

TSharedRefArray CreateErrorResponseMessage(
    const TRequestId& requestId,
    const TError& error)
{
    TResponseHeader header;
    ToProto(header.mutable_request_id(), requestId);
    if (!error.IsOK()) {
        ToProto(header.mutable_error(), error);
    }
    return CreateErrorResponseMessage(header);
}

TSharedRefArray CreateErrorResponseMessage(
    const TError& error)
{
    TResponseHeader header;
    if (!error.IsOK()) {
        ToProto(header.mutable_error(), error);
    }
    return CreateErrorResponseMessage(header);
}

////////////////////////////////////////////////////////////////////////////////

EMessageType GetMessageType(const TSharedRefArray& message)
{
    if (message.Size() < 1) {
        return EMessageType::Unknown;
    }

    const auto& headerPart = message[0];
    if (headerPart.Size() < sizeof(TFixedMessageHeader)) {
        return EMessageType::Unknown;
    }

    const auto* header = reinterpret_cast<const TFixedMessageHeader*>(headerPart.Begin());
    return header->Type;
}

bool ParseRequestHeader(
    const TSharedRefArray& message,
    TRequestHeader* header)
{
    if (GetMessageType(message) != EMessageType::Request) {
        return false;
    }

    return DeserializeFromProtoWithHeader(header, message[0]);
}

TSharedRefArray SetRequestHeader(
    const TSharedRefArray& message,
    const TRequestHeader& header)
{
    Y_ASSERT(GetMessageType(message) == EMessageType::Request);
    auto parts = message.ToVector();
    parts[0] = SerializeToProtoWithHeader(TFixedMessageHeader{EMessageType::Request}, header);
    return TSharedRefArray(parts);
}

bool ParseResponseHeader(
    const TSharedRefArray& message,
    TResponseHeader* header)
{
    if (GetMessageType(message) != EMessageType::Response) {
        return false;
    }

    return DeserializeFromProtoWithHeader(header, message[0]);
}

TSharedRefArray SetResponseHeader(
    const TSharedRefArray& message,
    const TResponseHeader& header)
{
    Y_ASSERT(GetMessageType(message) == EMessageType::Response);
    auto parts = message.ToVector();
    parts[0] = SerializeToProtoWithHeader(TFixedMessageHeader{EMessageType::Response}, header);
    return TSharedRefArray(parts);
}

void MergeRequestHeaderExtensions(
    TRequestHeader* to,
    const TRequestHeader& from)
{
#define X(name) \
    if (from.HasExtension(name)) { \
        to->MutableExtension(name)->CopyFrom(from.GetExtension(name)); \
    }

    X(TTracingExt::tracing_ext)

#undef X
}

bool ParseRequestCancelationHeader(
    const TSharedRefArray& message,
    TRequestCancelationHeader* header)
{
    if (GetMessageType(message) != EMessageType::RequestCancelation) {
        return false;
    }

    return DeserializeFromProtoWithHeader(header, message[0]);
}

////////////////////////////////////////////////////////////////////////////////

i64 GetMessageBodySize(const TSharedRefArray& message)
{
    return message.Size() >= 2 ? message[1].Size() : 0;
}

int GetMessageAttachmentCount(const TSharedRefArray& message)
{
    return std::max(message.Size() - 2, 0);
}

i64 GetTotalMesageAttachmentSize(const TSharedRefArray& message)
{
    i64 result = 0;
    for (int index = 2; index < message.Size(); ++index) {
        result += message[index].Size();
    }
    return result;
}

TError CheckBusMessageLimits(const TSharedRefArray& message)
{
    if (message.Size() > NBus::MaxMessagePartCount) {
        return TError(
            NRpc::EErrorCode::TransportError,
            "RPC message contains too many attachments: %v > %v",
            message.Size() - 2,
            NBus::MaxMessagePartCount - 2);
    }

    if (message.Size() < 2) {
        return TError();
    }

    if (message[1].Size() > NBus::MaxMessagePartSize) {
        return TError(
            NRpc::EErrorCode::TransportError,
            "RPC message body is too large: %v > %v",
            message[1].Size(),
            NBus::MaxMessagePartSize);
    }

    for (size_t index = 2; index < message.Size(); ++index) {
        if (message[index].Size() > NBus::MaxMessagePartSize) {
            return TError(
                NRpc::EErrorCode::TransportError,
                "RPC message attachment %v is too large: %v > %v",
                index - 2,
                message[index].Size(),
                NBus::MaxMessagePartSize);
        }
    }

    return TError();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT
