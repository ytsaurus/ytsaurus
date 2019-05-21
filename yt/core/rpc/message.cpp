#include "message.h"
#include "private.h"
#include "service.h"
#include "channel.h"

#include <yt/core/misc/protobuf_helpers.h>

#include <yt/core/rpc/proto/rpc.pb.h>

namespace NYT::NRpc {

using namespace NBus;

using NYT::ToProto;
using NYT::FromProto;

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
    TRef data)
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
    const NProto::TRequestHeader& header,
    const TSharedRef& body,
    const std::vector<TSharedRef>& attachments)
{
    SmallVector<TSharedRef, TypicalMessagePartCount> parts;
    parts.reserve(2 + attachments.size());

    parts.push_back(SerializeToProtoWithHeader(TFixedMessageHeader{EMessageType::Request}, header));

    parts.push_back(body);

    for (const auto& attachment : attachments) {
        parts.push_back(attachment);
    }

    return TSharedRefArray(std::move(parts), TSharedRefArray::TMoveParts{});
}

TSharedRefArray CreateRequestMessage(
    const NProto::TRequestHeader& header,
    const TSharedRefArray& data)
{
    SmallVector<TSharedRef, TypicalMessagePartCount> parts;
    parts.reserve(1 + data.Size());

    parts.push_back(SerializeToProtoWithHeader(TFixedMessageHeader{EMessageType::Request}, header));

    for (const auto& part : data) {
        parts.push_back(part);
    }

    return TSharedRefArray(std::move(parts), TSharedRefArray::TMoveParts{});
}

TSharedRefArray CreateRequestCancelationMessage(
    const NProto::TRequestCancelationHeader& header)
{
    auto headerData = SerializeToProtoWithHeader(TFixedMessageHeader{EMessageType::RequestCancelation}, header);
    return TSharedRefArray(std::move(headerData));
}

TSharedRefArray CreateResponseMessage(
    const NProto::TResponseHeader& header,
    const TSharedRef& body,
    const std::vector<TSharedRef>& attachments)
{
    SmallVector<TSharedRef, TypicalMessagePartCount> parts;
    parts.reserve(2 + attachments.size());

    parts.push_back(SerializeToProtoWithHeader(TFixedMessageHeader{EMessageType::Response}, header));

    parts.push_back(body);

    for (const auto& attachment : attachments) {
        parts.push_back(attachment);
    }

    return TSharedRefArray(std::move(parts), TSharedRefArray::TMoveParts{});
}

TSharedRefArray CreateResponseMessage(
    const ::google::protobuf::MessageLite& body,
    const std::vector<TSharedRef>& attachments)
{
    auto serializedBody = SerializeProtoToRefWithEnvelope(body, NCompression::ECodec::None, false);

    return CreateResponseMessage(
        NProto::TResponseHeader(),
        serializedBody,
        attachments);
}

TSharedRefArray CreateErrorResponseMessage(
    const NProto::TResponseHeader& header)
{
    auto headerData = SerializeToProtoWithHeader(TFixedMessageHeader{EMessageType::Response}, header);
    return TSharedRefArray(std::move(headerData));
}

TSharedRefArray CreateErrorResponseMessage(
    TRequestId requestId,
    const TError& error)
{
    NProto::TResponseHeader header;
    ToProto(header.mutable_request_id(), requestId);
    if (!error.IsOK()) {
        ToProto(header.mutable_error(), error);
    }
    return CreateErrorResponseMessage(header);
}

TSharedRefArray CreateErrorResponseMessage(
    const TError& error)
{
    NProto::TResponseHeader header;
    if (!error.IsOK()) {
        ToProto(header.mutable_error(), error);
    }
    return CreateErrorResponseMessage(header);
}

TSharedRefArray CreateStreamingPayloadMessage(
    const NProto::TStreamingPayloadHeader& header,
    const std::vector<TSharedRef>& attachments)
{
    SmallVector<TSharedRef, TypicalMessagePartCount> parts;
    parts.reserve(1 + attachments.size());

    parts.push_back(SerializeToProtoWithHeader(TFixedMessageHeader{EMessageType::StreamingPayload}, header));

    for (const auto& attachment : attachments) {
        parts.push_back(attachment);
    }

    return TSharedRefArray(std::move(parts), TSharedRefArray::TMoveParts{});
}

TSharedRefArray CreateStreamingFeedbackMessage(
    const NProto::TStreamingFeedbackHeader& header)
{
    std::array<TSharedRef, 1> parts{
        SerializeToProtoWithHeader(TFixedMessageHeader{EMessageType::StreamingFeedback}, header)
    };
    return TSharedRefArray(std::move(parts), TSharedRefArray::TMoveParts{});
}

////////////////////////////////////////////////////////////////////////////////

void ToProto(
    NProto::TStreamingParameters* protoParameters,
    const TStreamingParameters& parameters)
{
    protoParameters->set_window_size(parameters.WindowSize);
    if (parameters.ReadTimeout) {
        protoParameters->set_read_timeout(ToProto<i64>(*parameters.ReadTimeout));
    }
    if (parameters.WriteTimeout) {
        protoParameters->set_write_timeout(ToProto<i64>(*parameters.WriteTimeout));
    }
}

void FromProto(
    TStreamingParameters* parameters,
    const NProto::TStreamingParameters& protoParameters)
{
    if (protoParameters.has_window_size()) {
        parameters->WindowSize = parameters->WindowSize;
    }
    if (protoParameters.has_read_timeout()) {
        parameters->ReadTimeout = FromProto<TDuration>(protoParameters.read_timeout());
    }
    if (protoParameters.has_write_timeout()) {
        parameters->WriteTimeout = FromProto<TDuration>(protoParameters.write_timeout());
    }
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
    NProto::TRequestHeader* header)
{
    if (GetMessageType(message) != EMessageType::Request) {
        return false;
    }

    return DeserializeFromProtoWithHeader(header, message[0]);
}

TSharedRefArray SetRequestHeader(
    const TSharedRefArray& message,
    const NProto::TRequestHeader& header)
{
    Y_ASSERT(GetMessageType(message) == EMessageType::Request);
    auto parts = message.ToVector();
    parts[0] = SerializeToProtoWithHeader(TFixedMessageHeader{EMessageType::Request}, header);
    return TSharedRefArray(std::move(parts), TSharedRefArray::TMoveParts{});
}

bool ParseResponseHeader(
    const TSharedRefArray& message,
    NProto::TResponseHeader* header)
{
    if (GetMessageType(message) != EMessageType::Response) {
        return false;
    }

    return DeserializeFromProtoWithHeader(header, message[0]);
}

TSharedRefArray SetResponseHeader(
    const TSharedRefArray& message,
    const NProto::TResponseHeader& header)
{
    Y_ASSERT(GetMessageType(message) == EMessageType::Response);
    auto parts = message.ToVector();
    parts[0] = SerializeToProtoWithHeader(TFixedMessageHeader{EMessageType::Response}, header);
    return TSharedRefArray(std::move(parts), TSharedRefArray::TMoveParts{});
}

void MergeRequestHeaderExtensions(
    NProto::TRequestHeader* to,
    const NProto::TRequestHeader& from)
{
#define X(name) \
    if (from.HasExtension(name)) { \
        to->MutableExtension(name)->CopyFrom(from.GetExtension(name)); \
    }

    X(NProto::TTracingExt::tracing_ext)

#undef X
}

bool ParseRequestCancelationHeader(
    const TSharedRefArray& message,
    NProto::TRequestCancelationHeader* header)
{
    if (GetMessageType(message) != EMessageType::RequestCancelation) {
        return false;
    }

    return DeserializeFromProtoWithHeader(header, message[0]);
}

bool ParseStreamingPayloadHeader(
    const TSharedRefArray& message,
    NProto::TStreamingPayloadHeader * header)
{
    if (GetMessageType(message) != EMessageType::StreamingPayload) {
        return false;
    }

    return DeserializeFromProtoWithHeader(header, message[0]);
}

bool ParseStreamingFeedbackHeader(
    const TSharedRefArray& message,
    NProto::TStreamingFeedbackHeader * header)
{
    if (GetMessageType(message) != EMessageType::StreamingFeedback) {
        return false;
    }

    return DeserializeFromProtoWithHeader(header, message[0]);
}

////////////////////////////////////////////////////////////////////////////////

i64 GetMessageBodySize(const TSharedRefArray& message)
{
    return message.Size() >= 2 ? static_cast<i64>(message[1].Size()) : 0;
}

int GetMessageAttachmentCount(const TSharedRefArray& message)
{
    return std::max(static_cast<int>(message.Size()) - 2, 0);
}

i64 GetTotalMessageAttachmentSize(const TSharedRefArray& message)
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

} // namespace NYT::NRpc
