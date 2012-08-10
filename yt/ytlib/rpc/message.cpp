#include "stdafx.h"
#include "message.h"
#include "private.h"

#include <ytlib/misc/protobuf_helpers.h>
#include <ytlib/rpc/rpc.pb.h>

namespace NYT {
namespace NRpc {

using namespace NBus;

////////////////////////////////////////////////////////////////////////////////

IMessagePtr CreateRequestMessage(
    const NProto::TRequestHeader& header,
    TBlob&& body,
    const std::vector<TSharedRef>& attachments)
{
    std::vector<TSharedRef> parts;

    TBlob headerBlob;
    YCHECK(SerializeToProto(&header, &headerBlob));

    parts.push_back(TSharedRef(MoveRV(headerBlob)));
    parts.push_back(TSharedRef(MoveRV(body)));

    FOREACH (const auto& attachment, attachments) {
        parts.push_back(attachment);
    }

    return CreateMessageFromParts(MoveRV(parts));
}

IMessagePtr CreateResponseMessage(
    const NProto::TResponseHeader& header,
    const TSharedRef& body,
    const std::vector<TSharedRef>& attachments)
{
    std::vector<TSharedRef> parts;

    TBlob headerBlob;
    YCHECK(SerializeToProto(&header, &headerBlob));

    parts.push_back(TSharedRef(MoveRV(headerBlob)));
    parts.push_back(body);

    FOREACH (const auto& attachment, attachments) {
        parts.push_back(attachment);
    }

    return CreateMessageFromParts(MoveRV(parts));
}

IMessagePtr CreateErrorResponseMessage(
    const NProto::TResponseHeader& header)
{
    TBlob headerBlob;
    YCHECK(SerializeToProto(&header, &headerBlob));
    return CreateMessageFromPart(MoveRV(headerBlob));
}

IMessagePtr CreateErrorResponseMessage(
    const TRequestId& requestId,
    const TError& error)
{
    NProto::TResponseHeader header;
    *header.mutable_request_id() = requestId.ToProto();
    *header.mutable_error() = error.ToProto();
    return CreateErrorResponseMessage(header);
}

IMessagePtr CreateErrorResponseMessage(
    const TError& error)
{
    NProto::TResponseHeader header;
    *header.mutable_error() = error.ToProto();
    return CreateErrorResponseMessage(header);
}

bool ParseRequestHeader(
    IMessagePtr message,
    NProto::TRequestHeader* header)
{
    const auto& parts = message->GetParts();
    if (parts.empty()) {
        return false;
    }
    return DeserializeFromProto(header, parts[0]);
}

IMessagePtr SetRequestHeader(IMessagePtr message, const NProto::TRequestHeader& header)
{
    TBlob headerData;
    YCHECK(SerializeToProto(&header, &headerData));

    auto parts = message->GetParts();
    YASSERT(!parts.empty());
    parts[0] = TSharedRef(MoveRV(headerData));

    return CreateMessageFromParts(parts);
}

bool ParseResponseHeader(
    IMessagePtr message,
    NProto::TResponseHeader* header)
{
    const auto& parts = message->GetParts();
    if (parts.empty()) {
        return false;
    }
    return DeserializeFromProto(header, parts[0]);
}

IMessagePtr SetResponseHeader(IMessagePtr message, const NProto::TResponseHeader& header)
{
    TBlob headerData;
    YCHECK(SerializeToProto(&header, &headerData));

    auto parts = message->GetParts();
    YASSERT(!parts.empty());
    parts[0] = TSharedRef(MoveRV(headerData));

    return CreateMessageFromParts(parts);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT
