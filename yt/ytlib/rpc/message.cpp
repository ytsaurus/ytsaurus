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
    YVERIFY(SerializeToProto(&header, &headerBlob));

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
    YVERIFY(SerializeToProto(&header, &headerBlob));

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

NProto::TRequestHeader GetRequestHeader(IMessagePtr message)
{
    NProto::TRequestHeader header;
    const auto& parts = message->GetParts();
    YASSERT(!parts.empty());
    YVERIFY(DeserializeFromProto(&header, parts[0]));
    return header;
}

IMessagePtr SetRequestHeader(IMessagePtr message, const NProto::TRequestHeader& header)
{
    TBlob headerData;
    YVERIFY(SerializeToProto(&header, &headerData));

    auto parts = message->GetParts();
    YASSERT(!parts.empty());
    parts[0] = TSharedRef(MoveRV(headerData));

    return CreateMessageFromParts(parts);
}

NProto::TResponseHeader GetResponseHeader(IMessagePtr message)
{
    NProto::TResponseHeader header;
    const auto& parts = message->GetParts();
    YASSERT(parts.size() >= 1);
    YVERIFY(DeserializeFromProto(&header, parts[0]));
    return header;
}

IMessagePtr SetResponseHeader(IMessagePtr message, const NProto::TResponseHeader& header)
{
    TBlob headerData;
    YVERIFY(SerializeToProto(&header, &headerData));

    auto parts = message->GetParts();
    YASSERT(!parts.empty());
    parts[0] = TSharedRef(MoveRV(headerData));

    return CreateMessageFromParts(parts);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT
