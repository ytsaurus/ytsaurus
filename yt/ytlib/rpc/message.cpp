#include "stdafx.h"
#include "message.h"

#include <ytlib/rpc/rpc.pb.h>
#include <ytlib/misc/protobuf_helpers.h>

namespace NYT {
namespace NRpc {

using namespace NBus;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = RpcLogger;

////////////////////////////////////////////////////////////////////////////////

NBus::IMessage::TPtr CreateRequestMessage(
    const NProto::TRequestHeader& header,
    TBlob&& body,
    const yvector<TSharedRef>& attachments)
{
    yvector<TSharedRef> parts;

    TBlob headerBlob;
    YVERIFY(SerializeToProto(&header, &headerBlob));

    parts.push_back(TSharedRef(MoveRV(headerBlob)));
    parts.push_back(TSharedRef(MoveRV(body)));

    FOREACH (const auto& attachment, attachments) {
        parts.push_back(attachment);
    }

    return CreateMessageFromParts(MoveRV(parts));
}

NBus::IMessage::TPtr CreateResponseMessage(
    const NProto::TResponseHeader& header,
    const TSharedRef& body,
    const yvector<TSharedRef>& attachments)
{
    yvector<TSharedRef> parts;

    TBlob headerBlob;
    YVERIFY(SerializeToProto(&header, &headerBlob));

    parts.push_back(TSharedRef(MoveRV(headerBlob)));
    parts.push_back(body);

    FOREACH (const auto& attachment, attachments) {
        parts.push_back(attachment);
    }

    return CreateMessageFromParts(MoveRV(parts));
}

NBus::IMessage::TPtr CreateErrorResponseMessage(
    const NProto::TResponseHeader& header)
{
    TBlob headerBlob;
    if (!SerializeToProto(&header, &headerBlob)) {
        LOG_FATAL("Error serializing error response header");
    }

    return CreateMessageFromPart(MoveRV(headerBlob));
}

NBus::IMessage::TPtr CreateErrorResponseMessage(
    const TRequestId& requestId,
    const TError& error)
{
    NProto::TResponseHeader header;
    *header.mutable_request_id() = requestId.ToProto();
    *header.mutable_error() = error.ToProto();
    return CreateErrorResponseMessage(header);
}

NBus::IMessage::TPtr CreateErrorResponseMessage(
    const TError& error)
{
    NProto::TResponseHeader header;
    *header.mutable_error() = error.ToProto();
    return CreateErrorResponseMessage(header);
}

NProto::TRequestHeader GetRequestHeader(IMessage* message)
{
    NProto::TRequestHeader header;
    const auto& parts = message->GetParts();
    YASSERT(!parts.empty());
    YVERIFY(DeserializeFromProto(&header, parts[0]));
    return header;
}

IMessage::TPtr SetRequestHeader(IMessage* message, const NProto::TRequestHeader& header)
{
    TBlob headerData;
    YVERIFY(SerializeToProto(&header, &headerData));

    auto parts = message->GetParts();
    YASSERT(!parts.empty());
    parts[0] = TSharedRef(MoveRV(headerData));

    return CreateMessageFromParts(parts);
}

NProto::TResponseHeader GetResponseHeader(IMessage* message)
{
    NProto::TResponseHeader header;
    const auto& parts = message->GetParts();
    YASSERT(parts.size() >= 1);
    YVERIFY(DeserializeFromProto(&header, parts[0]));
    return header;
}

IMessage::TPtr SetResponseHeader(IMessage* message, const NProto::TResponseHeader& header)
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
