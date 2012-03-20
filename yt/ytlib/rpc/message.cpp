#include "stdafx.h"
#include "message.h"
#include <ytlib/rpc/rpc.pb.h>

#include <ytlib/misc/serialize.h>

namespace NYT {
namespace NRpc {

using namespace NBus;
using namespace NProto;

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
    if (!SerializeProtobuf(&header, &headerBlob)) {
        LOG_FATAL("Could not serialize request header");
    }

    parts.push_back(TSharedRef(MoveRV(headerBlob)));
    parts.push_back(TSharedRef(MoveRV(body)));

    FOREACH(const auto& attachment, attachments) {
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
    if (!SerializeProtobuf(&header, &headerBlob)) {
        LOG_FATAL("Error serializing response header");
    }

    parts.push_back(TSharedRef(MoveRV(headerBlob)));
    parts.push_back(body);

    FOREACH(const auto& attachment, attachments) {
        parts.push_back(attachment);
    }

    return CreateMessageFromParts(MoveRV(parts));
}

NBus::IMessage::TPtr CreateErrorResponseMessage(
    const TResponseHeader& header)
{
    TBlob headerBlob;
    if (!SerializeProtobuf(&header, &headerBlob)) {
        LOG_FATAL("Error serializing error response header");
    }

    return CreateMessageFromPart(MoveRV(headerBlob));
}

NBus::IMessage::TPtr CreateErrorResponseMessage(
    const TRequestId& requestId,
    const TError& error)
{
    TResponseHeader header;
    header.set_request_id(requestId.ToProto());
    header.set_error_code(error.GetCode());
    if (error.GetCode() != TError::OK) {
        header.set_error_message(error.GetMessage());
    }

    return CreateErrorResponseMessage(header);
}

NBus::IMessage::TPtr CreateErrorResponseMessage(
    const TError& error)
{
    TResponseHeader header;
    header.set_error_code(error.GetCode());
    header.set_error_message(error.GetMessage());

    return CreateErrorResponseMessage(header);
}

TRequestHeader GetRequestHeader(IMessage* message)
{
    TRequestHeader header;
    const auto& parts = message->GetParts();
    YASSERT(!parts.empty());

    if (!DeserializeProtobuf(&header, parts[0])) {
        LOG_FATAL("Error deserializing request header");
    }

    return header;
}

IMessage::TPtr SetRequestHeader(IMessage* message, const TRequestHeader& header)
{
    TBlob headerData;
    if (!SerializeProtobuf(&header, &headerData)) {
        LOG_FATAL("Error serializing request header");
    }

    auto parts = message->GetParts();
    YASSERT(!parts.empty());
    parts[0] = TSharedRef(MoveRV(headerData));

    return CreateMessageFromParts(parts);
}

TResponseHeader GetResponseHeader(IMessage* message)
{
    TResponseHeader header;
    const auto& parts = message->GetParts();
    YASSERT(parts.size() >= 1);

    if (!DeserializeProtobuf(&header, parts[0])) {
        LOG_FATAL("Error deserializing response header");
    }

    return header;
}

IMessage::TPtr SetResponseHeader(IMessage* message, const TResponseHeader& header)
{
    TBlob headerData;
    if (!SerializeProtobuf(&header, &headerData)) {
        LOG_FATAL("Error serializing response header");
    }

    auto parts = message->GetParts();
    YASSERT(!parts.empty());
    parts[0] = TSharedRef(MoveRV(headerData));

    return CreateMessageFromParts(parts);
}

TError GetResponseError(const NProto::TResponseHeader& header)
{
    if (header.error_code() == TError::OK) {
        return TError();
    } else {
        return TError(header.error_code(), header.error_message());
    }
}

void SetResponseError(TResponseHeader& header, const TError& error)
{
    if (error.IsOK()) {
        header.set_error_code(TError::OK);
        header.clear_error_message();
    } else {
        header.set_error_code(error.GetCode());
        header.set_error_message(error.GetMessage());
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT
