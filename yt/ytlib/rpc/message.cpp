#include "stdafx.h"
#include "message.h"
#include "rpc.pb.h"

#include "../misc/serialize.h"

namespace NYT {
namespace NRpc {

using namespace NBus;
using namespace NProto;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = RpcLogger;

////////////////////////////////////////////////////////////////////////////////

NBus::IMessage::TPtr CreateRequestMessage(
    const TRequestId& requestId,
    const Stroka& path,
    const Stroka& verb,
    TBlob&& body,
    const yvector<TSharedRef>& attachments)
{
    yvector<TSharedRef> parts;

    TRequestHeader requestHeader;
    requestHeader.SetRequestId(requestId.ToProto());
    requestHeader.SetPath(path);
    requestHeader.SetVerb(verb);

    TBlob header;
    if (!SerializeProtobuf(&requestHeader, &header)) {
        LOG_FATAL("Could not serialize request header");
    }

    parts.push_back(TSharedRef(MoveRV(header)));
    parts.push_back(TSharedRef(MoveRV(body)));

    FOREACH(const auto& attachment, attachments) {
        parts.push_back(attachment);
    }

    return CreateMessageFromParts(MoveRV(parts));
}

NBus::IMessage::TPtr CreateResponseMessage(
    const TRequestId& requestId,
    const TError& error,
    const TSharedRef& body,
    const yvector<TSharedRef>& attachments)
{
    yvector<TSharedRef> parts;

    TResponseHeader header;
    header.SetRequestId(requestId.ToProto());
    header.SetErrorCode(error.GetCode());
    header.SetErrorMessage(error.GetMessage());

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
    const TRequestId& requestId,
    const TError& error)
{
    TResponseHeader header;
    header.SetRequestId(requestId.ToProto());
    header.SetErrorCode(error.GetCode());
    header.SetErrorMessage(error.GetMessage());

    TBlob headerBlob;
    if (!SerializeProtobuf(&header, &headerBlob)) {
        LOG_FATAL("Error serializing error response header");
    }

    return CreateMessageFromPart(MoveRV(headerBlob));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT
