#include "stdafx.h"
#include "message.h"
#include "private.h"
#include "service.h"

#include <core/misc/protobuf_helpers.h>

#include <core/rpc/rpc.pb.h>

namespace NYT {
namespace NRpc {

using namespace NBus;

////////////////////////////////////////////////////////////////////////////////

TSharedRefArray CreateRequestMessage(
    const NProto::TRequestHeader& header,
    const TSharedRef& body,
    const std::vector<TSharedRef>& attachments)
{
    std::vector<TSharedRef> parts;

    TSharedRef headerData;
    YCHECK(SerializeToProto(header, &headerData));
    parts.push_back(headerData);

    parts.push_back(body);

    for (const auto& attachment : attachments) {
        parts.push_back(attachment);
    }

    return TSharedRefArray(std::move(parts));
}

TSharedRefArray CreateResponseMessage(
    const NProto::TResponseHeader& header,
    const TSharedRef& body,
    const std::vector<TSharedRef>& attachments)
{
    std::vector<TSharedRef> parts;

    TSharedRef headerData;
    YCHECK(SerializeToProto(header, &headerData));
    parts.push_back(headerData);

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
    NProto::TResponseHeader header;
    ToProto(header.mutable_error(), TError());

    TSharedRef serializedBody;
    YCHECK(SerializeToProtoWithEnvelope(body, &serializedBody));

    return CreateResponseMessage(
        header,
        serializedBody,
        attachments);
}

TSharedRefArray CreateResponseMessage(
    IServiceContextPtr context)
{
    YCHECK(context->IsReplied());

    NProto::TResponseHeader header;
    ToProto(header.mutable_request_id(), context->GetRequestId());
    ToProto(header.mutable_error(), context->GetError());

    return
        context->GetError().IsOK()
        ? CreateResponseMessage(
            header,
            context->GetResponseBody(),
            context->ResponseAttachments())
        : CreateErrorResponseMessage(header);
}

TSharedRefArray CreateErrorResponseMessage(
    const NProto::TResponseHeader& header)
{
    TSharedRef headerData;
    YCHECK(SerializeToProto(header, &headerData));
    return TSharedRefArray(std::move(headerData));
}

TSharedRefArray CreateErrorResponseMessage(
    const TRequestId& requestId,
    const TError& error)
{
    NProto::TResponseHeader header;
    ToProto(header.mutable_request_id(), requestId);
    ToProto(header.mutable_error(), error);
    return CreateErrorResponseMessage(header);
}

TSharedRefArray CreateErrorResponseMessage(
    const TError& error)
{
    NProto::TResponseHeader header;
    ToProto(header.mutable_error(), error);
    return CreateErrorResponseMessage(header);
}

bool ParseRequestHeader(
    TSharedRefArray message,
    NProto::TRequestHeader* header)
{
    if (message.Size() < 1) {
        return false;
    }
    return DeserializeFromProto(header, message[0]);
}

TSharedRefArray SetRequestHeader(TSharedRefArray message, const NProto::TRequestHeader& header)
{
    TSharedRef headerData;
    YCHECK(SerializeToProto(header, &headerData));

    auto parts = message.ToVector();
    YASSERT(parts.size() >= 1);
    parts[0] = headerData;

    return TSharedRefArray(parts);
}

bool ParseResponseHeader(
    TSharedRefArray message,
    NProto::TResponseHeader* header)
{
    if (message.Size() < 1) {
        return false;
    }
    return DeserializeFromProto(header, message[0]);
}

TSharedRefArray SetResponseHeader(TSharedRefArray message, const NProto::TResponseHeader& header)
{
    TSharedRef headerData;
    YCHECK(SerializeToProto(header, &headerData));

    auto parts = message.ToVector();
    YASSERT(parts.size() >= 1);
    parts[0] = headerData;

    return TSharedRefArray(parts);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT
