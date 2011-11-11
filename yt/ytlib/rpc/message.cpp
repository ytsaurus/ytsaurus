#include "stdafx.h"
#include "message.h"
#include "rpc.pb.h"

namespace NYT {
namespace NRpc {

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = RpcLogger;

////////////////////////////////////////////////////////////////////////////////

bool SerializeMessage(const google::protobuf::Message* message, TBlob* data)
{
    int size = message->ByteSize();
    data->resize(size);
    return message->SerializeToArray(data->begin(), size);
}

bool DeserializeMessage(google::protobuf::Message* message, TRef data)
{
    return message->ParseFromArray(data.Begin(), data.Size());
}

////////////////////////////////////////////////////////////////////////////////

TRpcRequestMessage::TRpcRequestMessage(
    const TRequestId& requestId,
    const Stroka& path,
    const Stroka& verb,
    TBlob* body,
    const yvector<TSharedRef>& attachments)
{
    TRequestHeader requestHeader;
    requestHeader.SetRequestId(requestId.ToProto());
    requestHeader.SetPath(path);
    requestHeader.SetVerb(verb);

    TBlob header;
    if (!SerializeMessage(&requestHeader, &header)) {
        LOG_FATAL("Could not serialize request header");
    }

    Parts.push_back(TSharedRef(header));
    Parts.push_back(TSharedRef(*body));

    FOREACH(const auto& attachment, attachments) {
        Parts.push_back(TSharedRef(attachment));
    }
}

const yvector<TSharedRef>& TRpcRequestMessage::GetParts()
{
    return Parts;
}

////////////////////////////////////////////////////////////////////////////////

TRpcResponseMessage::TRpcResponseMessage(
    const TRequestId& requestId,
    const TError& error,
    TBlob* body,
    const yvector<TSharedRef>& attachments)
{
    TResponseHeader header;
    header.SetRequestId(requestId.ToProto());
    header.SetErrorCode(error.GetCode());
    header.SetErrorCodeString(error.GetCode().ToString());
    header.SetErrorMessage(error.GetMessage());

    TBlob headerBlob;
    if (!SerializeMessage(&header, &headerBlob)) {
        LOG_FATAL("Error serializing response header");
    }

    Parts.push_back(TSharedRef(headerBlob));
    Parts.push_back(TSharedRef(*body));

    FOREACH(const auto& attachment, attachments) {
        Parts.push_back(TSharedRef(attachment));
    }
}

const yvector<TSharedRef>& TRpcResponseMessage::GetParts()
{
    return Parts;
}

////////////////////////////////////////////////////////////////////////////////

TRpcErrorResponseMessage::TRpcErrorResponseMessage(
    const TRequestId& requestId,
    const TError& error)
{
    TResponseHeader header;
    header.SetRequestId(requestId.ToProto());
    header.SetErrorCode(error.GetCode());
    header.SetErrorCodeString(error.GetCode().ToString());
    header.SetErrorMessage(error.GetMessage());

    TBlob body;
    if (!SerializeMessage(&header, &body)) {
        LOG_FATAL("Error serializing error response header");
    }
    Parts.push_back(TSharedRef(body));
}

const yvector<TSharedRef>& TRpcErrorResponseMessage::GetParts()
{
    return Parts;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT
