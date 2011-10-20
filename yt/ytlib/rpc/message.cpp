#include "stdafx.h"
#include "message.h"
#include "rpc.pb.h"

namespace NYT {
namespace NRpc {

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = RpcLogger;

////////////////////////////////////////////////////////////////////////////////

bool SerializeMessage(google::protobuf::Message* message, TBlob* data)
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
    TRequestId requestId,
    Stroka serviceName,
    Stroka methodName,
    TBlob* body,
    const yvector<TSharedRef>& attachments)
{
    TRequestHeader requestHeader;
    requestHeader.SetRequestId(requestId.ToProto());
    requestHeader.SetServiceName(serviceName);
    requestHeader.SetMethodName(methodName);

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
    TRequestId requestId,
    EErrorCode errorCode,
    TBlob* body,
    const yvector<TSharedRef>& attachments)
{
    TResponseHeader responseHeader;
    responseHeader.SetRequestId(requestId.ToProto());
    responseHeader.SetErrorCode(errorCode);

    TBlob header;
    if (!SerializeMessage(&responseHeader, &header)) {
        LOG_FATAL("Error serializing response header");
    }

    Parts.push_back(TSharedRef(header));
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

TRpcErrorResponseMessage::TRpcErrorResponseMessage(TRequestId requestId, EErrorCode errorCode)
{
    TResponseHeader responseHeader;
    responseHeader.SetRequestId(requestId.ToProto());
    responseHeader.SetErrorCode(errorCode);
    TBlob body;
    if (!SerializeMessage(&responseHeader, &body)) {
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
