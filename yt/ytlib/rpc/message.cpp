#include "message.h"
#include "rpc.pb.h"

#include "../misc/serialize.h"
#include "../logging/log.h"

#include <contrib/libs/protobuf/io/zero_copy_stream_impl_lite.h>

namespace NYT {
namespace NRpc {

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = RpcLogger;

////////////////////////////////////////////////////////////////////////////////

// TODO: move to misc/serialize.h/cpp
// TODO: use these functions in TMetaStatePart
bool SerializeMessage(google::protobuf::Message* message, TBlob* data)
{
    int size = message->ByteSize();
    data->resize(size);
    google::protobuf::io::ArrayOutputStream aos(data->begin(), size);
    return message->SerializeToZeroCopyStream(&aos);
}

bool DeserializeMessage(google::protobuf::Message* message, TRef data)
{
    google::protobuf::io::ArrayInputStream ais(data.Begin(), data.Size());
    return message->ParseFromZeroCopyStream(&ais);
}


////////////////////////////////////////////////////////////////////////////////

TRpcRequestMessage::TRpcRequestMessage(
    TRequestId requestId,
    Stroka serviceName,
    Stroka methodName,
    TBlob& body,
    yvector<TSharedRef>& attachments)
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
    Parts.push_back(TSharedRef(body));

    for (yvector<TSharedRef>::iterator it = attachments.begin();
         it != attachments.end();
         ++it)
    {
        Parts.push_back(TSharedRef(*it));
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
    TBlob& body,
    yvector<TSharedRef>& attachments)
{
    TResponseHeader responseHeader;
    responseHeader.SetRequestId(requestId.ToProto());
    responseHeader.SetErrorCode(errorCode);

    TBlob header;
    if (!SerializeMessage(&responseHeader, &header)) {
        LOG_FATAL("Error serializing response header");
    }

    Parts.push_back(TSharedRef(header));
    Parts.push_back(TSharedRef(body));

    for (yvector<TSharedRef>::iterator it = attachments.begin();
        it != attachments.end();
        ++it)
    {
        Parts.push_back(TSharedRef(*it));
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
