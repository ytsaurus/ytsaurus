#include "message.h"

#include "../misc/serialize.h"
#include "../logging/log.h"

#include <contrib/libs/protobuf/io/zero_copy_stream_impl_lite.h>

namespace NYT {
namespace NRpc {

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = TRpcManager::Get()->GetLogger();

////////////////////////////////////////////////////////////////////////////////

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

TBlobMessage::TBlobMessage(TBlob& blob)
{
    Parts.push_back(TSharedRef(blob));
}

TBlobMessage::TBlobMessage(TBlob& blob, yvector<TRef>& parts)
{
    TSharedRef::TBlobPtr sharedBlob = new TBlob();
    blob.swap(*sharedBlob);
    for (yvector<TRef>::const_iterator it = parts.begin();
         it != parts.end();
         ++it)
    {
        Parts.push_back(TSharedRef(sharedBlob, *it));
    }
}

const yvector<TSharedRef>& TBlobMessage::GetParts()
{
    return Parts;
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
    requestHeader.SetRequestId(ProtoGuidFromGuid(requestId));
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
    responseHeader.SetRequestId(ProtoGuidFromGuid(requestId));
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
    responseHeader.SetRequestId(ProtoGuidFromGuid(requestId));
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
