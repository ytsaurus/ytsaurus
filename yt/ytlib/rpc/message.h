#pragma once

#include "common.h"
#include "rpc.pb.h"

#include "../bus/message.h"

namespace NYT {
namespace NRpc {

////////////////////////////////////////////////////////////////////////////////

bool SerializeMessage(google::protobuf::Message* message, TBlob* data);
bool DeserializeMessage(google::protobuf::Message* message, TRef data);

////////////////////////////////////////////////////////////////////////////////

////////////////////////////////////////////////////////////////////////////////

class TRpcRequestMessage
    : public NBus::IMessage
{
public:
    TRpcRequestMessage(
        const TRequestId requestId,
        const Stroka serviceName,
        const Stroka methodName,
        TBlob* body,
        const yvector<TSharedRef>& attachments);

    virtual const yvector<TSharedRef>& GetParts();

private:
    yvector<TSharedRef> Parts;

};

////////////////////////////////////////////////////////////////////////////////

class TRpcResponseMessage
    : public NBus::IMessage
{
public:
    TRpcResponseMessage(
        TRequestId requestId,
        EErrorCode errorCode,
        TBlob* body,
        const yvector<TSharedRef>& attachments);

    virtual const yvector<TSharedRef>& GetParts();

private:
    yvector<TSharedRef> Parts;

};

////////////////////////////////////////////////////////////////////////////////

class TRpcErrorResponseMessage
    : public NBus::IMessage
{
public:
    TRpcErrorResponseMessage(TRequestId requestId, EErrorCode errorCode);

    virtual const yvector<TSharedRef>& GetParts();

private:
    yvector<TSharedRef> Parts;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT
