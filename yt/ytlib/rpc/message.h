#pragma once

#include "common.h"
#include "rpc.pb.h"

#include "../bus/message.h"

namespace NYT {
namespace NRpc {

////////////////////////////////////////////////////////////////////////////////

bool SerializeMessage(const google::protobuf::Message* message, TBlob* data);
bool DeserializeMessage(google::protobuf::Message* message, TRef data);

////////////////////////////////////////////////////////////////////////////////

class TRpcRequestMessage
    : public NBus::IMessage
{
public:
    TRpcRequestMessage(
        const TRequestId& requestId,
        const Stroka& path,
        const Stroka& verb,
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
        const TRequestId& requestId,
        const TError& error,
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
    TRpcErrorResponseMessage(
        const TRequestId& requestId,
        const TError& error);

    virtual const yvector<TSharedRef>& GetParts();

private:
    yvector<TSharedRef> Parts;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT
