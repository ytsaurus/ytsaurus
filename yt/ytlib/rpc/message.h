#pragma once

#include "common.h"
#include "rpc.pb.h"

#include "../bus/message.h"
#include "../misc/enum.h"

namespace NYT {
namespace NRpc {

////////////////////////////////////////////////////////////////////////////////

bool SerializeMessage(google::protobuf::Message* message, TBlob* data);
bool DeserializeMessage(google::protobuf::Message* message, TRef data);

////////////////////////////////////////////////////////////////////////////////

// TODO: move to a proper place
BEGIN_DECLARE_ENUM(EErrorCode,
    ((OK)(0))
    ((TransportError)(-1))
    ((ProtocolError)(-2))
    ((NoService)(-3))
    ((NoMethod)(-4))
    ((Timeout)(-5))
    ((ServiceError)(-6))
    ((Unavailable)(-7))
)
public:
    // Allow implicit construction of error code from integer value.
    EErrorCode(int value)
        : TEnumBase<EErrorCode>(value)
    { }

    // TODO: get rid of casts, compare enums as is
    bool IsOK() const
    {
        return ToValue() == OK;
    }

    bool IsRpcError() const
    {
        return ToValue() < static_cast<int>(EErrorCode::OK);
    }

    bool IsServiceError() const
    {
        return ToValue() > static_cast<int>(EErrorCode::OK);
    }
END_DECLARE_ENUM();

////////////////////////////////////////////////////////////////////////////////

typedef TGuid TRequestId;

////////////////////////////////////////////////////////////////////////////////

class TRpcRequestMessage
    : public NBus::IMessage
{
public:
    TRpcRequestMessage(
        TRequestId requestId,
        Stroka serviceName,
        Stroka methodName,
        TBlob& body,
        yvector<TSharedRef>& attachments);

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
        TBlob& body,
        yvector<TSharedRef>& attachments);

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
