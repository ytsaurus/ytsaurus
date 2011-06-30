#pragma once

#include "common.h"
#include "rpc.pb.h"

#include "../misc/enum.h"

namespace NYT {
namespace NRpc {

////////////////////////////////////////////////////////////////////////////////

bool SerializeMessage(google::protobuf::Message* message, TBlob* data);
bool DeserializeMessage(google::protobuf::Message* message, TRef data);

////////////////////////////////////////////////////////////////////////////////

struct IMessage
    : public virtual TRefCountedBase
{
    typedef TIntrusivePtr<IMessage> TPtr;

    virtual ~IMessage() {}
    virtual const yvector<TSharedRef>& GetParts() = 0;
};

////////////////////////////////////////////////////////////////////////////////

class TBlobMessage
    : public IMessage
{
private:
    yvector<TSharedRef> Parts;

public:
    TBlobMessage(TBlob& blob);
    TBlobMessage(TBlob& blob, yvector<TRef>& parts);

    const yvector<TSharedRef>& GetParts();
};

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

typedef TGUID TRequestId;

////////////////////////////////////////////////////////////////////////////////

class TRpcRequestMessage
    : public IMessage
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
    : public IMessage
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
    : public IMessage
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
