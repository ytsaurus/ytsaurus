#pragma once

#include "../misc/common.h"
#include "../misc/guid.h"
#include "../misc/enum.h"

#include "../logging/log.h"

namespace NYT {
namespace NRpc {

////////////////////////////////////////////////////////////////////////////////

extern NLog::TLogger RpcLogger;

////////////////////////////////////////////////////////////////////////////////

BEGIN_DECLARE_POLY_ENUM(EErrorCode, EErrorCode,
    ((OK)(0))
    ((TransportError)(-1))
    ((ProtocolError)(-2))
    ((NoSuchService)(-3))
    ((NoSuchMethod)(-4))
    ((Timeout)(-5))
    ((ServiceError)(-6))
    ((Unavailable)(-7))
)
public:
    // Allow implicit construction of error code from integer value.
    EErrorCode(int value)
        : TBase(value)
    { }
END_DECLARE_POLY_ENUM();

////////////////////////////////////////////////////////////////////////////////

// TODO: get rid of this
#undef GetMessage

class TError
{
public:
    TError();
    TError(EErrorCode code, const Stroka& message = "");

    EErrorCode GetCode() const;
    Stroka GetMessage() const;

    bool IsOK() const;
    bool IsRpcError() const;
    bool IsServiceError() const;

    Stroka ToString() const;

private:
    EErrorCode Code;
    Stroka Message;

};

////////////////////////////////////////////////////////////////////////////////

typedef TGuid TRequestId;
typedef TFuture<TError> TAsyncError;

////////////////////////////////////////////////////////////////////////////////

class TRpcManager
    : private TNonCopyable
{
public:
    TRpcManager();

    static TRpcManager* Get();
    Stroka GetDebugInfo();
    void Shutdown();
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT

