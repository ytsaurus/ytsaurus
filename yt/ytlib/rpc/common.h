#pragma once

#include "../misc/common.h"
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
    ((NoService)(-3))
    ((NoMethod)(-4))
    ((Timeout)(-5))
    ((ServiceError)(-6))
    ((Unavailable)(-7))
)
public:
    // Allow implicit construction of error code from integer value.
    EErrorCode(int value)
        : TBase(value)
    { }

    bool IsOK() const
    {
        return *this == OK;
    }

    bool IsRpcError() const
    {
        return *this < OK;
    }

    bool IsServiceError() const
    {
        return *this > OK;
    }
END_DECLARE_POLY_ENUM();

////////////////////////////////////////////////////////////////////////////////

typedef TGuid TRequestId;

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

