#pragma once

#include "../misc/common.h"
#include "../misc/ptr.h"
#include "../misc/guid.h"
#include "../misc/enum.h"

#include "../logging/log.h"

namespace NYT {
namespace NRpc {

////////////////////////////////////////////////////////////////////////////////

extern NLog::TLogger RpcLogger;

////////////////////////////////////////////////////////////////////////////////

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
typedef TGuidHash TRequestIdHash;

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

