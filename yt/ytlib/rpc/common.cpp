#include "stdafx.h"
#include "common.h"

// TODO: hack

namespace NYT {
namespace NBus {

extern void ShutdownClientDispatcher();
extern Stroka GetClientDispatcherDebugInfo();

}
}

namespace NYT {
namespace NRpc {

////////////////////////////////////////////////////////////////////////////////

NLog::TLogger RpcLogger("Rpc");

////////////////////////////////////////////////////////////////////////////////

TError::TError()
{ }

TError::TError(EErrorCode code, const Stroka& message)
    : Code(code)
    , Message(message)
{ }

EErrorCode TError::GetCode() const
{
    return Code;
}

Stroka TError::GetMessage() const
{
    return Message;
}

Stroka TError::ToString() const
{
    return Message.empty()
        ? Code.ToString()
        : Message + " (" + Code.ToString() + ")";
}

bool TError::IsOK() const
{
    return Code == EErrorCode::OK;
}

bool TError::IsRpcError() const
{
    return Code < EErrorCode::OK;
}

bool TError::IsServiceError() const
{
    return Code > EErrorCode::OK;
}

////////////////////////////////////////////////////////////////////////////////

TRpcManager::TRpcManager()
{ }

TRpcManager* TRpcManager::Get()
{
    return Singleton<TRpcManager>();
}

Stroka TRpcManager::GetDebugInfo()
{
    // TODO: implement
    return NBus::GetClientDispatcherDebugInfo();
}

void TRpcManager::Shutdown()
{
    NBus::ShutdownClientDispatcher();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT

