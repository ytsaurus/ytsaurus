#pragma once

#include "common.h"

namespace NYT {
namespace NRpc {

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
