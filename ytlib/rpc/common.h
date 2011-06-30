#pragma once

#include "../misc/common.h"
#include "../misc/ptr.h"

#include "../actions/invoker.h"
#include "../actions/action.h"
#include "../actions/async_result.h"
#include "../actions/action_util.h"

#include "../logging/log.h"

namespace NYT {
namespace NRpc {

////////////////////////////////////////////////////////////////////////////////

// TODO: move impl to cpp
class TRpcManager
    : private TNonCopyable
{
public:
    TRpcManager()
        : Logger("Rpc")
    { }

    static TRpcManager* Get()
    {
        return Singleton<TRpcManager>();
    }

    NLog::TLogger& GetLogger()
    {
        return Logger;
    }

    Stroka GetDebugInfo()
    {
        // TODO: implement
        return "";
    }

private:
    NLog::TLogger Logger;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT

