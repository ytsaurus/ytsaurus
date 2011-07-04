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

typedef TGUID TSessionId;
typedef i64 TSequenceId;

////////////////////////////////////////////////////////////////////////////////

class TRpcManager
    : private TNonCopyable
{
public:
    TRpcManager();

    static TRpcManager* Get();
    NLog::TLogger& GetLogger();
    Stroka GetDebugInfo();

private:
    NLog::TLogger Logger;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT

