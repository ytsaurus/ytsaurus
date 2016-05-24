#include "local_server.h"
#include "server_detail.h"

#include <yt/core/misc/common.h>

namespace NYT {
namespace NRpc {

////////////////////////////////////////////////////////////////////////////////

class TLocalServer
    : public TServerBase
{ };

IServerPtr CreateLocalServer()
{
    return New<TLocalServer>();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT
