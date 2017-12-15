#include "connection.h"
#include "connection_impl.h"

namespace NYT {
namespace NRpcProxy {

////////////////////////////////////////////////////////////////////////////////

NApi::IProxyConnectionPtr CreateRpcProxyConnection(TConnectionConfigPtr config)
{
    return New<TConnection>(std::move(config));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpcProxy
} // namespace NYT

