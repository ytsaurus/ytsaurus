#include "connection.h"
#include "connection_impl.h"

namespace NYT::NApi::NRpcProxy {

////////////////////////////////////////////////////////////////////////////////

NApi::IConnectionPtr CreateConnection(TConnectionConfigPtr config)
{
    return New<TConnection>(std::move(config));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NRpcProxy

