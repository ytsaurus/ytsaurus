#include "connection.h"
#include "connection_impl.h"

namespace NYT {
namespace NApi {
namespace NRpcProxy {

////////////////////////////////////////////////////////////////////////////////

NApi::IConnectionPtr CreateConnection(TConnectionConfigPtr config)
{
    return New<TConnection>(std::move(config));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpcProxy
} // namespace NApi
} // namespace NYT

