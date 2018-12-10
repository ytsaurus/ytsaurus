#include "transaction.h"
#include "transaction_impl.h"

namespace NYT {
namespace NApi {
namespace NRpcProxy {

////////////////////////////////////////////////////////////////////////////////

NApi::ITransactionPtr CreateTransaction(
    TConnectionPtr connection,
    TClientPtr client,
    NRpc::IChannelPtr channel,
    const NTransactionClient::TTransactionId& id,
    NTransactionClient::TTimestamp startTimestamp,
    NTransactionClient::ETransactionType type,
    NTransactionClient::EAtomicity atomicity,
    NTransactionClient::EDurability durability,
    TDuration timeout,
    std::optional<TDuration> pingPeriod,
    bool sticky)
{
    return New<TTransaction>(
        std::move(connection),
        std::move(client),
        std::move(channel),
        id,
        startTimestamp,
        type,
        atomicity,
        durability,
        timeout,
        pingPeriod,
        sticky);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpcProxy
} // namespace NApi
} // namespace NYT

