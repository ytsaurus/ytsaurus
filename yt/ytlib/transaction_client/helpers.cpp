#include "stdafx.h"
#include "helpers.h"
#include "transaction_manager.h"

#include <core/rpc/client.h>

#include <ytlib/cypress_client/rpc_helpers.h>

namespace NYT {
namespace NTransactionClient {

using namespace NRpc;

////////////////////////////////////////////////////////////////////////////////

void SetTransactionId(IClientRequestPtr request, TTransactionPtr transaction)
{
    NCypressClient::SetTransactionId(
        request,
        transaction ? transaction->GetId() : NullTransactionId);
}

std::pair<TInstant, TInstant> TimestampToInstant(TTimestamp timestamp)
{
    auto lo = TInstant::Seconds((timestamp & TimestampValueMask) >> TimestampCounterWidth);
    auto hi = lo + TDuration::Seconds(1);
    return std::make_pair(lo, hi);
}

std::pair<TTimestamp, TTimestamp> InstantToTimestamp(TInstant instant)
{
    auto lo = instant.Seconds() << TimestampCounterWidth;
    auto hi = lo + (1 << TimestampCounterWidth);
    return std::make_pair(lo, hi);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTransactionClient
} // namespace NYT

