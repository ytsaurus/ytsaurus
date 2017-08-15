#pragma once

#include "rpc_proxy_connection.h"

#include <yt/ytlib/transaction_client/timestamp_provider_base.h>

namespace NYT {
namespace NRpcProxy {

////////////////////////////////////////////////////////////////////////////////

class TRpcProxyTimestampProvider
    : public NTransactionClient::TTimestampProviderBase
{
public:
    TRpcProxyTimestampProvider(
        TWeakPtr<TRpcProxyConnection> connection,
        TDuration defaultTimeout);
    ~TRpcProxyTimestampProvider();

private:
    TWeakPtr<TRpcProxyConnection> Connection_;
    TDuration DefaultTimeout_;

    virtual TFuture<NTransactionClient::TTimestamp> DoGenerateTimestamps(int count) override;
};

DEFINE_REFCOUNTED_TYPE(TRpcProxyTimestampProvider)

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpcProxy
} // namespace NYT
