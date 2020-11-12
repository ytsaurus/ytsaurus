#include "noop_timestamp_provider.h"

#include "private.h"
#include "timestamp_provider.h"

namespace NYT::NTransactionClient {

///////////////////////////////////////////////////////////////////////////////

class TNoopTimestampProvider
    : public ITimestampProvider
{
public:
    virtual TFuture<TTimestamp> GenerateTimestamps(int /*count*/) override
    {
        return MakeFuture(NullTimestamp);
    }

    virtual TTimestamp GetLatestTimestamp() override
    {
        return NullTimestamp;
    }
};

////////////////////////////////////////////////////////////////////////////////

ITimestampProviderPtr CreateNoopTimestampProvider()
{
    return New<TNoopTimestampProvider>();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNTransactionClient
