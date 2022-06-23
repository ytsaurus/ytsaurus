#include "public.h"

#include <yt/yt/core/actions/public.h>

#include <yt/yt/core/concurrency/public.h>

#include <yt/yt/core/net/public.h>

#include <library/cpp/yt/memory/ref.h>

namespace NYT::NZookeeper {

////////////////////////////////////////////////////////////////////////////////

using TRequestHandler = TCallback<TSharedRef(TSharedRef)>;
using TFailHandler = TCallback<void(TError)>;

struct IConnection
    : public virtual TRefCounted
{
    virtual bool Start() = 0;
    virtual TFuture<void> Stop() = 0;
};

DEFINE_REFCOUNTED_TYPE(IConnection)

////////////////////////////////////////////////////////////////////////////////

IConnectionPtr CreateConnection(
    NNet::IConnectionPtr connection,
    NConcurrency::IPollerPtr poller,
    IInvokerPtr invoker,
    TRequestHandler requestHandler,
    TFailHandler failHandler,
    TString loggingTag);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NZookeeper
