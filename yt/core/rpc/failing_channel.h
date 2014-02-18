#pragma once

#include "public.h"

namespace NYT {
namespace NRpc {

////////////////////////////////////////////////////////////////////////////////

struct TFailureModel
    :  public virtual TRefCounted
{
    TFailureModel();

    std::atomic<bool> ShouldRequestFail;
    std::atomic<bool> ShouldResponseFail;
};

typedef TIntrusivePtr<TFailureModel> TFailureModelPtr;


////////////////////////////////////////////////////////////////////////////////

IChannelPtr CreateFailingChannel(
    IChannelPtr underlyingChannel,
    TFailureModelPtr failureModel);

} // namespace NRpc
} // namespae NYT
