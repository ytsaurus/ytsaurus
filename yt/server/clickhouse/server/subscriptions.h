#pragma once

#include "public.h"

#include <yt/ytlib/api/native/public.h>

#include <yt/server/clickhouse/interop/directory.h>

namespace NYT {
namespace NClickHouse {

// TODO: split polling from subscription management

////////////////////////////////////////////////////////////////////////////////

struct ISubscriptionManager
    : public virtual TRefCounted
{
public:
    virtual void Subscribe(
        NApi::NNative::IClientPtr client,
        TString path,
        NInterop::TNodeRevision expectedRevision,
        NInterop::INodeEventHandlerWeakPtr eventHandler) = 0;
};

DEFINE_REFCOUNTED_TYPE(ISubscriptionManager);

////////////////////////////////////////////////////////////////////////////////

ISubscriptionManagerPtr CreateSubscriptionManager();

}   // namespace NClickHouse
}   // namespace NYT
