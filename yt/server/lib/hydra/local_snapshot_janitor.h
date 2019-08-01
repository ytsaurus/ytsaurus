#pragma once

#include "public.h"

#include <yt/core/actions/public.h>

namespace NYT::NHydra {

////////////////////////////////////////////////////////////////////////////////

struct ILocalSnapshotJanitor
    : public TRefCounted
{
    virtual void Start() = 0;
};

DEFINE_REFCOUNTED_TYPE(ILocalSnapshotJanitor)

////////////////////////////////////////////////////////////////////////////////

ILocalSnapshotJanitorPtr CreateLocalSnapshotJanitor(
    TLocalSnapshotJanitorConfigPtr config,
    IInvokerPtr invoker);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra
