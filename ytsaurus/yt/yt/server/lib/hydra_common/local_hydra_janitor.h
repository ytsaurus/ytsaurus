#pragma once

#include "public.h"

#include <yt/yt/core/actions/public.h>

namespace NYT::NHydra {

////////////////////////////////////////////////////////////////////////////////

struct ILocalHydraJanitor
    : public TRefCounted
{
    virtual void Start() = 0;
};

DEFINE_REFCOUNTED_TYPE(ILocalHydraJanitor)

ILocalHydraJanitorPtr CreateLocalHydraJanitor(
    TString snapshotPath,
    TString changelogPath,
    TLocalHydraJanitorConfigPtr config,
    IInvokerPtr ioInvoker);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra
