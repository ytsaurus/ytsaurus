#pragma once

#include "public.h"

#include <yt/core/actions/public.h>

namespace NYT::NHydra {

////////////////////////////////////////////////////////////////////////////////

class TLocalHydraJanitor
    : public TRefCounted
{
public:
    TLocalHydraJanitor(
        TString snapshotPath,
        TString changelogPath,
        TLocalHydraJanitorConfigPtr config,
        IInvokerPtr invoker);

    ~TLocalHydraJanitor();

    void Start();

private:
    class TImpl;
    const TIntrusivePtr<TImpl> Impl_;
};

DEFINE_REFCOUNTED_TYPE(TLocalHydraJanitor)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra
