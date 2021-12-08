#pragma once

#include <yt/yt/server/lib/hydra_common/public.h>

#include <yt/yt/core/actions/public.h>

namespace NYT::NHydra2 {

////////////////////////////////////////////////////////////////////////////////

class TLocalHydraJanitor
    : public TRefCounted
{
public:
    TLocalHydraJanitor(
        TString snapshotPath,
        TString changelogPath,
        NHydra::TLocalHydraJanitorConfigPtr config,
        IInvokerPtr invoker);

    ~TLocalHydraJanitor();

    void Start();

private:
    class TImpl;
    const TIntrusivePtr<TImpl> Impl_;
};

DEFINE_REFCOUNTED_TYPE(TLocalHydraJanitor)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra2
