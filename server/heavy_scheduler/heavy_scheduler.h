#pragma once

#include "public.h"

namespace NYP::NServer::NHeavyScheduler {

////////////////////////////////////////////////////////////////////////////////

class THeavyScheduler
    : public TRefCounted
{
public:
    THeavyScheduler(TBootstrap* bootstrap, THeavySchedulerConfigPtr config);

    void Initialize();

private:
    class TImpl;
    const TIntrusivePtr<TImpl> Impl_;
};

DEFINE_REFCOUNTED_TYPE(THeavyScheduler);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NHeavyScheduler
