#pragma once

#include "public.h"

namespace NYP::NServer::NScheduler {

////////////////////////////////////////////////////////////////////////////////

class TScheduler
    : public TRefCounted
{
public:
    TScheduler(
        NServer::NMaster::TBootstrap* bootstrap,
        TSchedulerConfigPtr config);

    void Initialize();

    //! Updates scheduler configuration.
    //! Returns a future that becomes set when configuration is fully updated.
    TFuture<void> UpdateConfig(TSchedulerConfigPtr config);

private:
    class TImpl;
    using TImplPtr = NYT::TIntrusivePtr<TImpl>;
    const TImplPtr Impl_;
};

DEFINE_REFCOUNTED_TYPE(TScheduler)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NScheduler
