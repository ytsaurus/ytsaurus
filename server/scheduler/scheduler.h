#pragma once

#include "public.h"

namespace NYP {
namespace NServer {
namespace NScheduler {

////////////////////////////////////////////////////////////////////////////////

class TScheduler
    : public TRefCounted
{
public:
    TScheduler(
        NServer::NMaster::TBootstrap* bootstrap,
        TSchedulerConfigPtr config);

    void Initialize();

private:
    class TImpl;
    using TImplPtr = NYT::TIntrusivePtr<TImpl>;
    const TImplPtr Impl_;
};

DEFINE_REFCOUNTED_TYPE(TScheduler)

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NServer
} // namespace NYP
