#pragma once

#include "public.h"

#include <yt/core/actions/signal.h>

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

    //! Returns the cluster state. Note that this state is being updated
    //! from a dedicated scheduler thread.
    const TClusterPtr& GetCluster() const;

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
