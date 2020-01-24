#pragma once

#include "public.h"

#include <yt/core/logging/log.h>

namespace NYP::NServer::NHeavyScheduler {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(ETaskState,
    (Active)
    (Succeeded)
    (Failed)
);

struct ITask
    : public virtual TRefCounted
{
    virtual TGuid GetId() const = 0;

    virtual TInstant GetStartTime() const = 0;

    virtual ETaskState GetState() const = 0;

    virtual std::vector<TObjectId> GetInvolvedPodIds() const = 0;

    virtual void ReconcileState(const NCluster::TClusterPtr& cluster) = 0;
};

DEFINE_REFCOUNTED_TYPE(ITask);

////////////////////////////////////////////////////////////////////////////////

class TTaskBase
    : public virtual ITask
{
public:
    TTaskBase(TGuid id, TInstant startTime);

    virtual TGuid GetId() const override;

    virtual TInstant GetStartTime() const override;

    virtual ETaskState GetState() const override;

protected:
    const NYT::NLogging::TLogger Logger;
    ETaskState State_;

private:
    const TGuid Id_;
    const TInstant StartTime_;
};

DEFINE_REFCOUNTED_TYPE(TTaskBase);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NHeavyScheduler
