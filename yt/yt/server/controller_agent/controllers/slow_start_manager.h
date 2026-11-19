#pragma once

#include <yt/yt/ytlib/controller_agent/persistence.h>

#include <yt/yt/ytlib/scheduler/public.h>

#include <yt/yt/core/ytree/fluent.h>

#include <memory>

namespace NYT::NControllerAgent::NControllers {

////////////////////////////////////////////////////////////////////////////////

struct ISlowStartManager
    : public IPersistent
{
    virtual void OnJobRegistered() = 0;
    virtual void OnJobUnregistered(bool isCompleted) = 0;

    virtual i64 GetRemainingAvailableJobCount() const = 0;

    virtual void BuildInfo(NYTree::TFluentMap fluent) const = 0;
};

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<ISlowStartManager> CreateSlowStartManager(NScheduler::TSlowStartConfigPtr config);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent::NControllers
