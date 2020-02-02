#pragma once

#include "public.h"

#include <yt/core/profiling/public.h>

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

//! Returns the current fiber id.
TFiberId GetCurrentFiberId();

//! Sets the current fiber id.
void SetCurrentFiberId(TFiberId id);

////////////////////////////////////////////////////////////////////////////////

//! Thrown when a fiber is being terminated by an external event.
class TFiberCanceledException
{ };

////////////////////////////////////////////////////////////////////////////////

class TContextSwitchGuard
{
public:
    TContextSwitchGuard(std::function<void()> out, std::function<void()> in);
    TContextSwitchGuard(const TContextSwitchGuard& other) = delete;
    ~TContextSwitchGuard();
};

class TOneShotContextSwitchGuard
    : public TContextSwitchGuard
{
public:
    explicit TOneShotContextSwitchGuard(std::function<void()> handler);

private:
    bool Active_;

};

class TForbidContextSwitchGuard
    : public TOneShotContextSwitchGuard
{
public:
    TForbidContextSwitchGuard();
};

////////////////////////////////////////////////////////////////////////////////

void PushContextHandler(std::function<void()> out, std::function<void()> in);
void PopContextHandler();

//! Returns the duration the fiber is running.
//! This counts CPU wall time but excludes periods the fiber was sleeping.
NProfiling::TCpuDuration GetCurrentFiberRunCpuTime();

//! Returns |true| if there is enough remaining stack space.
bool CheckFreeStackSpace(size_t space);

////////////////////////////////////////////////////////////////////////////////

} //namespace NYT::NConcurrency
