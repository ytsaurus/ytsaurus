#pragma once

#include "public.h"

#include <yt/yt/core/actions/signal.h>

namespace NYT::NCellMaster {

////////////////////////////////////////////////////////////////////////////////

using TAlertSource = std::function<std::vector<TError>()>;

////////////////////////////////////////////////////////////////////////////////

struct IAlertManager
    : public virtual TRefCounted
{
    virtual void Initialize() = 0;

    virtual void RegisterAlertSource(TAlertSource alertSource) = 0;

    virtual std::vector<TError> GetAlerts() const = 0;
};

DEFINE_REFCOUNTED_TYPE(IAlertManager)

////////////////////////////////////////////////////////////////////////////////

IAlertManagerPtr CreateAlertManager(TBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellMaster
