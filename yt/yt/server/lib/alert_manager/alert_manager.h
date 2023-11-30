#pragma once

#include "public.h"

#include <yt/yt/core/actions/signal.h>

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NAlertManager {

////////////////////////////////////////////////////////////////////////////////

struct TAlert
{
    TString Category;
    TError Error;
};

////////////////////////////////////////////////////////////////////////////////

struct IAlertManager
    : public TRefCounted
{
    virtual NYTree::IYPathServicePtr GetOrchidService() const = 0;

    virtual void Start() = 0;

    virtual void Reconfigure(
        const TAlertManagerDynamicConfigPtr& oldConfig,
        const TAlertManagerDynamicConfigPtr& newConfig) = 0;

    DEFINE_SIGNAL(void(std::vector<TAlert>*), PopulateAlerts);
};

DEFINE_REFCOUNTED_TYPE(IAlertManager)

IAlertManagerPtr CreateAlertManager(IInvokerPtr controlInvoker);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NAlertManager
