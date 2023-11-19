#pragma once

#include "config.h"

#include <yt/yt/core/ytree/public.h>

#include <yt/yt/core/actions/public.h>
#include <yt/yt/core/actions/signal.h>

namespace NYT::NQueryTracker {

////////////////////////////////////////////////////////////////////////////////

class TAlertManager
    : public TRefCounted
{
public:
    DEFINE_SIGNAL(void(std::vector<TError>*), PopulateAlerts);

public:
    TAlertManager(IInvokerPtr controlInvoker);

    NYTree::IYPathServicePtr GetOrchidService() const;

    void Start();

    void OnDynamicConfigChanged(const TAlertManagerDynamicConfigPtr& newConfig);

private:
    TAlertManagerDynamicConfigPtr DynamicConfig_;
    const IInvokerPtr ControlInvoker_;
    const NYTree::IYPathServicePtr OrchidService_;
    const NConcurrency::TPeriodicExecutorPtr AlertCollectionExecutor_;

    std::vector<TError> Alerts_;

    void CollectAlerts();

    void BuildOrchid(NYson::IYsonConsumer* consumer) const;
};

DEFINE_REFCOUNTED_TYPE(TAlertManager)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryTracker
