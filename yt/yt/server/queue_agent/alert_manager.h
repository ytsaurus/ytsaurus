#pragma once

#include "private.h"
#include "config.h"

#include <yt/yt/core/ytree/public.h>

#include <yt/yt/core/actions/public.h>
#include <yt/yt/core/actions/signal.h>

namespace NYT::NQueueAgent {

////////////////////////////////////////////////////////////////////////////////

class TAlertManager
    : public TRefCounted
{
public:
    explicit TAlertManager(IInvokerPtr controlInvoker);

    NYTree::IYPathServicePtr GetOrchidService() const;

    void Start();

    DEFINE_SIGNAL(void(std::vector<TError>*), PopulateAlerts);

    void OnDynamicConfigChanged(
        const TAlertManagerDynamicConfigPtr& oldConfig,
        const TAlertManagerDynamicConfigPtr& newConfig);

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

} // namespace NYT::NQueueAgent
