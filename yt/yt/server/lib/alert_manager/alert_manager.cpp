#include "alert_manager.h"
#include "config.h"
#include "private.h"

#include <yt/yt/core/concurrency/periodic_executor.h>

#include <yt/yt/core/ytree/ypath_proxy.h>
#include <yt/yt/core/ytree/ypath_service.h>
#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NAlertManager {

using namespace NConcurrency;
using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = AlertManagerLogger;

////////////////////////////////////////////////////////////////////////////////

class TAlertManager
    : public IAlertManager
{
public:
    explicit TAlertManager(IInvokerPtr controlInvoker)
        : ControlInvoker_(std::move(controlInvoker))
        , OrchidService_(IYPathService::FromProducer(
            BIND(&TAlertManager::BuildOrchid, MakeWeak(this)))->Via(ControlInvoker_))
        , DynamicConfig_(New<TAlertManagerDynamicConfig>())
        , AlertCollectionExecutor_(New<TPeriodicExecutor>(
            ControlInvoker_,
            BIND(&TAlertManager::CollectAlerts, MakeWeak(this)),
            DynamicConfig_->AlertCollectionPeriod))
    { }

    NYTree::IYPathServicePtr GetOrchidService() const
    {
        return OrchidService_;
    }

    void Start()
    {
        AlertCollectionExecutor_->Start();
    }

    void Reconfigure(
        const TAlertManagerDynamicConfigPtr& oldConfig,
        const TAlertManagerDynamicConfigPtr& newConfig)
    {
        VERIFY_SERIALIZED_INVOKER_AFFINITY(ControlInvoker_);

        DynamicConfig_ = newConfig;

        AlertCollectionExecutor_->SetPeriod(newConfig->AlertCollectionPeriod);

        YT_LOG_DEBUG(
            "Updated alert manager dynamic config (OldConfig: %v, NewConfig: %v)",
            ConvertToYsonString(oldConfig, EYsonFormat::Text),
            ConvertToYsonString(newConfig, EYsonFormat::Text));
    }

private:
    const IInvokerPtr ControlInvoker_;
    const NYTree::IYPathServicePtr OrchidService_;

    TAlertManagerDynamicConfigPtr DynamicConfig_;
    NConcurrency::TPeriodicExecutorPtr AlertCollectionExecutor_;

    std::vector<TAlert> Alerts_;

    void CollectAlerts()
    {
        VERIFY_SERIALIZED_INVOKER_AFFINITY(ControlInvoker_);

        std::vector<TAlert> alerts;
        PopulateAlerts_.Fire(&alerts);
        Alerts_.swap(alerts);

        THashSet<TString> encounteredCategories;

        for (const auto& alert : Alerts_) {
            InsertOrCrash(encounteredCategories, alert.Category);

            YT_LOG_WARNING(alert.Error);
        }

        YT_LOG_DEBUG("Collected alerts (Count: %v)", Alerts_.size());
    }

    void BuildOrchid(NYson::IYsonConsumer* consumer) const
    {
        VERIFY_SERIALIZED_INVOKER_AFFINITY(ControlInvoker_);

        BuildYsonFluently(consumer)
            .BeginAttributes()
                .Item("opaque").Value(true)
            .EndAttributes()
            .DoMapFor(Alerts_, [] (TFluentMap fluent, const auto& alert) {
                fluent
                    .Item(alert.Category).Value(alert.Error);
            });
    }
};

DEFINE_REFCOUNTED_TYPE(TAlertManager)

////////////////////////////////////////////////////////////////////////////////

IAlertManagerPtr CreateAlertManager(IInvokerPtr controlInvoker)
{
    return New<TAlertManager>(std::move(controlInvoker));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NAlertManager
