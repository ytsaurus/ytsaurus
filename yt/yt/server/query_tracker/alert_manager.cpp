#include "alert_manager.h"

#include "config.h"

#include <yt/yt/core/concurrency/periodic_executor.h>

#include <yt/yt/core/ytree/ypath_proxy.h>
#include <yt/yt/core/ytree/ypath_service.h>
#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NQueryTracker {

using namespace NConcurrency;
using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = AlertManagerLogger;

////////////////////////////////////////////////////////////////////////////////

TAlertManager::TAlertManager(IInvokerPtr controlInvoker)
    : DynamicConfig_(New<TAlertManagerDynamicConfig>())
    , ControlInvoker_(std::move(controlInvoker))
    , OrchidService_(IYPathService::FromProducer(BIND(&TAlertManager::BuildOrchid, MakeWeak(this)))->Via(ControlInvoker_))
    , AlertCollectionExecutor_(New<TPeriodicExecutor>(
        ControlInvoker_,
        BIND(&TAlertManager::CollectAlerts, MakeWeak(this)),
        DynamicConfig_->AlertCollectionPeriod))
{ }

NYTree::IYPathServicePtr TAlertManager::GetOrchidService() const
{
    return OrchidService_;
}

void TAlertManager::Start()
{
    AlertCollectionExecutor_->Start();
}

void TAlertManager::CollectAlerts()
{
    VERIFY_SERIALIZED_INVOKER_AFFINITY(ControlInvoker_);

    std::vector<TError> alerts;
    PopulateAlerts_.Fire(&alerts);
    Alerts_.swap(alerts);

    static const auto possibleAlertErrorCodes = TEnumTraits<NAlerts::EErrorCode>::GetDomainValues();
    THashSet<NAlerts::EErrorCode> encounteredCodes;

    for (const auto& alert : Alerts_) {
        auto code = static_cast<int>(alert.GetCode());
        auto category = static_cast<NAlerts::EErrorCode>(code);
        YT_VERIFY(std::find(possibleAlertErrorCodes.begin(), possibleAlertErrorCodes.end(), category) != possibleAlertErrorCodes.end());
        InsertOrCrash(encounteredCodes, category);

        YT_LOG_WARNING(alert);
    }

    YT_LOG_DEBUG("Collected alerts (Count: %v)", Alerts_.size());
}

void TAlertManager::OnDynamicConfigChanged(const TAlertManagerDynamicConfigPtr& newConfig)
{
    VERIFY_SERIALIZED_INVOKER_AFFINITY(ControlInvoker_);

    DynamicConfig_ = newConfig;

    AlertCollectionExecutor_->SetPeriod(newConfig->AlertCollectionPeriod);
}

void TAlertManager::BuildOrchid(NYson::IYsonConsumer* consumer) const
{
    VERIFY_SERIALIZED_INVOKER_AFFINITY(ControlInvoker_);

    BuildYsonFluently(consumer)
        .BeginAttributes()
            .Item("opaque").Value(true)
        .EndAttributes()
        .DoMapFor(Alerts_, [] (TFluentMap fluent, const auto& alert) {
            auto category = static_cast<NAlerts::EErrorCode>(static_cast<int>(alert.GetCode()));
            fluent
                .Item(FormatEnum(category)).Value(alert);
        });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryTracker
