#include "alert_manager.h"
#include "config.h"
#include "private.h"

#include <yt/yt/core/concurrency/periodic_executor.h>

#include <yt/yt/core/ytree/ypath_proxy.h>
#include <yt/yt/core/ytree/ypath_service.h>
#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NAlertManager {

using namespace NConcurrency;
using namespace NLogging;
using namespace NProfiling;
using namespace NThreading;
using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TError TAlert::GetTaggedError() const
{
    std::vector<TErrorAttribute> errorAttributes;
    errorAttributes.reserve(Tags.size());
    for (const auto& tag : Tags) {
        errorAttributes.emplace_back(tag.first, tag.second);
    }

    return Error << errorAttributes;
}

////////////////////////////////////////////////////////////////////////////////

class TAlertManager
    : public IAlertManager
{
public:
    TAlertManager(NLogging::TLogger logger, NProfiling::TProfiler alertProfiler, IInvokerPtr invoker)
        : Logger(std::move(logger))
        , AlertProfiler_(std::move(alertProfiler))
        , Invoker_(std::move(invoker))
        , OrchidService_(IYPathService::FromProducer(
            BIND(&TAlertManager::BuildOrchid, MakeWeak(this)))->Via(Invoker_))
        , DynamicConfig_(New<TAlertManagerDynamicConfig>())
        , AlertCollectionExecutor_(New<TPeriodicExecutor>(
            Invoker_,
            BIND(&TAlertManager::CollectAlerts, MakeWeak(this)),
            DynamicConfig_->AlertCollectionPeriod))
    { }

    IYPathServicePtr GetOrchidService() const override
    {
        return OrchidService_;
    }

    void Start() override
    {
        AlertCollectionExecutor_->Start();
    }

    void Reconfigure(
        const TAlertManagerDynamicConfigPtr& oldConfig,
        const TAlertManagerDynamicConfigPtr& newConfig) override
    {
        {
            auto guard = WriterGuard(SpinLock_);
            DynamicConfig_ = newConfig;
        }

        AlertCollectionExecutor_->SetPeriod(newConfig->AlertCollectionPeriod);

        YT_LOG_DEBUG(
            "Updated alert manager dynamic config (OldConfig: %v, NewConfig: %v)",
            ConvertToYsonString(oldConfig, EYsonFormat::Text),
            ConvertToYsonString(newConfig, EYsonFormat::Text));
    }

    void CollectAlerts()
    {
        std::vector<TAlert> rawAlerts;
        PopulateAlerts_.Fire(&rawAlerts);

        struct TAggregatedAlert
        {
            std::optional<TErrorCode> ErrorCode;
            std::optional<TString> Description;
            std::vector<TError> Errors;
        };
        THashMap<TString, TAggregatedAlert> categoryToAggregatedAlerts;

        THashMap<TString, THashSet<TTagList>> uniqueAlerts;

        for (const auto& rawAlert: rawAlerts) {
            // Each category + tags combination should be unique.
            InsertOrCrash(uniqueAlerts[rawAlert.Category], rawAlert.Tags);

            auto& aggregatedAlert = categoryToAggregatedAlerts[rawAlert.Category];

            // Error codes and descriptions should be equal for alerts in the same category.
            if (!aggregatedAlert.ErrorCode) {
                aggregatedAlert.ErrorCode = rawAlert.ErrorCode;
            } else {
                YT_VERIFY(*aggregatedAlert.ErrorCode == rawAlert.ErrorCode);
            }
            // TODO(achulkov2): Avoid duplicating description strings? Doesn't seem to be worth the hassle for now.
            if (!aggregatedAlert.Description) {
                aggregatedAlert.Description = rawAlert.Description;
            } else {
                YT_VERIFY(*aggregatedAlert.Description == rawAlert.Description);
            }
            aggregatedAlert.Errors.push_back(rawAlert.GetTaggedError());
        }

        THashMap<TString, TError> alerts;
        for (const auto& [category, aggregatedAlert] : categoryToAggregatedAlerts) {
            alerts.emplace(category, TError(*aggregatedAlert.ErrorCode, *aggregatedAlert.Description) << aggregatedAlert.Errors);
        }

        auto guard = WriterGuard(SpinLock_);
        Alerts_ = alerts;

        YT_LOG_DEBUG("Collected alerts (Count: %v)", Alerts_.size());
    }

    THashMap<TString, TError> GetAlerts() const override
    {
        auto guard = ReaderGuard(SpinLock_);

        return Alerts_;
    }

    TLogger GetLogger() const override
    {
        return Logger;
    }

    TProfiler GetAlertProfiler() const override
    {
        return AlertProfiler_;
    }

    void BuildOrchid(NYson::IYsonConsumer* consumer) const
    {
        BuildYsonFluently(consumer)
            .BeginAttributes()
                .Item("opaque").Value(true)
            .EndAttributes()
            .Value(GetAlerts());
    }

    DEFINE_SIGNAL_OVERRIDE(void(std::vector<TAlert>*), PopulateAlerts);

private:
    const NLogging::TLogger Logger;
    const TProfiler AlertProfiler_;
    const IInvokerPtr Invoker_;
    const IYPathServicePtr OrchidService_;
    TAlertManagerDynamicConfigPtr DynamicConfig_;
    const TPeriodicExecutorPtr AlertCollectionExecutor_;

    YT_DECLARE_SPIN_LOCK(TReaderWriterSpinLock, SpinLock_);
    THashMap<TString, TError> Alerts_;
};

DEFINE_REFCOUNTED_TYPE(TAlertManager)

IAlertManagerPtr CreateAlertManager(NLogging::TLogger logger, NProfiling::TProfiler alertProfiler, IInvokerPtr invoker)
{
    return New<TAlertManager>(std::move(logger), std::move(alertProfiler), std::move(invoker));
}

////////////////////////////////////////////////////////////////////////////////

class TAlertCollector
    : public IAlertCollector
{
public:
    TAlertCollector(const IAlertManagerPtr& alertManager)
        : AlertManager_(alertManager)
        , AlertProfiler_(alertManager->GetAlertProfiler())
        , Logger(alertManager->GetLogger())
    {
        alertManager->SubscribePopulateAlerts(PopulateAlertsCallback_);
    }
    ~TAlertCollector()
    {
        if (auto alertManager = AlertManager_.Lock()) {
            alertManager->UnsubscribePopulateAlerts(PopulateAlertsCallback_);
        }
    }


    void StageAlert(TAlert alert) override
    {
        auto guard = WriterGuard(SpinLock_);

        EmplaceOrCrash(StagedAlerts_[alert.Category], alert.Tags, alert);
        CategoryToGauges_[alert.Category].try_emplace(
            alert.Tags,
            AlertProfiler_.WithTag("category", alert.Category).WithTags(TTagSet{alert.Tags}).Gauge("/alerts"));

        YT_LOG_DEBUG(alert.Error, "Staged alert (Category: %v, Description: %v, Tags: %v)", alert.Category, alert.Description, alert.Tags);
    }

    void PublishAlerts() override
    {
        auto guard = WriterGuard(SpinLock_);

        for (const auto& [category, tagsToGauges] : CategoryToGauges_) {
            auto tagsToAlertsIt = StagedAlerts_.find(category);
            for (const auto& [tags, gauge] : tagsToGauges) {
                if (tagsToAlertsIt != StagedAlerts_.end() && tagsToAlertsIt->second.contains(tags)) {
                    gauge.Update(1);
                } else {
                    gauge.Update(0);
                }
            }
        }

        Alerts_.clear();
        for (const auto& [category, tagsToAlerts] : StagedAlerts_) {
            auto alerts = GetValues(tagsToAlerts);
            Alerts_.insert(Alerts_.end(), std::make_move_iterator(alerts.begin()), std::make_move_iterator(alerts.end()));
        }

        YT_LOG_DEBUG("Publishing staged alerts (Count: %v)", StagedAlerts_.size());

        StagedAlerts_.clear();
    }

private:
    const TCallback<void(std::vector<TAlert>*)> PopulateAlertsCallback_ =
        BIND(&TAlertCollector::DoPopulateAlerts, MakeWeak(this));
    const TWeakPtr<IAlertManager> AlertManager_;
    const NProfiling::TProfiler AlertProfiler_;
    const NLogging::TLogger Logger;

    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, SpinLock_);
    std::vector<TAlert> Alerts_;
    THashMap<TString, THashMap<NProfiling::TTagList, TAlert>> StagedAlerts_;
    THashMap<TString, THashMap<NProfiling::TTagList, NProfiling::TGauge>> CategoryToGauges_;

    void DoPopulateAlerts(std::vector<TAlert>* alerts)
    {
        auto guard = ReaderGuard(SpinLock_);

        alerts->insert(alerts->end(), Alerts_.begin(), Alerts_.end());
    }
};

DEFINE_REFCOUNTED_TYPE(TAlertCollector)

IAlertCollectorPtr CreateAlertCollector(const IAlertManagerPtr& alertManager)
{
    return New<TAlertCollector>(alertManager);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NAlertManager
