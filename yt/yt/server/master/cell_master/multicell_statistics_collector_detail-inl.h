#ifndef MULTICELL_STATISTICS_COLLECTOR_DETAIL_INL_H_
#error "Direct inclusion of this file is not allowed, include multicell_statistics_collector_detail.h"
// For the sake of sane code completion.
#include "multicell_statistics_collector_detail.h"
#endif

#include "hydra_facade.h"
#include "multicell_statistics_collector.h"
#include "private.h"

namespace NYT::NCellMaster {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

template <CMulticellStatisticsValue... TMulticellStatisticsValues>
TMulticellStatisticsCollectorCommon<TMulticellStatisticsValues...>::TMulticellStatisticsCollectorCommon(TBootstrap* bootstrap)
    : TMasterAutomatonPart(bootstrap, EAutomatonThreadQueue::MulticellGossip)
    , Values_(MakeNodes(bootstrap))
{ }

template <CMulticellStatisticsValue... TMulticellStatisticsValues>
template <CMulticellStatisticsValue TMulticellStatisticsValue>
TMulticellStatisticsValue& TMulticellStatisticsCollectorCommon<TMulticellStatisticsValues...>::GetValue()
{
    return std::get<TMulticellStatisticsValue>(Values_);
}

template <CMulticellStatisticsValue... TMulticellStatisticsValues>
auto TMulticellStatisticsCollectorCommon<TMulticellStatisticsValues...>::MakeNodes(TBootstrap* bootstrap) -> TValues
{
    auto impl = [bootstrap] <std::size_t... Idxs> (std::index_sequence<Idxs...>) {
        return std::tuple(std::tuple_element_t<Idxs, TValues>(bootstrap)...);
    };
    return impl(ValuesIndex);
}

template <CMulticellStatisticsValue... TMulticellStatisticsValues>
template <CMulticellStatisticsValue TMulticellStatisticsValue>
void TMulticellStatisticsCollectorCommon<TMulticellStatisticsValues...>::RegisterMutationHandler(TMulticellStatisticsValue* value)
{
    auto impl = [&] <template <class...> class TList, class... TReqs> (TList<TReqs...>*) {
        (RegisterMethod(BIND_NO_PROPAGATE(
            static_cast<void(TMulticellStatisticsValue::*)(TReqs*)>(
                &TMulticellStatisticsValue::HydraApplyMulticellStatisticsUpdate),
            value)),
        ...);
    };
    impl(static_cast<typename TMulticellStatisticsValue::TMutationRequestTypes*>(nullptr));
}

template <CMulticellStatisticsValue... TMulticellStatisticsValues>
void TMulticellStatisticsCollectorCommon<TMulticellStatisticsValues...>::RegisterMutationHandlers()
{
    std::apply([&] (auto&... values) {
        (RegisterMutationHandler(&values), ...);
    }, Values_);
}

template <CMulticellStatisticsValue... TMulticellStatisticsValues>
template <CMulticellStatisticsValue TMulticellStatisticsValue>
TPeriodicExecutorPtr TMulticellStatisticsCollectorCommon<TMulticellStatisticsValues...>::MakeUpdatePeriodic(
    TMulticellStatisticsValue* node)
{
    return New<TPeriodicExecutor>(
        Bootstrap_->GetHydraFacade()->GetEpochAutomatonInvoker(EAutomatonThreadQueue::Periodic),
        BIND(&TMulticellStatisticsCollectorCommon::OnPushUpdates<TMulticellStatisticsValue>, Unretained(this), node),
        node->GetUpdatePeriod());
}

template <CMulticellStatisticsValue... TMulticellStatisticsValues>
template <CMulticellStatisticsValue TMulticellStatisticsValue>
void TMulticellStatisticsCollectorCommon<TMulticellStatisticsValues...>::OnPushUpdates(TMulticellStatisticsValue* node)
{
    const auto& multicellManager = Bootstrap_->GetMulticellManager();
    if (!multicellManager->IsLocalMasterCellRegistered()) {
        return;
    }

    auto requestOrError = WaitFor(node->GetLocalCellUpdate());
    if (!requestOrError.IsOK()) {
        NDetail::LogGetLocalCellUpdateError(requestOrError);
        return;
    }

    auto request = requestOrError.Value();

    if (multicellManager->IsPrimaryMaster()) {
        YT_UNUSED_FUTURE(CreateMutation(Bootstrap_->GetHydraFacade()->GetHydraManager(), request)
            ->CommitAndLog(CellMasterLogger()));
        node->FinishUpdate();
    } else {
        multicellManager->PostToPrimaryMaster(request, false);
    }
}

template <CMulticellStatisticsValue... TMulticellStatisticsValues>
void TMulticellStatisticsCollectorCommon<TMulticellStatisticsValues...>::SaveValues(TSaveContext& context)
{
    std::apply([&context] (auto&... values) {
        (values.Save(context), ...);
    }, Values_);
}

template <CMulticellStatisticsValue... TMulticellStatisticsValues>
void TMulticellStatisticsCollectorCommon<TMulticellStatisticsValues...>::LoadValues(TLoadContext& context)
{
    std::apply([&context] (auto&... values) {
        (values.Load(context), ...);
    }, Values_);
}

template <CMulticellStatisticsValue... TMulticellStatisticsValues>
void TMulticellStatisticsCollectorCommon<TMulticellStatisticsValues...>::OnDynamicConfigChanged(TDynamicClusterConfigPtr)
{
    auto updatePeriod = [this] <std::size_t Idx> () {
        if (const auto& periodic = std::get<Idx>(Periodics_)) {
            periodic->SetPeriod(std::get<Idx>(Values_).GetUpdatePeriod());
        }
    };

    auto impl = [&] <std::size_t... Idxs> (std::index_sequence<Idxs...>) {
        (updatePeriod.template operator()<Idxs>(), ...);
    };
    impl(ValuesIndex);
}

template <CMulticellStatisticsValue... TMulticellStatisticsValues>
void TMulticellStatisticsCollectorCommon<TMulticellStatisticsValues...>::OnLeaderActive()
{
    TMasterAutomatonPart::OnLeaderActive();

    auto impl = [this] <std::size_t... Idxs> (std::index_sequence<Idxs...>) {
        ((std::get<Idxs>(Periodics_) = MakeUpdatePeriodic(&std::get<Idxs>(Values_))), ...);
        (std::get<Idxs>(Periodics_)->Start(), ...);
    };
    impl(ValuesIndex);
}

template <CMulticellStatisticsValue... TMulticellStatisticsValues>
void TMulticellStatisticsCollectorCommon<TMulticellStatisticsValues...>::OnStopLeading()
{
    TMasterAutomatonPart::OnStopLeading();

    auto stopPeriodic = [] (TPeriodicExecutorPtr& periodic) {
        if (periodic) {
            YT_UNUSED_FUTURE(periodic->Stop());
            periodic.Reset();
        }
    };

    std::apply([&stopPeriodic] (auto&... periodics) {
        (stopPeriodic(periodics), ...);
    }, Periodics_);
}

template <CMulticellStatisticsValue... TMulticellStatisticsValues>
void TMulticellStatisticsCollectorCommon<TMulticellStatisticsValues...>::Clear()
{
    TMasterAutomatonPart::Clear();

    std::apply([] (auto&... values) {
        (values.Clear(), ...);
    }, Values_);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellMaster
