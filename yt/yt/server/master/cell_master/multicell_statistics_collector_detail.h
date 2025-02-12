#include "multicell_statistics_collector.h"

#include "automaton.h"
#include "bootstrap.h"
#include "serialize.h"

#include <yt/yt/server/lib/hydra/mutation.h>

#include <yt/yt/core/concurrency/periodic_executor.h>

namespace NYT::NCellMaster {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

template <CMulticellStatisticsValue... TMulticellStatisticsValues>
class TMulticellStatisticsCollectorCommon
    : public IMulticellStatisticsCollector
    , public TMasterAutomatonPart
{
protected:
    using TValues = std::tuple<TMulticellStatisticsValues...>;
    using TPeriodics = std::array<TPeriodicExecutorPtr, sizeof...(TMulticellStatisticsValues)>;

    static constexpr std::index_sequence_for<TMulticellStatisticsValues...> ValuesIndex;

    TValues Values_;
    TPeriodics Periodics_;

    // Helpers.
    static TValues MakeNodes(TBootstrap* bootstrap);

    template <CMulticellStatisticsValue TMulticellStatisticsValue>
    void RegisterMutationHandler(TMulticellStatisticsValue* value);

    void RegisterMutationHandlers();

    template <CMulticellStatisticsValue TMulticellStatisticsValue>
    TPeriodicExecutorPtr MakeUpdatePeriodic(TMulticellStatisticsValue* node);

    // Implementation.
    explicit TMulticellStatisticsCollectorCommon(TBootstrap* bootstrap);

    template <CMulticellStatisticsValue TMulticellStatisticsValue>
    TMulticellStatisticsValue& GetValue();

    template <CMulticellStatisticsValue TMulticellStatisticsValue>
    void OnPushUpdates(TMulticellStatisticsValue* node);

    void SaveValues(TSaveContext& context);

    void LoadValues(TLoadContext& context);

    void OnDynamicConfigChanged(TDynamicClusterConfigPtr);

    void OnLeaderActive() override;

    void OnStopLeading() override;

    void Clear() override;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellMaster

#define MULTICELL_STATISTICS_COLLECTOR_DETAIL_INL_H_
#include "multicell_statistics_collector_detail-inl.h"
#undef MULTICELL_STATISTICS_COLLECTOR_DETAIL_INL_H_
