#pragma once

#include "public.h"

#include <yt/yt/core/actions/future.h>

namespace NYT::NCellMaster {

////////////////////////////////////////////////////////////////////////////////

template <class T>
concept CMulticellStatisticsValue =
    requires (T& value, TSaveContext& saveContext, TLoadContext& loadContext) {
        { value.Clear() }
            -> std::same_as<void>;

        { std::as_const(value).Save(saveContext) }
            -> std::same_as<void>;

        { value.Load(loadContext) }
            -> std::same_as<void>;

        {
            [] <template <class...> class TList, class... TReqs> (T& value, TList<TReqs...>&&) {
                (value.HydraApplyMulticellStatisticsUpdate(static_cast<std::add_pointer_t<TReqs>>(nullptr)), ...);
            }(value, std::declval<typename T::TMutationRequestTypes>())
        };

        { value.GetLocalCellUpdate() }
            -> CFuture;

        {
            value.HydraApplyMulticellStatisticsUpdate(
                static_cast<std::add_pointer_t<
                    typename std::invoke_result_t<decltype(&T::GetLocalCellUpdate), T*>::TValueType
                >>(nullptr))
        }
            -> std::same_as<void>;

        { value.FinishUpdate() }
            -> std::same_as<void>;

        { value.GetUpdatePeriod() }
            -> std::same_as<std::optional<TDuration>>;
    };

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellMaster

////////////////////////////////////////////////////////////////////////////////

namespace NYT::NCellMaster {

class TMulticellNodeStatistics;

} // namespace NYT::NCellMaster

namespace NYT::NChunkServer {

class TLostVitalChunksSample;

} // namespace NYT::NChunkServer

////////////////////////////////////////////////////////////////////////////////

namespace NYT::NCellMaster {

struct IMulticellStatisticsCollector
    : public virtual TRefCounted
{
    virtual void Initialize() = 0;

    virtual const TMulticellNodeStatistics& GetMulticellNodeStatistics() = 0;
    virtual TMulticellNodeStatistics& GetMutableMulticellNodeStatistics() = 0;

    virtual const NChunkServer::TLostVitalChunksSample& GetLostVitalChunksSample() = 0;
    virtual NChunkServer::TLostVitalChunksSample& GetMutableLostVitalChunksSample() = 0;
};

DEFINE_REFCOUNTED_TYPE(IMulticellStatisticsCollector);

////////////////////////////////////////////////////////////////////////////////

IMulticellStatisticsCollectorPtr CreateMulticellStatisticsCollector(TBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellMaster
