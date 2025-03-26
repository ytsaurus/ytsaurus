#include "service.h"


#include <contrib/ydb/core/statistics/events.h>
#include <contrib/ydb/core/statistics/database/database.h>

#include <contrib/ydb/library/services/services.pb.h>
#include <contrib/ydb/core/base/feature_flags.h>
#include <contrib/ydb/core/base/path.h>
#include <contrib/ydb/core/base/tablet_pipecache.h>
#include <contrib/ydb/core/tx/scheme_cache/scheme_cache.h>
#include <contrib/ydb/core/cms/console/configs_dispatcher.h>
#include <contrib/ydb/core/cms/console/console.h>
#include <contrib/ydb/core/base/appdata_fwd.h>
#include <contrib/ydb/core/mon/mon.h>
#include <contrib/ydb/core/protos/statistics.pb.h>
#include <contrib/ydb/core/protos/data_events.pb.h>

#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>
#include <contrib/ydb/library/actors/core/hfunc.h>
#include <contrib/ydb/library/actors/core/log.h>

#include <library/cpp/monlib/service/pages/templates.h>

namespace NKikimr {
namespace NStat {

static constexpr TDuration DefaultAggregateKeepAlivePeriod = TDuration::MilliSeconds(500);
static constexpr TDuration DefaultAggregateKeepAliveTimeout = TDuration::Seconds(3);
static constexpr TDuration DefaultAggregateKeepAliveAckTimeout = TDuration::Seconds(3);
static constexpr TDuration DefaultStatisticsRequestTimeout = TDuration::Seconds(5);
static constexpr size_t DefaultMaxInFlightTabletRequests = 5;
static constexpr size_t DefaultFanOutFactor = 5;



TStatServiceSettings::TStatServiceSettings()
    : AggregateKeepAlivePeriod(DefaultAggregateKeepAlivePeriod)
    , AggregateKeepAliveTimeout(DefaultAggregateKeepAliveTimeout)
    , AggregateKeepAliveAckTimeout(DefaultAggregateKeepAliveAckTimeout)
    , StatisticsRequestTimeout(DefaultStatisticsRequestTimeout)
    , MaxInFlightTabletRequests(DefaultMaxInFlightTabletRequests)
    , FanOutFactor(DefaultFanOutFactor)
{}

NActors::TActorId MakeStatServiceID(ui32 node) {
    const char x[12] = "StatService";
    return NActors::TActorId(node, TStringBuf(x, 12));
}

} // NStat
} // NKikimr
