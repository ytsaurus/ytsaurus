#pragma once


#include <contrib/ydb/core/base/memory_controller_iface.h>
#include <contrib/ydb/core/mon_alloc/memory_info.h>
#include <contrib/ydb/core/protos/memory_controller_config.pb.h>
#include <contrib/ydb/library/actors/core/defs.h>
#include <contrib/ydb/library/actors/core/actor.h>
#include <library/cpp/monlib/dynamic_counters/counters.h>

namespace NKikimr::NMemory {

struct TResourceBrokerConfig {
    ui64 LimitBytes = 0;
    ui64 QueryExecutionLimitBytes = 0;

    auto operator<=>(const TResourceBrokerConfig&) const = default;

    TString ToString() const noexcept {
        TStringBuilder result;
        result << "LimitBytes: " << LimitBytes;
        result << " QueryExecutionLimitBytes: " << QueryExecutionLimitBytes;
        return result;
    }
};

NActors::IActor* CreateMemoryController(
    TDuration interval,
    TIntrusiveConstPtr<IProcessMemoryInfoProvider> processMemoryInfoProvider,
    const NKikimrConfig::TMemoryControllerConfig& config,
    const TResourceBrokerConfig& resourceBrokerSelfConfig,
    const TIntrusivePtr<::NMonitoring::TDynamicCounters>& counters);

}