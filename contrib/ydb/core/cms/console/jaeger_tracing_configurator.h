#pragma once

#include "defs.h"

#include <contrib/ydb/core/jaeger_tracing/sampling_throttling_configurator.h>
#include <contrib/ydb/core/protos/config.pb.h>

namespace NKikimr::NConsole {

IActor* CreateJaegerTracingConfigurator(TIntrusivePtr<NJaegerTracing::TSamplingThrottlingConfigurator> tracingConfigurator,
                                                NKikimrConfig::TTracingConfig cfg);

} // namespace NKikimr::NConsole
