
#pragma once

#include <contrib/ydb/core/kqp/counters/kqp_counters.h>
#include <contrib/ydb/core/protos/kqp.pb.h>

#include <contrib/ydb/library/yql/dq/actors/compute/dq_compute_actor_async_io_factory.h>

namespace NKikimr::NKqp {

void RegisterKqpSysViewSource(NYql::NDq::TDqAsyncIoFactory& factory, TIntrusivePtr<TKqpCounters> counters);

} // namespace NKikimr::NKqp
