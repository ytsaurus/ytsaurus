#pragma once

#include <contrib/ydb/core/kqp/counters/kqp_counters.h>
#include <contrib/ydb/library/yql/dq/actors/compute/dq_compute_actor_async_io_factory.h>

namespace NKikimr {
namespace NKqp {

void RegisterStreamLookupActorFactory(NYql::NDq::TDqAsyncIoFactory& factory, TIntrusivePtr<NKqp::TKqpCounters>);

} // namespace NKqp
} // namespace NKikimr
