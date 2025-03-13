#pragma once

#include <contrib/ydb/core/kqp/common/buffer/events.h>
#include <contrib/ydb/core/kqp/counters/kqp_counters.h>
#include <contrib/ydb/core/scheme/scheme_tabledefs.h>
#include <contrib/ydb/core/scheme/scheme_types_proto.h>
#include <contrib/ydb/core/tx/data_events/events.h>
#include <contrib/ydb/library/yql/dq/actors/compute/dq_compute_actor_async_io_factory.h>

namespace NKikimr {
namespace NKqp {

void RegisterKqpWriteActor(NYql::NDq::TDqAsyncIoFactory&, TIntrusivePtr<TKqpCounters>);

}
}
