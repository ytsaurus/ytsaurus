#pragma once
#include <contrib/ydb/core/ymq/error/error.h>
#include <contrib/ydb/core/protos/msgbus.pb.h>
#include <contrib/ydb/core/ymq/base/counters.h>

namespace NKikimr::NSQS {

size_t ErrorsCount(const NKikimrClient::TSqsResponse& response, TAPIStatusesCounters* counters);

} // namespace NKikimr::NSQS
