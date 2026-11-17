#pragma once

#include "worker_info.h"

#include <contrib/ydb/library/yql/providers/dq/worker_manager/interface/events.h>

#include <util/generic/ptr.h>
#include <util/generic/vector.h>

namespace NYql::NDqs {

THolder<TEvAllocateWorkersResponse> MakeAllocateWorkersResponse(
    ui64 resourceId,
    const TVector<TWorkerInfo::TPtr>& workerInfos);

} // namespace NYql::NDqs
