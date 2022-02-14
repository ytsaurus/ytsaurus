#pragma once

#include <yt/yt/core/rpc/service_detail.h>

#include <yt/yt/client/misc/workload.h>

#include <library/cpp/yt/misc/enum.h>

namespace NYT::NRpc {

////////////////////////////////////////////////////////////////////////////////

class TPerWorkloadCategoryRequestQueue
{
public:
    TPerWorkloadCategoryRequestQueue();

    TRequestQueueProvider GetProvider() const;

private:
    TEnumIndexedVector<EWorkloadCategory, TRequestQueuePtr> RequestQueues_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc
