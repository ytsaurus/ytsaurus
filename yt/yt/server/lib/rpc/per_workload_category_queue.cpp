#include "per_workload_category_queue.h"

#include <yt/yt_proto/yt/client/misc/proto/workload.pb.h>

#include <yt/yt/core/rpc/service_detail.h>

namespace NYT::NRpc {

using NYT::FromProto;

////////////////////////////////////////////////////////////////////////////////

TPerWorkloadCategoryRequestQueue::TPerWorkloadCategoryRequestQueue()
{
    for (auto category : TEnumTraits<EWorkloadCategory>::GetDomainValues()) {
        RequestQueues_[category] = CreateRequestQueue(FormatEnum(category));
    }
}

TRequestQueueProvider TPerWorkloadCategoryRequestQueue::GetProvider() const
{
    return BIND([=] (const NRpc::NProto::TRequestHeader& header) {
        auto category = header.HasExtension(NYT::NProto::TWorkloadDescriptorExt::workload_descriptor)
            ? FromProto<EWorkloadCategory>(header.GetExtension(NYT::NProto::TWorkloadDescriptorExt::workload_descriptor).category())
            : EWorkloadCategory::UserBatch;
        return RequestQueues_[category].Get();
    });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc
