#include "per_workload_category_request_queue_provider.h"

#include <yt/yt_proto/yt/client/misc/proto/workload.pb.h>

#include <yt/yt/client/misc/workload.h>

#include <yt/yt/core/rpc/request_queue_provider.h>
#include <yt/yt/core/rpc/service_detail.h>

#include <library/cpp/yt/misc/enum.h>

namespace NYT::NRpc {

using NYT::FromProto;

////////////////////////////////////////////////////////////////////////////////

class TPerWorkloadCategoryRequestQueueProvider
    : public TRequestQueueProviderBase
{
public:
    TPerWorkloadCategoryRequestQueueProvider();

    TRequestQueue* GetQueue(const NRpc::NProto::TRequestHeader& header) override;

private:
    TEnumIndexedVector<EWorkloadCategory, TRequestQueuePtr> RequestQueues_;
};

DECLARE_REFCOUNTED_CLASS(TPerWorkloadCategoryRequestQueueProvider)
DEFINE_REFCOUNTED_TYPE(TPerWorkloadCategoryRequestQueueProvider)

////////////////////////////////////////////////////////////////////////////////

TPerWorkloadCategoryRequestQueueProvider::TPerWorkloadCategoryRequestQueueProvider()
{
    for (auto category : TEnumTraits<EWorkloadCategory>::GetDomainValues()) {
        RequestQueues_[category] = CreateRequestQueue(FormatEnum(category));
    }
}

TRequestQueue* TPerWorkloadCategoryRequestQueueProvider::GetQueue(
    const NProto::TRequestHeader& header)
{
    const auto extId = NYT::NProto::TWorkloadDescriptorExt::workload_descriptor;
    auto category = header.HasExtension(extId)
        ? FromProto<EWorkloadCategory>(header.GetExtension(extId).category())
        : EWorkloadCategory::UserBatch;
    return RequestQueues_[category].Get();
}

////////////////////////////////////////////////////////////////////////////////

IRequestQueueProviderPtr CreatePerWorkloadCategoryRequestQueueProvider()
{
    return New<TPerWorkloadCategoryRequestQueueProvider>();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc
