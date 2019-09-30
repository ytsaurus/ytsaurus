#include "pod.h"

#include "pod_set.h"

namespace NYP::NServer::NCluster {

////////////////////////////////////////////////////////////////////////////////

TPod::TPod(
    TObjectId id,
    NYT::NYson::TYsonString labels,
    TObjectId podSetId,
    TObjectId nodeId,
    TObjectId accountId,
    TObjectId uuid,
    NObjects::TPodResourceRequests resourceRequests,
    const NObjects::TPodDiskVolumeRequests& diskVolumeRequests,
    const NObjects::TPodGpuRequests& gpuRequests,
    const NObjects::TPodIP6AddressRequests& ip6AddressRequests,
    const NObjects::TPodIP6SubnetRequests& ip6SubnetRequests,
    TString nodeFilter,
    NClient::NApi::NProto::TPodStatus_TEviction eviction)
    : TObject(std::move(id), std::move(labels))
    , PodSetId_(std::move(podSetId))
    , NodeId_(std::move(nodeId))
    , AccountId_(std::move(accountId))
    , Uuid_(std::move(uuid))
    , ResourceRequests_(std::move(resourceRequests))
    , DiskVolumeRequests_(diskVolumeRequests)
    , GpuRequests_(gpuRequests)
    , IP6AddressRequests_(ip6AddressRequests)
    , IP6SubnetRequests_(ip6SubnetRequests)
    , NodeFilter_(std::move(nodeFilter))
    , Eviction_(std::move(eviction))
{ }

TAccount* TPod::GetEffectiveAccount() const
{
    YT_VERIFY(PodSet_);
    return Account_ ? Account_ : PodSet_->GetAccount();
}

const TString& TPod::GetEffectiveNodeFilter() const
{
    YT_VERIFY(PodSet_);
    return !NodeFilter_.Empty() ? NodeFilter_ : PodSet_->NodeFilter();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NCluster
