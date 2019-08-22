#include "pod.h"
#include "pod_set.h"

namespace NYP::NServer::NScheduler {

using namespace NYT::NYson;

////////////////////////////////////////////////////////////////////////////////

TPod::TPod(
    TObjectId id,
    TYsonString labels,
    TObjectId podSetId,
    TObjectId nodeId,
    TObjectId accountId,
    TObjectId uuid,
    NObjects::TPodResourceRequests resourceRequests,
    const NObjects::TPodDiskVolumeRequests& diskVolumeRequests,
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
    , IP6AddressRequests_(ip6AddressRequests)
    , IP6SubnetRequests_(ip6SubnetRequests)
    , NodeFilter_(std::move(nodeFilter))
    , Eviction_(std::move(eviction))
{ }

TAccount* TPod::GetEffectiveAccount() const
{
    return Account_ ? Account_ : PodSet_->GetAccount();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NScheduler
