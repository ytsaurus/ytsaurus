#include "pod.h"

#include "pod_set.h"
#include "resource_capacities.h"

#include <yt/core/misc/protobuf_helpers.h>
#include <yt/core/ypath/tokenizer.h>
#include <yt/core/ytree/node.h>

namespace NYP::NServer::NCluster {

using namespace NYT::NYPath;
using namespace NYT::NYTree;

////////////////////////////////////////////////////////////////////////////////

namespace {

TErrorOr<TString> ParseAntiaffinityGroupIdPathLabelsKey(const NYPath::TYPath& groupIdPath)
{
    try {
        TTokenizer tokenizer(groupIdPath);

        auto advanceToLiteral = [&tokenizer] {
            tokenizer.Advance();
            tokenizer.Expect(ETokenType::Slash);
            tokenizer.Advance();
            tokenizer.Expect(ETokenType::Literal);
        };

        advanceToLiteral();
        static const TString LabelsLiteral("labels");
        if (tokenizer.GetLiteralValue() != LabelsLiteral) {
            tokenizer.ThrowUnexpected();
        }

        advanceToLiteral();
        TString key = tokenizer.GetLiteralValue();

        tokenizer.Advance();
        tokenizer.Expect(ETokenType::EndOfStream);

        return key;
    } catch (const std::exception& ex) {
        return TError("Expected pod antiaffinity group id path of the form /labels/<key>, but got %v",
            groupIdPath)
            << TError(ex);
    }
}

} // namespace

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
    const NObjects::TSchedulingHints& schedulingHints,
    bool enableScheduling,
    NClient::NApi::NProto::TPodStatus_TEviction eviction,
    NYT::NProto::TError schedulingError,
    const NObjects::TNodeAlerts& nodeAlerts,
    NClient::NApi::NProto::TPodStatus_TMaintenance maintenance)
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
    , SchedulingHints_(schedulingHints)
    , EnableScheduling_(enableScheduling)
    , Eviction_(std::move(eviction))
    , SchedulingError_(std::move(schedulingError))
    , NodeAlerts_(nodeAlerts)
    , Maintenance_(std::move(maintenance))
    , AntiaffinityGroupIdsOrError_(TError("Uninitialized pod antiaffinity group ids"))
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

TError TPod::ParseSchedulingError() const
{
    return FromProto<TError>(SchedulingError_);
}

ui64 TPod::GetInternetAddressRequestCount() const
{
    ui64 result = 0;
    for (const auto& addressRequest : IP6AddressRequests_) {
        if (addressRequest.enable_internet() || !addressRequest.ip4_address_pool_id().empty()) {
            ++result;
        }
    }
    return result;
}

ui64 TPod::GetDiskRequestTotalCapacity(const TString& storageClass) const
{
    ui64 result = 0;
    for (const auto& request : DiskVolumeRequests_) {
        if (request.storage_class() == storageClass) {
            result += GetDiskCapacity(GetDiskVolumeRequestCapacities(request));
        }
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

void TPod::PostprocessAttributes()
{
    auto labelsMap = ParseLabels();

    AntiaffinityGroupIdsOrError_ = PostprocessAntiaffinityGroupIds(labelsMap);
}

TError TPod::GetSchedulingAttributesValidationError() const
{
    if (!AntiaffinityGroupIdsOrError_.IsOK()) {
        return TError("Error validating pod scheduling attributes")
            << AntiaffinityGroupIdsOrError_;
    }
    return TError();
}

////////////////////////////////////////////////////////////////////////////////

TErrorOr<TString> TPod::GetAntiaffinityGroupId(const NYPath::TYPath& groupIdAttributePath) const
{
    if (!AntiaffinityGroupIdsOrError_.IsOK()) {
        return TError(AntiaffinityGroupIdsOrError_);
    }

    const auto& antiaffinityGroupIds = AntiaffinityGroupIdsOrError_.Value();

    auto it = antiaffinityGroupIds.find(groupIdAttributePath);
    if (it == antiaffinityGroupIds.end()) {
        static const TString NullGroupId;
        return NullGroupId;
    }

    const auto& groupId = it->second;
    YT_VERIFY(groupId);

    return groupId;
}

////////////////////////////////////////////////////////////////////////////////

TErrorOr<TPod::TAntiaffinityGroupIdByAttributePath> TPod::PostprocessAntiaffinityGroupIds(
    const IMapNodePtr& labelsMap) const
{
    TAntiaffinityGroupIdByAttributePath result;
    for (const auto& antiaffinityConstraint : PodSet_->AntiaffinityConstraints()) {
        const auto& groupIdPath = antiaffinityConstraint.pod_group_id_path();
        if (!groupIdPath) {
            continue;
        }

        auto labelsGroupIdKeyOrError = ParseAntiaffinityGroupIdPathLabelsKey(groupIdPath);
        if (!labelsGroupIdKeyOrError.IsOK()) {
            return TError(labelsGroupIdKeyOrError);
        }
        const auto& labelsGroupIdKey = labelsGroupIdKeyOrError
            .Value();

        if (auto groupIdNode = labelsMap->FindChild(labelsGroupIdKey)) {
            if (groupIdNode->GetType() != ENodeType::String) {
                return TError(
                    "Unexpected type of the pod antiaffinity group id at path %v: expected %Qlv, but got %Qlv",
                    groupIdPath,
                    ENodeType::String,
                    groupIdNode->GetType());
            }

            const auto& groupId = groupIdNode->GetValue<TString>();

            if (!groupId) {
                return TError("Null pod antiaffinity group id at path %v",
                    groupIdPath);
            }

            result[groupIdPath] = groupId;
        }
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NCluster
