#include "helpers.h"

#include <yt/core/misc/format.h>

namespace NYP {
namespace NServer {
namespace NAccounting {

using namespace NObjects;

////////////////////////////////////////////////////////////////////////////////

namespace {

ui64 CpuCapacityFromSpec(const NServer::NObjects::NProto::TPodSpecOther& spec)
{
    return spec.resource_requests().vcpu_guarantee();
}

ui64 MemoryCapacityFromSpec(const NServer::NObjects::NProto::TPodSpecOther& spec)
{
    return spec.resource_requests().memory_limit();
}

ui64 DiskCapacityFromRequest(const NClient::NApi::NProto::TPodSpec::TDiskVolumeRequest& request)
{
    if (request.has_quota_policy()) {
        return request.quota_policy().capacity();
    } else if (request.has_exclusive_policy()) {
        // TODO(babenko)
        return request.exclusive_policy().min_capacity();
    } else {
        return 0;
    }
}

ui64 InternetAddressCapacityFromSpec(const NServer::NObjects::NProto::TPodSpecOther& spec)
{
    ui64 result = 0;
    for (const auto& request : spec.ip6_address_requests()) {
        if (request.enable_internet()) {
            ++result;
        }
    }
    return result;
}

} // namespace

TResourceTotals ResourceUsageFromPodSpec(
    const NServer::NObjects::NProto::TPodSpecOther& spec,
    const TObjectId& segmentId)
{
    TResourceTotals usage;
    auto& perSegmentUsage = (*usage.mutable_per_segment())[segmentId];
    perSegmentUsage.mutable_cpu()->set_capacity(perSegmentUsage.cpu().capacity() + CpuCapacityFromSpec(spec));
    perSegmentUsage.mutable_memory()->set_capacity(perSegmentUsage.memory().capacity() + MemoryCapacityFromSpec(spec));
    perSegmentUsage.mutable_internet_address()->set_capacity(perSegmentUsage.internet_address().capacity() + InternetAddressCapacityFromSpec(spec));
    for (const auto& volumeRequest : spec.disk_volume_requests()) {
        const auto& storageClass = volumeRequest.storage_class();
        auto& diskTotals = (*perSegmentUsage.mutable_disk_per_storage_class())[storageClass];
        diskTotals.set_capacity(diskTotals.capacity() + DiskCapacityFromRequest(volumeRequest));
    }
    return usage;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NAccounting
} // namespace NServer
} // namespace NYP

namespace NYP {
namespace NClient {
namespace NApi {
namespace NProto {

using namespace NYT;

////////////////////////////////////////////////////////////////////////////////

namespace {

void Aggregate(
    TPerSegmentResourceTotals& lhs,
    const TPerSegmentResourceTotals& rhs,
    int multiplier)
{
    lhs.mutable_cpu()->set_capacity(lhs.cpu().capacity() + rhs.cpu().capacity() * multiplier);
    lhs.mutable_memory()->set_capacity(lhs.memory().capacity() + rhs.memory().capacity() * multiplier);
    lhs.mutable_internet_address()->set_capacity(lhs.internet_address().capacity() + rhs.internet_address().capacity() * multiplier);
    for (const auto& pair : rhs.disk_per_storage_class()) {
        auto& diskTotals = (*lhs.mutable_disk_per_storage_class())[pair.first];
        diskTotals.set_capacity(diskTotals.capacity() + pair.second.capacity() * multiplier);
    }
}

void Aggregate(
    TResourceTotals& lhs,
    const TResourceTotals& rhs,
    int multiplier)
{
    for (const auto& pair : rhs.per_segment()) {
        Aggregate((*lhs.mutable_per_segment())[pair.first], pair.second, multiplier);
    }
}

} // namespace

TPerSegmentResourceTotals& operator +=(
    TPerSegmentResourceTotals& lhs,
    const TPerSegmentResourceTotals& rhs)
{
    Aggregate(lhs, rhs, +1);
    return lhs;
}

TPerSegmentResourceTotals operator +(
    const TPerSegmentResourceTotals& lhs,
    const TPerSegmentResourceTotals& rhs)
{
    auto result = lhs;
    result += rhs;
    return result;
}

TPerSegmentResourceTotals& operator -=(
    TPerSegmentResourceTotals& lhs,
    const TPerSegmentResourceTotals& rhs)
{
    Aggregate(lhs, rhs, -1);
    return lhs;
}

TPerSegmentResourceTotals operator -(
    const TPerSegmentResourceTotals& lhs,
    const TPerSegmentResourceTotals& rhs)
{
    auto result = lhs;
    result -= rhs;
    return result;
}

TResourceTotals& operator +=(
    TResourceTotals& lhs,
    const TResourceTotals& rhs)
{
    Aggregate(lhs, rhs, +1);
    return lhs;
}

TResourceTotals operator +(
    const TResourceTotals& lhs,
    const TResourceTotals& rhs)
{
    auto result = lhs;
    result += rhs;
    return result;
}

TResourceTotals& operator -=(
    TResourceTotals& lhs,
    const TResourceTotals& rhs)
{
    Aggregate(lhs, rhs, -1);
    return lhs;
}

TResourceTotals operator -(
    const TResourceTotals& lhs,
    const TResourceTotals& rhs)
{
    auto result = lhs;
    result -= rhs;
    return result;
}

TResourceTotals operator -(const TResourceTotals& arg)
{
    TResourceTotals result;
    result -= arg;
    return result;
}

void FormatValue(TStringBuilder* builder, const TPerSegmentResourceTotals& totals, TStringBuf /*format*/)
{
    builder->AppendString("{");

    TDelimitedStringBuilderWrapper globalDelimitedBuilder(builder);
    globalDelimitedBuilder->AppendFormat("Cpu: %v",
        totals.cpu().capacity());
    globalDelimitedBuilder->AppendFormat("Memory: %v",
        totals.memory().capacity());
    globalDelimitedBuilder->AppendFormat("InternetAddress: %v",
        totals.internet_address().capacity());
    
    {
        globalDelimitedBuilder->AppendString("PerStorageClass = {");
        TDelimitedStringBuilderWrapper diskDelimitedBuilder(builder);
        for (const auto& pair : totals.disk_per_storage_class()) {
            diskDelimitedBuilder->AppendFormat("%v=>{Disk: %v}",
                pair.first,
                pair.second.capacity());
        }
        builder->AppendString("}");
    }
    
    builder->AppendString("}");
}

void FormatValue(TStringBuilder* builder, const TResourceTotals& totals, TStringBuf /*format*/)
{
    builder->AppendString("{PerSegment: {");

    TDelimitedStringBuilderWrapper delimitedBuilder(builder);
    for (const auto& pair : totals.per_segment()) {
        delimitedBuilder->AppendFormat("%v=>%v",
            pair.first,
            pair.second);
    }
    builder->AppendString("}}");
}

TString ToString(const TPerSegmentResourceTotals& totals)
{
    return ToStringViaBuilder(totals);
}

TString ToString(const TResourceTotals& totals)
{
    return ToStringViaBuilder(totals);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NProto
} // namespace NApi
} // namespace NClient
} // namespace NYP
