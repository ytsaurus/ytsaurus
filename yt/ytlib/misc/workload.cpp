#include "workload.h"

#include <yt/ytlib/misc/workload.pb.h>

#include <yt/core/misc/string.h>

#include <yt/core/rpc/service.h>

#include <yt/core/ytree/yson_serializable.h>

namespace NYT {

using namespace NYTree;
using namespace NYson;
using namespace NRpc;

////////////////////////////////////////////////////////////////////////////////

static const i64 CategoryPriorityFactor = (i64) 1 << 56;
static const i64 BandPriorityFactor = (i64) 1 << 48;

////////////////////////////////////////////////////////////////////////////////

TWorkloadDescriptor::TWorkloadDescriptor(
    EWorkloadCategory category,
    int band,
    TInstant instant,
    std::vector<TString> annotations)
    : Category(category)
    , Band(band)
    , Instant(instant)
    , Annotations(std::move(annotations))
{ }

TWorkloadDescriptor TWorkloadDescriptor::SetCurrentInstant() const
{
    return TWorkloadDescriptor(Category, Band, TInstant::Now(), Annotations);
}

i64 TWorkloadDescriptor::GetPriority() const
{
    auto priority = GetBasicPriority(Category) + BandPriorityFactor * Band;
    if (Category == EWorkloadCategory::UserBatch) {
        priority -= Instant.MilliSeconds();
    }
    return priority;
}

////////////////////////////////////////////////////////////////////////////////

i64 GetBasicPriority(EWorkloadCategory category)
{
    switch (category) {
        case EWorkloadCategory::Idle:
            return 0;

        case EWorkloadCategory::SystemReplication:
        case EWorkloadCategory::SystemTabletCompaction:
        case EWorkloadCategory::SystemTabletPartitioning:
        case EWorkloadCategory::SystemTabletPreload:
        case EWorkloadCategory::SystemArtifactCacheDownload:
        case EWorkloadCategory::UserBatch:
            return CategoryPriorityFactor * 1;

        case EWorkloadCategory::SystemRepair:
            return CategoryPriorityFactor * 2;

        case EWorkloadCategory::UserInteractive:
        case EWorkloadCategory::SystemTabletRecovery:
            return CategoryPriorityFactor * 3;

        case EWorkloadCategory::UserRealtime:
        case EWorkloadCategory::SystemTabletLogging:
            return CategoryPriorityFactor * 4;

        // Graceful fallback for possible future extensions of categories.
        default:
            return 0;
    }
}

struct TSerializableWorkloadDescriptor
    : public TWorkloadDescriptor
    , public TYsonSerializableLite
{
    TSerializableWorkloadDescriptor()
    {
        RegisterParameter("category", Category);
        RegisterParameter("band", Band)
            .Default(0);
        RegisterParameter("annotations", Annotations)
            .Default();
    }
};

void Serialize(const TWorkloadDescriptor& descriptor, IYsonConsumer* consumer)
{
    TSerializableWorkloadDescriptor wrapper;
    static_cast<TWorkloadDescriptor&>(wrapper) = descriptor;
    Serialize(static_cast<const TYsonSerializableLite&>(wrapper), consumer);
}

void Deserialize(TWorkloadDescriptor& descriptor, INodePtr node)
{
    TSerializableWorkloadDescriptor wrapper;
    Deserialize(static_cast<TYsonSerializableLite&>(wrapper), node);
    descriptor = static_cast<TWorkloadDescriptor&>(wrapper);
}

void ToProto(NYT::NProto::TWorkloadDescriptor* protoDescriptor, const TWorkloadDescriptor& descriptor)
{
    protoDescriptor->set_category(static_cast<int>(descriptor.Category));
    protoDescriptor->set_band(descriptor.Band);
    protoDescriptor->set_instant(ToProto<i64>(descriptor.Instant));
    ToProto(protoDescriptor->mutable_annotations(), descriptor.Annotations);
}

void FromProto(TWorkloadDescriptor* descriptor, const NYT::NProto::TWorkloadDescriptor& protoDescriptor)
{
    descriptor->Category = EWorkloadCategory(protoDescriptor.category());
    descriptor->Band = protoDescriptor.band();
    descriptor->Instant = FromProto<TInstant>(protoDescriptor.instant());
    FromProto(&descriptor->Annotations, protoDescriptor.annotations());
}

void FormatValue(
    TStringBuilder* builder,
    const TWorkloadDescriptor& descriptor,
    TStringBuf /*format*/)
{
    builder->AppendFormat("%v:%v",
        descriptor.Category,
        descriptor.Band);
    if (descriptor.Instant != TInstant::Zero()) {
        builder->AppendFormat(":%v",
            descriptor.Instant);
    }
    if (!descriptor.Annotations.empty()) {
        builder->AppendString(":{");
        for (size_t index = 0; index < descriptor.Annotations.size(); ++index) {
            builder->AppendString(descriptor.Annotations[index]);
            if (index != descriptor.Annotations.size() - 1) {
                builder->AppendString(", ");
            }
        }
        builder->AppendChar('}');
    }
}

TString ToString(const TWorkloadDescriptor& descriptor)
{
    return ToStringViaBuilder(descriptor);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

