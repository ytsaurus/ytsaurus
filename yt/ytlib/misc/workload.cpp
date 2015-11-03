#include "stdafx.h"
#include "workload.h"

#include <core/misc/string.h>

#include <core/ytree/yson_serializable.h>

#include <core/rpc/service.h>

#include <ytlib/misc/workload.pb.h>

namespace NYT {

using namespace NYTree;
using namespace NYson;
using namespace NRpc;

////////////////////////////////////////////////////////////////////////////////

static const i64 CategoryPriorityFactor = (i64) 1 << 56;
static const i64 BandPriorityFactor = (i64) 1 << 48;

////////////////////////////////////////////////////////////////////////////////

TWorkloadDescriptor TWorkloadDescriptor::SetCurrentInstant() const
{
    return TWorkloadDescriptor(Category, Band, TInstant::Now());
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
        case EWorkloadCategory::SystemRepair:
        case EWorkloadCategory::UserBatch:
            return -CategoryPriorityFactor * 1;

        case EWorkloadCategory::UserRealtime:
            return -CategoryPriorityFactor * 2;

        case EWorkloadCategory::SystemRealtime:
            return -CategoryPriorityFactor * 2;

        default:
            YUNREACHABLE();
    }
}

struct TSerializableWorkloadDescriptor
    : public TWorkloadDescriptor
    , public TYsonSerializableLite
{
    TSerializableWorkloadDescriptor(const TWorkloadDescriptor& other = TWorkloadDescriptor())
        : TWorkloadDescriptor(other)
    {
        RegisterParameter("category", Category);
        RegisterParameter("band", Band)
            .Default(0);
    }
};

void Serialize(const TWorkloadDescriptor& resources, IYsonConsumer* consumer)
{
    TSerializableWorkloadDescriptor wrapper(resources);
    Serialize(static_cast<const TYsonSerializableLite&>(wrapper), consumer);
}

void Deserialize(TWorkloadDescriptor& value, INodePtr node)
{
    TSerializableWorkloadDescriptor wrapper;
    Deserialize(static_cast<TYsonSerializableLite&>(wrapper), node);
    value = static_cast<TWorkloadDescriptor&>(wrapper);
}

void ToProto(NYT::NProto::TWorkloadDescriptor* protoDescriptor, const TWorkloadDescriptor& descriptor)
{
    protoDescriptor->set_category(static_cast<int>(descriptor.Category));
    protoDescriptor->set_band(descriptor.Band);
    protoDescriptor->set_instant(descriptor.Instant.MicroSeconds());
}

void FromProto(TWorkloadDescriptor* descriptor, const NYT::NProto::TWorkloadDescriptor& protoDescriptor)
{
    descriptor->Category = EWorkloadCategory(protoDescriptor.category());
    descriptor->Band = protoDescriptor.band();
    descriptor->Instant = TInstant(protoDescriptor.instant());
}

void FormatValue(
    TStringBuilder* builder,
    const TWorkloadDescriptor& descriptor,
    const TStringBuf& /*format*/)
{
    builder->AppendFormat("%v:%v",
        descriptor.Category,
        descriptor.Band);
    if (descriptor.Instant != TInstant::Zero()) {
        builder->AppendFormat(":%v",
            descriptor.Instant);
    }
}

Stroka ToString(const TWorkloadDescriptor& descriptor)
{
    TStringBuilder builder;
    FormatValue(&builder, descriptor, TStringBuf());
    return builder.Flush();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

