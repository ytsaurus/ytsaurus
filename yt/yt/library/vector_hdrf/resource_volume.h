#pragma once

#include <yt/yt/library/vector_hdrf/job_resources.h>

#include <library/cpp/yt/misc/property.h>

#include <util/datetime/base.h>

namespace NYT::NVectorHdrf {

////////////////////////////////////////////////////////////////////////////////

class TResourceVolume
{
public:
    DEFINE_BYVAL_RW_PROPERTY(double, UserSlots);
    DEFINE_BYVAL_RW_PROPERTY(TCpuResource, Cpu);
    DEFINE_BYVAL_RW_PROPERTY(double, Gpu);
    DEFINE_BYVAL_RW_PROPERTY(double, Memory);
    DEFINE_BYVAL_RW_PROPERTY(double, Network);

    TResourceVolume() = default;

    explicit TResourceVolume(const TJobResources& jobResources, TDuration duration);

    double GetMinResourceRatio(const TJobResources& denominator) const;

    bool IsZero() const;

    template <class TFunction>
    static void ForEachResource(TFunction processResource)
    {
        processResource(EJobResourceType::UserSlots, &TResourceVolume::UserSlots_);
        processResource(EJobResourceType::Cpu, &TResourceVolume::Cpu_);
        processResource(EJobResourceType::Network, &TResourceVolume::Network_);
        processResource(EJobResourceType::Memory, &TResourceVolume::Memory_);
        processResource(EJobResourceType::Gpu, &TResourceVolume::Gpu_);
    }
};

TResourceVolume Max(const TResourceVolume& lhs, const TResourceVolume& rhs);
TResourceVolume Min(const TResourceVolume& lhs, const TResourceVolume& rhs);

bool operator == (const TResourceVolume& lhs, const TResourceVolume& rhs);
TResourceVolume& operator += (TResourceVolume& lhs, const TResourceVolume& rhs);
TResourceVolume& operator -= (TResourceVolume& lhs, const TResourceVolume& rhs);
TResourceVolume& operator *= (TResourceVolume& lhs, double rhs);
TResourceVolume& operator /= (TResourceVolume& lhs, double rhs);
TResourceVolume operator + (const TResourceVolume& lhs, const TResourceVolume& rhs);
TResourceVolume operator - (const TResourceVolume& lhs, const TResourceVolume& rhs);
TResourceVolume operator * (const TResourceVolume& lhs, double rhs);
TResourceVolume operator / (const TResourceVolume& lhs, double rhs);

void Serialize(const TResourceVolume& volume, NYson::IYsonConsumer* consumer);
void Deserialize(TResourceVolume& volume, NYTree::INodePtr node);
void Deserialize(TResourceVolume& volume, NYson::TYsonPullParserCursor* cursor);

void FormatValue(TStringBuilderBase* builder, const TResourceVolume& volume, TStringBuf /*spec*/);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NVectorHdrf

