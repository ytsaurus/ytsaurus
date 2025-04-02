#include "resource_volume.h"

#include <yt/yt/core/ytree/fluent.h>

#include <yt/yt/library/numeric/serialize/fixed_point_number.h>

namespace NYT::NVectorHdrf {

////////////////////////////////////////////////////////////////////////////////

using std::round;
using namespace NYson;
using namespace NYTree;

TResourceVolume::TResourceVolume(const TJobResources& jobResources, TDuration duration)
{
    auto seconds = duration.SecondsFloat();

    #define XX(name, Name) Name##_ = static_cast<decltype(Name##_)>(jobResources.Get##Name() * seconds);
    ITERATE_JOB_RESOURCES(XX)
    #undef XX
}

double TResourceVolume::GetMinResourceRatio(const TJobResources& denominator) const
{
    double result = std::numeric_limits<double>::max();
    bool updated = false;
    auto update = [&] (auto a, auto b) {
        if (static_cast<double>(b) > 0.0) {
            result = std::min(result, static_cast<double>(a) / static_cast<double>(b));
            updated = true;
        }
    };
    #define XX(name, Name) update(Get##Name(), denominator.Get##Name());
    ITERATE_JOB_RESOURCES(XX)
    #undef XX
    return updated ? result : 0.0;
}

bool TResourceVolume::IsZero() const
{
    bool result = true;
    TResourceVolume::ForEachResource([&] (EJobResourceType /*resourceType*/, auto TResourceVolume::* resourceDataMember) {
        result = result && this->*resourceDataMember == 0;
    });
    return result;
}

TResourceVolume Max(const TResourceVolume& lhs, const TResourceVolume& rhs)
{
    TResourceVolume result;
    #define XX(name, Name) result.Set##Name(std::max(lhs.Get##Name(), rhs.Get##Name()));
    ITERATE_JOB_RESOURCES(XX)
    #undef XX
    return result;
}

TResourceVolume Min(const TResourceVolume& lhs, const TResourceVolume& rhs)
{
    TResourceVolume result;
    #define XX(name, Name) result.Set##Name(std::min(lhs.Get##Name(), rhs.Get##Name()));
    ITERATE_JOB_RESOURCES(XX)
    #undef XX
    return result;
}

bool operator == (const TResourceVolume& lhs, const TResourceVolume& rhs)
{
    return
    #define XX(name, Name) lhs.Get##Name() == rhs.Get##Name() &&
        ITERATE_JOB_RESOURCES(XX)
    #undef XX
    true;
}

TResourceVolume& operator += (TResourceVolume& lhs, const TResourceVolume& rhs)
{
    #define XX(name, Name) lhs.Set##Name(lhs.Get##Name() + rhs.Get##Name());
    ITERATE_JOB_RESOURCES(XX)
    #undef XX
    return lhs;
}

TResourceVolume& operator -= (TResourceVolume& lhs, const TResourceVolume& rhs)
{
    #define XX(name, Name) lhs.Set##Name(lhs.Get##Name() - rhs.Get##Name());
    ITERATE_JOB_RESOURCES(XX)
    #undef XX
    return lhs;
}

TResourceVolume& operator *= (TResourceVolume& lhs, double rhs)
{
    #define XX(name, Name) lhs.Set##Name(lhs.Get##Name() * rhs);
    ITERATE_JOB_RESOURCES(XX)
    #undef XX
    return lhs;
}

TResourceVolume& operator /= (TResourceVolume& lhs, double rhs)
{
    #define XX(name, Name) lhs.Set##Name(lhs.Get##Name() / rhs);
    ITERATE_JOB_RESOURCES(XX)
    #undef XX
    return lhs;
}

TResourceVolume operator + (const TResourceVolume& lhs, const TResourceVolume& rhs)
{
    TResourceVolume result = lhs;
    result += rhs;
    return result;
}

TResourceVolume operator - (const TResourceVolume& lhs, const TResourceVolume& rhs)
{
    TResourceVolume result = lhs;
    result -= rhs;
    return result;
}

TResourceVolume operator * (const TResourceVolume& lhs, double rhs)
{
    TResourceVolume result = lhs;
    result *= rhs;
    return result;
}

TResourceVolume operator / (const TResourceVolume& lhs, double rhs)
{
    TResourceVolume result = lhs;
    result /= rhs;
    return result;
}

void Serialize(const TResourceVolume& volume, NYson::IYsonConsumer* consumer)
{
    NYTree::BuildYsonFluently(consumer)
        .BeginMap()
            #define XX(name, Name) .Item(#name).Value(volume.Get##Name())
            ITERATE_JOB_RESOURCES(XX)
            #undef XX
        .EndMap();
}

void Deserialize(TResourceVolume& volume, INodePtr node)
{
    auto mapNode = node->AsMap();
    #define XX(name, Name) \
        if (auto child = mapNode->FindChild(#name)) { \
            auto value = volume.Get##Name(); \
            Deserialize(value, child); \
            volume.Set##Name(value); \
        }
    ITERATE_JOB_RESOURCES(XX)
    #undef XX
}

void Deserialize(TResourceVolume& volume, TYsonPullParserCursor* cursor)
{
    Deserialize(volume, ExtractTo<INodePtr>(cursor));
}

void FormatValue(TStringBuilderBase* builder, const TResourceVolume& volume, TStringBuf /*spec*/)
{
    builder->AppendFormat(
        "{UserSlots: %.2f, Cpu: %v, Gpu: %.2f, Memory: %.2fMBs, Network: %.2f}",
        volume.GetUserSlots(),
        volume.GetCpu(),
        volume.GetGpu(),
        volume.GetMemory() / 1_MB,
        volume.GetNetwork());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NVectorHdrf

