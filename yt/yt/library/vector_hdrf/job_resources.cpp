#include "job_resources.h"

#include <yt/yt/core/ytree/fluent.h>

#include <yt/yt/library/numeric/serialize/fixed_point_number.h>

namespace NYT::NVectorHdrf {

using std::round;
using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TJobResources TJobResources::Infinite()
{
    TJobResources result;
#define XX(name, Name) result.Set##Name(std::numeric_limits<decltype(result.Get##Name())>::max() / 4);
    ITERATE_JOB_RESOURCES(XX)
#undef XX
    return result;
}

////////////////////////////////////////////////////////////////////////////////

EJobResourceType GetDominantResource(
    const TJobResources& demand,
    const TJobResources& limits)
{
    auto maxType = EJobResourceType::Cpu;
    double maxRatio = 0.0;
    auto update = [&] (auto a, auto b, EJobResourceType type) {
        if (static_cast<double>(b) > 0.0) {
            double ratio = static_cast<double>(a) / static_cast<double>(b);
            if (ratio > maxRatio) {
                maxRatio = ratio;
                maxType = type;
            }
        }
    };
    #define XX(name, Name) update(demand.Get##Name(), limits.Get##Name(), EJobResourceType::Name);
    ITERATE_JOB_RESOURCES(XX)
    #undef XX
    return maxType;
}

double GetDominantResourceUsage(
    const TJobResources& usage,
    const TJobResources& limits)
{
    double maxRatio = 0.0;
    auto update = [&] (auto a, auto b) {
        if (static_cast<double>(b) > 0.0) {
            double ratio = static_cast<double>(a) / static_cast<double>(b);
            if (ratio > maxRatio) {
                maxRatio = ratio;
            }
        }
    };
    #define XX(name, Name) update(usage.Get##Name(), limits.Get##Name());
    ITERATE_JOB_RESOURCES(XX)
    #undef XX
    return maxRatio;
}

double GetResource(const TJobResources& resources, EJobResourceType type)
{
    switch (type) {
        #define XX(name, Name) \
            case EJobResourceType::Name: \
                return static_cast<double>(resources.Get##Name());
        ITERATE_JOB_RESOURCES(XX)
        #undef XX
        default:
            Y_ABORT();
    }
}

void SetResource(TJobResources& resources, EJobResourceType type, double value)
{
    switch (type) {
        #define XX(name, Name) \
            case EJobResourceType::Name: \
                resources.Set##Name(value); \
                break;
        ITERATE_JOB_RESOURCES(XX)
        #undef XX
        default:
            Y_ABORT();
    }
}

double GetMinResourceRatio(
    const TJobResources& nominator,
    const TJobResources& denominator)
{
    double result = std::numeric_limits<double>::max();
    bool updated = false;
    auto update = [&] (auto a, auto b) {
        if (static_cast<double>(b) > 0.0) {
            result = std::min(result, static_cast<double>(a) / static_cast<double>(b));
            updated = true;
        }
    };
    #define XX(name, Name) update(nominator.Get##Name(), denominator.Get##Name());
    ITERATE_JOB_RESOURCES(XX)
    #undef XX
    return updated ? result : 0.0;
}

double GetMaxResourceRatio(
    const TJobResources& nominator,
    const TJobResources& denominator)
{
    double result = 0.0;
    auto update = [&] (auto a, auto b) {
        if (static_cast<double>(b) > 0.0) {
            result = std::max(result, static_cast<double>(a) / static_cast<double>(b));
        }
    };
    #define XX(name, Name) update(nominator.Get##Name(), denominator.Get##Name());
    ITERATE_JOB_RESOURCES(XX)
    #undef XX
    return result;
}

TJobResources operator + (const TJobResources& lhs, const TJobResources& rhs)
{
    TJobResources result;
    #define XX(name, Name) result.Set##Name(lhs.Get##Name() + rhs.Get##Name());
    ITERATE_JOB_RESOURCES(XX)
    #undef XX
    return result;
}

TJobResources& operator += (TJobResources& lhs, const TJobResources& rhs)
{
    #define XX(name, Name) lhs.Set##Name(lhs.Get##Name() + rhs.Get##Name());
    ITERATE_JOB_RESOURCES(XX)
    #undef XX
    return lhs;
}

TJobResources operator - (const TJobResources& lhs, const TJobResources& rhs)
{
    TJobResources result;
    #define XX(name, Name) result.Set##Name(lhs.Get##Name() - rhs.Get##Name());
    ITERATE_JOB_RESOURCES(XX)
    #undef XX
    return result;
}

TJobResources& operator -= (TJobResources& lhs, const TJobResources& rhs)
{
    #define XX(name, Name) lhs.Set##Name(lhs.Get##Name() - rhs.Get##Name());
    ITERATE_JOB_RESOURCES(XX)
    #undef XX
    return lhs;
}

TJobResources operator * (const TJobResources& lhs, i64 rhs)
{
    TJobResources result;
    #define XX(name, Name) result.Set##Name(lhs.Get##Name() * rhs);
    ITERATE_JOB_RESOURCES(XX)
    #undef XX
    return result;
}

TJobResources operator * (const TJobResources& lhs, double rhs)
{
    TJobResources result;
    #define XX(name, Name) result.Set##Name(static_cast<decltype(lhs.Get##Name())>(round(lhs.Get##Name() * rhs)));
    ITERATE_JOB_RESOURCES(XX)
    #undef XX
    return result;
}

TJobResources& operator *= (TJobResources& lhs, i64 rhs)
{
    #define XX(name, Name) lhs.Set##Name(lhs.Get##Name() * rhs);
    ITERATE_JOB_RESOURCES(XX)
    #undef XX
    return lhs;
}

TJobResources& operator *= (TJobResources& lhs, double rhs)
{
    #define XX(name, Name) lhs.Set##Name(static_cast<decltype(lhs.Get##Name())>(round(lhs.Get##Name() * rhs)));
    ITERATE_JOB_RESOURCES(XX)
    #undef XX
    return lhs;
}

TJobResources  operator - (const TJobResources& resources)
{
    TJobResources result;
    #define XX(name, Name) result.Set##Name(-resources.Get##Name());
    ITERATE_JOB_RESOURCES(XX)
    #undef XX
    return result;
}

bool operator == (const TJobResources& lhs, const TJobResources& rhs)
{
    return
    #define XX(name, Name) lhs.Get##Name() == rhs.Get##Name() &&
        ITERATE_JOB_RESOURCES(XX)
    #undef XX
    true;
}

std::ostream& operator << (std::ostream& os, const TJobResources& jobResources)
{
    return os << Format("%v", jobResources);
}

bool Dominates(const TJobResources& lhs, const TJobResources& rhs)
{
    return
    #define XX(name, Name) lhs.Get##Name() >= rhs.Get##Name() &&
        ITERATE_JOB_RESOURCES(XX)
    #undef XX
    true;
}

bool StrictlyDominates(const TJobResources& lhs, const TJobResources& rhs)
{
    return
    #define XX(name, Name) lhs.Get##Name() > rhs.Get##Name() &&
        ITERATE_JOB_RESOURCES(XX)
    #undef XX
    true;
}

TJobResources Max(const TJobResources& lhs, const TJobResources& rhs)
{
    TJobResources result;
    #define XX(name, Name) result.Set##Name(std::max(lhs.Get##Name(), rhs.Get##Name()));
    ITERATE_JOB_RESOURCES(XX)
    #undef XX
    return result;
}

TJobResources Min(const TJobResources& lhs, const TJobResources& rhs)
{
    TJobResources result;
    #define XX(name, Name) result.Set##Name(std::min(lhs.Get##Name(), rhs.Get##Name()));
    ITERATE_JOB_RESOURCES(XX)
    #undef XX
    return result;
}

void Serialize(const TJobResources& resources, IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer)
        .BeginMap()
    #define XX(name, Name) .Item(#name).Value(resources.Get##Name())
    ITERATE_JOB_RESOURCES(XX)
    #undef XX
        .EndMap();
}

void Deserialize(TJobResources& resources, INodePtr node)
{
    TCpuResource x{};
    Deserialize(x, node);

    auto mapNode = node->AsMap();
    #define XX(name, Name) \
        if (auto child = mapNode->FindChild(#name)) { \
            auto value = resources.Get##Name(); \
            Deserialize(value, child); \
            resources.Set##Name(value); \
        }
    ITERATE_JOB_RESOURCES(XX)
    #undef XX
}

void FormatValue(TStringBuilderBase* builder, const TJobResources& resources, TStringBuf /*format*/)
{
    builder->AppendFormat(
        "{UserSlots: %v, Cpu: %v, Gpu: %v, Memory: %vMB, Network: %v}",
        resources.GetUserSlots(),
        resources.GetCpu(),
        resources.GetGpu(),
        resources.GetMemory() / 1_MB,
        resources.GetNetwork());
}

////////////////////////////////////////////////////////////////////////////////

bool TJobResourcesConfig::IsNonTrivial()
{
    bool isNonTrivial = false;
    ForEachResource([this, &isNonTrivial] (auto TJobResourcesConfig::* resourceDataMember, EJobResourceType /*resourceType*/) {
        isNonTrivial = isNonTrivial || (this->*resourceDataMember).has_value();
    });
    return isNonTrivial;
}

bool TJobResourcesConfig::IsEqualTo(const TJobResourcesConfig& other)
{
    bool result = true;
    ForEachResource([this, &result, &other] (auto TJobResourcesConfig::* resourceDataMember, EJobResourceType /*resourceType*/) {
        result = result && (this->*resourceDataMember == other.*resourceDataMember);
    });
    return result;
}

TJobResourcesConfig& TJobResourcesConfig::operator+=(const TJobResourcesConfig& addend)
{
    ForEachResource([this, &addend] (auto TJobResourcesConfig::* resourceDataMember, EJobResourceType /*resourceType*/) {
        if (!(addend.*resourceDataMember).has_value()) {
            return;
        }
        if ((this->*resourceDataMember).has_value()) {
            *(this->*resourceDataMember) += *(addend.*resourceDataMember);
        } else {
            this->*resourceDataMember = addend.*resourceDataMember;
        }
    });
    return *this;
}

////////////////////////////////////////////////////////////////////////////////

TJobResources ToJobResources(const TJobResourcesConfig& config, TJobResources defaultValue)
{
    if (config.UserSlots) {
        defaultValue.SetUserSlots(*config.UserSlots);
    }
    if (config.Cpu) {
        defaultValue.SetCpu(*config.Cpu);
    }
    if (config.Network) {
        defaultValue.SetNetwork(*config.Network);
    }
    if (config.Memory) {
        defaultValue.SetMemory(*config.Memory);
    }
    if (config.Gpu) {
        defaultValue.SetGpu(*config.Gpu);
    }
    return defaultValue;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NVectorHdrf
