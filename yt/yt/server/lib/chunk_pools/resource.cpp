#include "resource.h"

#include <yt/yt/ytlib/chunk_client/legacy_data_slice.h>

namespace NYT::NChunkPools {

using namespace NChunkClient;

////////////////////////////////////////////////////////////////////////////////

TResourceVector TResourceVector::FromDataSlice(const TLegacyDataSlicePtr& dataSlice, bool isPrimary)
{
    TResourceVector result;
    result.Values[EResourceKind::DataSliceCount] = 1;
    result.Values[EResourceKind::DataWeight] = dataSlice->GetDataWeight();
    result.Values[EResourceKind::PrimaryDataWeight] = isPrimary ? dataSlice->GetDataWeight() : 0;
    return result;
}

TResourceVector TResourceVector::Zero()
{
    TResourceVector result;
    std::fill(result.Values.begin(), result.Values.end(), 0);
    return result;
}

TResourceVector TResourceVector::operator+(const TResourceVector& other) const
{
    TResourceVector result;
    for (auto kind : TEnumTraits<EResourceKind>::GetDomainValues()) {
        result.Values[kind] = Values[kind] + other.Values[kind];
    }
    return result;
}

TResourceVector TResourceVector::operator-(const TResourceVector& other) const
{
    TResourceVector result;
    for (auto kind : TEnumTraits<EResourceKind>::GetDomainValues()) {
        result.Values[kind] = Values[kind] - other.Values[kind];
    }
    return result;
}

TResourceVector& TResourceVector::operator+=(const TResourceVector& other)
{
    for (auto kind : TEnumTraits<EResourceKind>::GetDomainValues()) {
        Values[kind] += other.Values[kind];
    }
    return *this;
}

TResourceVector& TResourceVector::operator-=(const TResourceVector& other)
{
    for (auto kind : TEnumTraits<EResourceKind>::GetDomainValues()) {
        Values[kind] -= other.Values[kind];
    }
    return *this;
}

TResourceVector TResourceVector::operator*(double scale) const
{
    TResourceVector result;
    for (auto kind : TEnumTraits<EResourceKind>::GetDomainValues()) {
        result.Values[kind] = Values[kind] * scale;
    }
    return result;
}

bool TResourceVector::Violates(const TResourceVector& limits) const
{
    for (auto kind : TEnumTraits<EResourceKind>::GetDomainValues()) {
        if (Values[kind] > limits.Values[kind]) {
            return true;
        }
    }
    return false;
}

THashSet<EResourceKind> TResourceVector::ViolatedResources(const TResourceVector& limits) const
{
    THashSet<EResourceKind> result;
    for (auto kind : TEnumTraits<EResourceKind>::GetDomainValues()) {
        if (Values[kind] > limits.Values[kind]) {
            result.insert(kind);
        }
    }
    return result;
}

bool TResourceVector::IsZero() const
{
    for (auto kind : TEnumTraits<EResourceKind>::GetDomainValues()) {
        if (Values[kind] != 0) {
            return false;
        }
    }
    return true;
}

bool TResourceVector::HasNegativeComponent() const
{
    for (auto kind : TEnumTraits<EResourceKind>::GetDomainValues()) {
        if (Values[kind] < 0) {
            return true;
        }
    }
    return false;
}

i64 TResourceVector::GetDataWeight() const
{
    return Values[EResourceKind::DataWeight];
}

i64 TResourceVector::GetPrimaryDataWeight() const
{
    return Values[EResourceKind::PrimaryDataWeight];
}

////////////////////////////////////////////////////////////////////////////////

TString ToString(const TResourceVector& vector)
{
    return Format(
        "{DSC: %v, DW: %v, PDW: %v}",
        vector.Values[EResourceKind::DataSliceCount],
        vector.Values[EResourceKind::DataWeight],
        vector.Values[EResourceKind::PrimaryDataWeight]);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkPools
