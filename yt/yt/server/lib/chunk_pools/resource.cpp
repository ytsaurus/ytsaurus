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
    result.Values[EResourceKind::CompressedDataSize] = dataSlice->GetCompressedDataSize();
    result.Values[EResourceKind::PrimaryCompressedDataSize] = isPrimary ? dataSlice->GetCompressedDataSize() : 0;
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

TResourceVector& TResourceVector::PartialMultiply(double scale, const std::vector<EResourceKind>& resourceKinds, i64 clampValue)
{
    i64 smallClamp = clampValue / scale;

    for (auto kind : resourceKinds) {
        Values[kind] = std::clamp(Values[kind], -smallClamp, smallClamp) * scale;
    }

    return *this;
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

void FormatValue(TStringBuilderBase* builder, const TResourceVector& vector, TStringBuf /*spec*/)
{
    Format(
        builder,
        "{DSC: %v, DW: %v, PDW: %v, CDS: %v, PCDS: %v}",
        vector.Values[EResourceKind::DataSliceCount],
        vector.Values[EResourceKind::DataWeight],
        vector.Values[EResourceKind::PrimaryDataWeight],
        vector.Values[EResourceKind::CompressedDataSize],
        vector.Values[EResourceKind::PrimaryCompressedDataSize]);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkPools
