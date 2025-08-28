#pragma once

#include <yt/yt/ytlib/chunk_client/public.h>

#include <library/cpp/yt/containers/enum_indexed_array.h>

namespace NYT::NChunkPools {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EResourceKind,
    (DataSliceCount)
    (DataWeight)
    (PrimaryDataWeight)
);

//! Helper structure for representing job parameters.
struct TResourceVector
{
    TEnumIndexedArray<EResourceKind, i64> Values;

    static TResourceVector FromDataSlice(const NChunkClient::TLegacyDataSlicePtr& dataSlice, bool isPrimary);
    static TResourceVector Zero();

    TResourceVector operator+(const TResourceVector& other) const;
    TResourceVector& operator+=(const TResourceVector& other);
    TResourceVector operator-(const TResourceVector& other) const;
    TResourceVector& operator-=(const TResourceVector& other);

    bool operator==(const TResourceVector& other) const = default;

    TResourceVector operator*(double scale) const;

    TResourceVector& PartialMultiply(double scale, const std::vector<EResourceKind>& resourceKinds, i64 clampValue);

    bool Violates(const TResourceVector& limits) const;
    THashSet<EResourceKind> ViolatedResources(const TResourceVector& limits) const;

    bool IsZero() const;
    bool HasNegativeComponent() const;

    i64 GetDataWeight() const;
    i64 GetPrimaryDataWeight() const;
};

void FormatValue(TStringBuilderBase* builder, const TResourceVector& vector, TStringBuf /*spec*/);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkPools
