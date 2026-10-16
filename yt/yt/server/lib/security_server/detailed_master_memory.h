#pragma once

#include "public.h"

#include <yt/yt/core/yson/public.h>

#include <yt/yt/core/ytree/public.h>

#include <library/cpp/yt/containers/enum_indexed_array.h>

#include <library/cpp/yt/misc/property.h>

namespace NYT::NSecurityServer {

////////////////////////////////////////////////////////////////////////////////

class TDetailedMasterMemory
{
public:
    using TMasterMemoryArray = TEnumIndexedArray<EMasterMemoryType, i64>;

    DEFINE_BYREF_RW_PROPERTY(TMasterMemoryArray, DetailedMasterMemory);

    i64 operator[](EMasterMemoryType type) const;
    i64& operator[](EMasterMemoryType type);

    TDetailedMasterMemory& operator+=(const TDetailedMasterMemory& other);
    TDetailedMasterMemory operator+(const TDetailedMasterMemory& other) const;

    TDetailedMasterMemory& operator-=(const TDetailedMasterMemory& other);
    TDetailedMasterMemory operator-(const TDetailedMasterMemory& other) const;

    TDetailedMasterMemory& operator*=(i64 other);
    TDetailedMasterMemory operator*(i64 other) const;

    TDetailedMasterMemory operator-() const;

    bool operator==(const TDetailedMasterMemory& other) const;

    bool IsNegative() const;
    bool IsZero() const;

    i64 GetTotal() const;

    friend void Serialize(const TDetailedMasterMemory& detailedMasterMemory, NYson::IYsonConsumer* consumer);
    friend void Deserialize(TDetailedMasterMemory& detailedMasterMemory, NYTree::INodePtr node);

    friend void FormatValue(TStringBuilderBase* builder, const TDetailedMasterMemory& detailedMasterMemory, TStringBuf spec);
};

////////////////////////////////////////////////////////////////////////////////

void ToProto(
    NProto::TDetailedMasterMemory* protoDetailedMasterMemory,
    const TDetailedMasterMemory& detailedMasterMemory);
void FromProto(
    TDetailedMasterMemory* detailedMasterMemory,
    const NProto::TDetailedMasterMemory& protoDetailedMasterMemory);

void FormatValue(
    TStringBuilderBase* builder,
    const TDetailedMasterMemory& detailedMasterMemory,
    TStringBuf spec);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSecurityServer
