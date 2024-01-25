#pragma once

#include "public.h"

#include <yt/yt/server/master/cell_master/public.h>

#include <yt/yt/server/master/cypress_server/public.h>

#include <yt/yt/core/yson/public.h>

#include <library/cpp/yt/misc/enum_indexed_array.h>

namespace NYT::NSecurityServer {

////////////////////////////////////////////////////////////////////////////////

// TODO(aleksandra-zh): rename to TMasterMemory.
class TDetailedMasterMemory
{
public:
    i64 operator[] (EMasterMemoryType type) const;
    i64& operator[] (EMasterMemoryType type);

    TDetailedMasterMemory& operator += (const TDetailedMasterMemory& other);
    TDetailedMasterMemory operator + (const TDetailedMasterMemory& other) const;

    TDetailedMasterMemory& operator -= (const TDetailedMasterMemory& other);
    TDetailedMasterMemory operator - (const TDetailedMasterMemory& other) const;

    TDetailedMasterMemory& operator *= (i64 other);
    TDetailedMasterMemory operator * (i64 other) const;

    TDetailedMasterMemory operator-() const;

    bool operator==(const TDetailedMasterMemory& other) const;

    bool IsNegative() const;
    bool IsZero() const;

    i64 GetTotal() const;

    void Save(NCellMaster::TSaveContext& context) const;
    void Load(NCellMaster::TLoadContext& context);

    void Save(NCypressServer::TBeginCopyContext& context) const;
    void Load(NCypressServer::TEndCopyContext& context);

    friend void Serialize(const TDetailedMasterMemory& detailedMasterMemory, NYson::IYsonConsumer* consumer);
    friend void Deserialize(TDetailedMasterMemory& detailedMasterMemory, NYTree::INodePtr node);

    friend void FormatValue(TStringBuilderBase* builder, const TDetailedMasterMemory& detailedMasterMemory, TStringBuf spec);

private:
    TEnumIndexedArray<EMasterMemoryType, i64> DetailedMasterMemory_;
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
TString ToString(const TDetailedMasterMemory& detailedMasterMemory);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSecurityServer
