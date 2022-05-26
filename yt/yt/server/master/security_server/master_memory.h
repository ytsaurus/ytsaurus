#pragma once

#include <yt/yt/server/master/cell_master/public.h>
#include <yt/yt/server/master/cypress_server/serialize.h>

#include <yt/yt/client/object_client/public.h>

namespace NYT::NSecurityServer {

////////////////////////////////////////////////////////////////////////////////

struct TMasterMemoryLimits
{
    TMasterMemoryLimits() = default;
    TMasterMemoryLimits(i64 total, i64 chunkHost, THashMap<NObjectClient::TCellTag, i64> perCell);

    void Save(NCellMaster::TSaveContext& context) const;
    void Load(NCellMaster::TLoadContext& context);
    void Save(NCypressServer::TBeginCopyContext& context) const;
    void Load(NCypressServer::TEndCopyContext& context);

    bool IsViolatedBy(const TMasterMemoryLimits& rhs) const;
    TMasterMemoryLimits GetViolatedBy(const TMasterMemoryLimits& rhs) const;

    bool operator==(const TMasterMemoryLimits& rhs) const;

    TMasterMemoryLimits& operator+=(const TMasterMemoryLimits& rhs);
    TMasterMemoryLimits& operator-=(const TMasterMemoryLimits& rhs);
    TMasterMemoryLimits& operator*=(i64 rhs);

    TMasterMemoryLimits operator+(const TMasterMemoryLimits& rhs) const;
    TMasterMemoryLimits operator-(const TMasterMemoryLimits& rhs) const;
    TMasterMemoryLimits operator*(i64 rhs) const;

    TMasterMemoryLimits operator-() const;

    i64 Total = 0;
    i64 ChunkHost = 0;
    THashMap<NObjectClient::TCellTag, i64> PerCell;
};

void FormatValue(TStringBuilderBase* builder, const TMasterMemoryLimits& limits, TStringBuf /*format*/);
TString ToString(const TMasterMemoryLimits& limits);

// NB: this serialization requires access to multicell manager and cannot be
// easily integrated into yson serialization framework.

void SerializeMasterMemoryLimits(
    const TMasterMemoryLimits& limits,
    NYson::IYsonConsumer* consumer,
    const NCellMaster::IMulticellManagerPtr& multicellManager);

void DeserializeMasterMemoryLimits(
    TMasterMemoryLimits& limits,
    NYTree::INodePtr node,
    const NCellMaster::IMulticellManagerPtr& multicellManager);

void SerializeViolatedMasterMemoryLimits(
    const TMasterMemoryLimits& violatedLimits,
    NYson::IYsonConsumer* consumer,
    const NCellMaster::IMulticellManagerPtr& multicellManager);

void SerializeViolatedMasterMemoryLimitsInBooleanFormat(
    const TMasterMemoryLimits& violatedLimits,
    NYson::IYsonConsumer* consumer,
    const NCellMaster::IMulticellManagerPtr& multicellManager);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSecurityServer

