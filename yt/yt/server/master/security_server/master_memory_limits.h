#pragma once

#include <yt/yt/server/master/cell_master/public.h>
#include <yt/yt/server/master/cypress_server/serialize.h>

#include <yt/yt/client/object_client/public.h>

#include <yt/yt/core/misc/default_map.h>
#include <yt/yt/core/misc/maybe_inf.h>

namespace NYT::NSecurityServer {

////////////////////////////////////////////////////////////////////////////////

// Represents the number of violated accounts in an account subtree.
struct TViolatedMasterMemoryLimits
{
    int Total = 0;
    int ChunkHost = 0;
    THashMap<NObjectServer::TCellTag, int> PerCell;
};

////////////////////////////////////////////////////////////////////////////////

struct TMasterMemoryLimits
{
    TLimit64 Total{i64(0)};
    TLimit64 ChunkHost{i64(0)};

    using TPerCellLimits = TDefaultMap<THashMap<NObjectServer::TCellTag, TLimit64>>;
    TPerCellLimits PerCell{/*defaultValue*/ TLimit64::Infinity()};

    bool IsViolatedBy(const TMasterMemoryLimits& that) const noexcept;
    TViolatedMasterMemoryLimits GetViolatedBy(const TMasterMemoryLimits& that) const;

    std::optional<TViolatedMasterMemoryLimits> CheckIncrease(const TMasterMemoryLimits& delta) const;
    std::optional<TViolatedMasterMemoryLimits> CheckDecrease(const TMasterMemoryLimits& delta) const;

    void Increase(const TMasterMemoryLimits& delta);
    void IncreaseWithInfinityAllowed(const TMasterMemoryLimits& that);
    void Decrease(const TMasterMemoryLimits& delta);

    static TMasterMemoryLimits Zero();
    static TMasterMemoryLimits Infinity();

    void Save(NCellMaster::TSaveContext& context) const;
    void Load(NCellMaster::TLoadContext& context);
    void Save(NCypressServer::TBeginCopyContext& context) const;
    void Load(NCypressServer::TEndCopyContext& context);

    bool operator==(const TMasterMemoryLimits& that) const noexcept = default;
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
    const NCellMaster::IMulticellManagerPtr& multicellManager,
    bool zeroByDefault);

void SerializeViolatedMasterMemoryLimits(
    const TViolatedMasterMemoryLimits& violatedLimits,
    NYson::IYsonConsumer* consumer,
    const NCellMaster::IMulticellManagerPtr& multicellManager);

void SerializeViolatedMasterMemoryLimitsInBooleanFormat(
    const TViolatedMasterMemoryLimits& violatedLimits,
    NYson::IYsonConsumer* consumer,
    const NCellMaster::IMulticellManagerPtr& multicellManager);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSecurityServer

