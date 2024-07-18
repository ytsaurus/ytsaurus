#pragma once

#include "public.h"
#include "master_memory_limits.h"

#include <yt/yt/server/master/cell_master/public.h>

#include <yt/yt/server/master/chunk_server/public.h>

#include <yt/yt/server/master/cypress_server/public.h>

#include <yt/yt/client/cell_master_client/public.h>

#include <library/cpp/yt/yson/public.h>

namespace NYT::NSecurityServer {

////////////////////////////////////////////////////////////////////////////////

class TClusterResourceLimits
{
public:
    static TClusterResourceLimits Infinity();
    static TClusterResourceLimits Zero();

    TClusterResourceLimits&& SetMediumDiskSpace(int mediumIndex, TLimit64 diskSpace) &&;
    void SetMediumDiskSpace(int mediumIndex, TLimit64 diskSpace) &;

    //! Increases medium disk space by a given amount.
    //! NB: the amount may be negative.
    void AddToMediumDiskSpace(int mediumIndex, i64 diskSpaceDelta);

    bool IsViolatedBy(const TClusterResourceLimits& rhs) const noexcept;
    TViolatedClusterResourceLimits GetViolatedBy(const TClusterResourceLimits& usage) const;

    using TDiskSpaceLimits = TDefaultMap<NChunkClient::TMediumMap<TLimit64>>;
    const TDiskSpaceLimits& DiskSpace() const;

    DEFINE_BYVAL_RW_PROPERTY_WITH_FLUENT_SETTER(TClusterResourceLimits, TLimit64, NodeCount);
    DEFINE_BYVAL_RW_PROPERTY_WITH_FLUENT_SETTER(TClusterResourceLimits, TLimit64, ChunkCount);
    DEFINE_BYVAL_RW_PROPERTY_WITH_FLUENT_SETTER(TClusterResourceLimits, TLimit32, TabletCount);
    DEFINE_BYVAL_RW_PROPERTY_WITH_FLUENT_SETTER(TClusterResourceLimits, TLimit64, TabletStaticMemory);

    TMasterMemoryLimits& MasterMemory();
    const TMasterMemoryLimits& MasterMemory() const;
    TClusterResourceLimits&& SetMasterMemory(TMasterMemoryLimits masterMemoryLimits) &&;
    void SetMasterMemory(TMasterMemoryLimits masterMemoryLimits) &;

    std::optional<TViolatedClusterResourceLimits> CheckIncrease(
        const TClusterResourceLimits& delta) const;
    std::optional<TViolatedClusterResourceLimits> CheckDecrease(
        const TClusterResourceLimits& delta) const;

    void Increase(const TClusterResourceLimits& delta);
    void Decrease(const TClusterResourceLimits& delta);

    // Used to calculate sum of limits.
    void IncreaseWithInfinityAllowed(const TClusterResourceLimits& that);

    struct TModification
    {
        std::optional<TViolatedClusterResourceLimits> (TClusterResourceLimits::*Check)(const TClusterResourceLimits& delta) const;
        void (TClusterResourceLimits::*Do)(const TClusterResourceLimits& delta);
    };

    static constexpr TModification IncreasingModification = {
        &TClusterResourceLimits::CheckIncrease,
        &TClusterResourceLimits::Increase,
    };

    static constexpr TModification DecreasingModification = {
        &TClusterResourceLimits::CheckDecrease,
        &TClusterResourceLimits::Decrease,
    };

public:
    void Save(NCellMaster::TSaveContext& context) const;
    void Load(NCellMaster::TLoadContext& context);

    void Save(NCypressServer::TBeginCopyContext& context) const;
    void Load(NCypressServer::TEndCopyContext& context);

    bool operator==(const TClusterResourceLimits& other) const noexcept = default;

private:
    TDiskSpaceLimits DiskSpace_{/*defaultValue*/ TLimit64(i64(0))};
    TMasterMemoryLimits MasterMemory_;
};

////////////////////////////////////////////////////////////////////////////////

class TViolatedClusterResourceLimits
{
public:
    DEFINE_BYVAL_RW_PROPERTY_WITH_FLUENT_SETTER(TViolatedClusterResourceLimits, i64, NodeCount);
    DEFINE_BYVAL_RW_PROPERTY_WITH_FLUENT_SETTER(TViolatedClusterResourceLimits, i64, ChunkCount);
    DEFINE_BYVAL_RW_PROPERTY_WITH_FLUENT_SETTER(TViolatedClusterResourceLimits, int, TabletCount);
    DEFINE_BYVAL_RW_PROPERTY_WITH_FLUENT_SETTER(TViolatedClusterResourceLimits, i64, TabletStaticMemory);

    TViolatedMasterMemoryLimits& MasterMemory();
    const TViolatedMasterMemoryLimits& MasterMemory() const;
    void SetMasterMemory(TViolatedMasterMemoryLimits masterMemoryLimits) &;

    NChunkClient::TMediumMap<i64>& DiskSpace();
    const NChunkClient::TMediumMap<i64>& DiskSpace() const;

    void SetMediumDiskSpace(int mediumIndex, i64 diskSpace) &;
    void AddToMediumDiskSpace(int mediumIndex, i64 diskSpaceDelta);

private:
    NChunkClient::TMediumMap<i64> DiskSpace_;
    TViolatedMasterMemoryLimits MasterMemory_;
};

// NB: this serialization requires access to chunk and multicell managers and
// cannot be easily integrated into yson serialization framework.

void SerializeClusterResourceLimits(
    const TClusterResourceLimits& resourceLimits,
    NYson::IYsonConsumer* consumer,
    const NCellMaster::TBootstrap* bootstrap,
    bool serializeDiskSpace);

void DeserializeClusterResourceLimits(
    TClusterResourceLimits& resourceLimits,
    NYTree::INodePtr node,
    const NCellMaster::TBootstrap* bootstrap,
    bool zeroByDefault);

void SerializeViolatedClusterResourceLimits(
    const TViolatedClusterResourceLimits& violatedResourceLimits,
    NYson::IYsonConsumer* consumer,
    const NCellMaster::TBootstrap* bootstrap);

void SerializeViolatedClusterResourceLimitsInCompactFormat(
    const TViolatedClusterResourceLimits& violatedResourceLimits,
    NYson::IYsonConsumer* consumer,
    const NCellMaster::TBootstrap* bootstrap);

void SerializeViolatedClusterResourceLimitsInBooleanFormat(
    const TViolatedClusterResourceLimits& violatedResourceLimits,
    NYson::IYsonConsumer* consumer,
    const NCellMaster::TBootstrap* bootstrap,
    bool serializeDiskSpace,
    std::optional<int> relevantMediumIndex = std::nullopt);

////////////////////////////////////////////////////////////////////////////////

void FormatValue(TStringBuilderBase* builder, const TClusterResourceLimits& resources, TStringBuf /*spec*/);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSecurityServer

