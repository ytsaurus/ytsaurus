#pragma once

#include "public.h"

#include <yt/server/master/cell_master/public.h>

#include <yt/server/master/chunk_server/public.h>

#include <yt/server/master/cypress_server/public.h>

#include <yt/core/yson/public.h>

#include <yt/core/ytree/yson_serializable.h>

#include <yt/client/cell_master_client/public.h>

namespace NYT::NSecurityServer {

////////////////////////////////////////////////////////////////////////////////

class TClusterResourceLimits
{
public:
    TClusterResourceLimits();

    //! Gets infinite resource limits.
    static TClusterResourceLimits Infinite();

    //! Gets zero resource limits.
    static TClusterResourceLimits Zero(const NCellMaster::TMulticellManagerPtr& multicellManager);

    //! Sets node count.
    TClusterResourceLimits&& SetNodeCount(i64 nodeCount) &&;

    //! Sets chunk count.
    TClusterResourceLimits&& SetChunkCount(i64 chunkCount) &&;

    //! Sets tablet count.
    TClusterResourceLimits&& SetTabletCount(int tabletCount) &&;

    //! Sets tablet static memory size.
    TClusterResourceLimits&& SetTabletStaticMemory(i64 tabletStaticMemory) &&;

    //! Sets master memory.
    TClusterResourceLimits&& SetMasterMemory(i64 masterMemory) &&;
    void SetMasterMemory(NObjectServer::TCellTag cellTag, i64 masterMemory);

    //! Sets chunk host master memory.
    TClusterResourceLimits&& SetChunkHostMasterMemory(i64 chunkHostMasterMemory) &&;

    //! Adds master memory for a given cell tag.
    void AddMasterMemory(NObjectServer::TCellTag cellTag, i64 masterMemory);

    //! Removes master memory limit for a given cell tag.
    void RemoveCellMasterMemoryEntry(NObjectServer::TCellTag cellTag);

    //! Sets medium disk space.
    TClusterResourceLimits&& SetMediumDiskSpace(int mediumIndex, i64 diskSpace) &&;
    void SetMediumDiskSpace(int mediumIndex, i64 diskSpace) &;

    //! Increases medium disk space by a given amount.
    //! NB: the amount may be negative.
    void AddToMediumDiskSpace(int mediumIndex, i64 diskSpaceDelta);

    bool IsViolatedBy(const TClusterResourceLimits& rhs) const;

    // TODO(shakurov): introduce a separate TViolatedResourceLimits type.
    using TViolatedResourceLimits = TClusterResourceLimits;
    TViolatedResourceLimits GetViolatedBy(const TClusterResourceLimits& usage) const;

    const NChunkClient::TMediumMap<i64>& DiskSpace() const;
    const THashMap<NObjectServer::TCellTag, i64>& CellMasterMemoryLimits() const;

    // TODO(aleksandra-zh): move to private.
    i64 NodeCount;
    i64 ChunkCount;
    int TabletCount;
    i64 TabletStaticMemory;

    i64 MasterMemory;
    i64 ChunkHostMasterMemory;

    void Save(NCellMaster::TSaveContext& context) const;
    void Load(NCellMaster::TLoadContext& context);

    void Save(NCypressServer::TBeginCopyContext& context) const;
    void Load(NCypressServer::TEndCopyContext& context);

private:
    NChunkClient::TMediumMap<i64> DiskSpace_;
    THashMap<NObjectServer::TCellTag, i64> CellMasterMemoryLimits_;
};

////////////////////////////////////////////////////////////////////////////////

//! A helper for (de)serializing TClusterResourceLimits.
//! This cannot be done directly as serialization requires converting medium
//! indexes to names, which is impossible without the chunk manager.
class TSerializableClusterResourceLimits
    : public NYTree::TYsonSerializable
{
public:
    // For deserialization.
    explicit TSerializableClusterResourceLimits(bool serializeDiskSpace = true);
    // For serialization.
    TSerializableClusterResourceLimits(
        const NChunkServer::TChunkManagerPtr& chunkManager,
        const NCellMaster::TMulticellManagerPtr& multicellManager,
        const TClusterResourceLimits& clusterResources,
        bool serializeDiskSpace = true);

    TClusterResourceLimits ToClusterResourceLimits(
        const NChunkServer::TChunkManagerPtr& chunkManager,
        const NCellMaster::TMulticellManagerPtr& multicellManager) const;

    void AddToMediumDiskSpace(const TString& mediumName, i64 mediumDiskSpace);

private:
    i64 NodeCount_ = 0;
    i64 ChunkCount_ = 0;
    int TabletCount_ = 0;
    i64 TabletStaticMemory_ = 0;
    THashMap<TString, i64> DiskSpacePerMedium_;
    // COMPAT(shakurov)
    i64 DiskSpace_;
    
    struct TSerializableMasterMemoryLimits
        : public NYTree::TYsonSerializable
    {
        TSerializableMasterMemoryLimits();

        i64 Total = 0;
        i64 ChunkHost = 0;
        THashMap<TString, i64> PerCell;
    };
    using TSerializableMasterMemoryLimitsPtr = TIntrusivePtr<TSerializableMasterMemoryLimits>;
    TSerializableMasterMemoryLimitsPtr MasterMemory_;
};

DEFINE_REFCOUNTED_TYPE(TSerializableClusterResourceLimits)

////////////////////////////////////////////////////////////////////////////////

TClusterResourceLimits& operator += (TClusterResourceLimits& lhs, const TClusterResourceLimits& rhs);
TClusterResourceLimits  operator +  (const TClusterResourceLimits& lhs, const TClusterResourceLimits& rhs);

TClusterResourceLimits& operator -= (TClusterResourceLimits& lhs, const TClusterResourceLimits& rhs);
TClusterResourceLimits  operator -  (const TClusterResourceLimits& lhs, const TClusterResourceLimits& rhs);

TClusterResourceLimits& operator *= (TClusterResourceLimits& lhs, i64 rhs);
TClusterResourceLimits  operator *  (const TClusterResourceLimits& lhs, i64 rhs);

TClusterResourceLimits  operator -  (const TClusterResourceLimits& resources);

bool operator == (const TClusterResourceLimits& lhs, const TClusterResourceLimits& rhs);
bool operator != (const TClusterResourceLimits& lhs, const TClusterResourceLimits& rhs);

void FormatValue(TStringBuilderBase* builder, const TClusterResourceLimits& resources, TStringBuf /*format*/);
TString ToString(const TClusterResourceLimits& resources);

////////////////////////////////////////////////////////////////////////////////

//! A helper for serializing TClusterResourceLimits as violated resource limits.
// TODO(shakurov): introduce an actual TViolatedClusterResourceLimits and use it here.
class TSerializableViolatedClusterResourceLimits
    : public NYTree::TYsonSerializable
{
public:
    TSerializableViolatedClusterResourceLimits(
        const NChunkServer::TChunkManagerPtr& chunkManager,
        const NCellMaster::TMulticellManagerPtr& multicellManager,
        const TClusterResourceLimits& violatedResourceLimits);

private:
    bool NodeCount_ = 0;
    bool ChunkCount_ = 0;
    bool TabletCount_ = 0;
    bool TabletStaticMemory_ = 0;
    THashMap<TString, bool> DiskSpacePerMedium_;

    struct TSerializableViolatedMasterMemoryLimits
        : public NYTree::TYsonSerializable
    {
        explicit TSerializableViolatedMasterMemoryLimits();

        bool Total = 0;
        bool ChunkHost = 0;
        THashMap<TString, bool> PerCell;
    };
    using TSerializableViolatedMasterMemoryLimitsPtr = TIntrusivePtr<TSerializableViolatedMasterMemoryLimits>;
    TSerializableViolatedMasterMemoryLimitsPtr MasterMemory_;
};

DEFINE_REFCOUNTED_TYPE(TSerializableViolatedClusterResourceLimits)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSecurityServer

