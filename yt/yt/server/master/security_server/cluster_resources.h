#pragma once

#include "public.h"
#include "detailed_master_memory.h"

#include <yt/yt/server/master/cell_master/public.h>

#include <yt/yt/server/master/chunk_server/public.h>

#include <yt/yt/server/master/cypress_server/public.h>

#include <yt/yt/server/master/tablet_server/tablet_resources.h>

#include <yt/yt/core/yson/public.h>

#include <yt/yt/core/ytree/yson_serializable.h>

namespace NYT::NSecurityServer {

////////////////////////////////////////////////////////////////////////////////

//! Cluster resources occupied by a particular user or object.
class TClusterResources
{
public:
    TClusterResources();

    TClusterResources&& SetDetailedMasterMemory(const TDetailedMasterMemory&) &&;
    TClusterResources&& SetDetailedMasterMemory(EMasterMemoryType type, i64 masterMemory) &&;

    TClusterResources&& SetMediumDiskSpace(int mediumIndex, i64 diskSpace) &&;
    void SetMediumDiskSpace(int mediumIndex, i64 diskSpace) &;

    //! Increases medium disk space by a given amount.
    //! NB: the amount may be negative.
    void AddToMediumDiskSpace(int mediumIndex, i64 diskSpaceDelta);

    //! Completely empties disk space counts for all media.
    void ClearDiskSpace();

    const NChunkClient::TMediumMap<i64>& DiskSpace() const;

    i64 GetTotalMasterMemory() const;

    DEFINE_BYVAL_RW_PROPERTY_WITH_FLUENT_SETTER(TClusterResources, i64, NodeCount);
    DEFINE_BYVAL_RW_PROPERTY_WITH_FLUENT_SETTER(TClusterResources, i64, ChunkCount);
    DEFINE_BYVAL_RW_PROPERTY_WITH_FLUENT_SETTER(TClusterResources, int, TabletCount);
    DEFINE_BYVAL_RW_PROPERTY_WITH_FLUENT_SETTER(TClusterResources, i64, TabletStaticMemory);

    DEFINE_BYREF_RW_PROPERTY(TDetailedMasterMemory, DetailedMasterMemory);

public:
    TClusterResources& operator += (const TClusterResources& other);
    TClusterResources operator + (const TClusterResources& other) const;

    TClusterResources& operator -= (const TClusterResources& rhs);
    TClusterResources operator - (const TClusterResources& rhs) const;

    TClusterResources& operator *= (i64 rhs); 
    TClusterResources operator * (i64 rhs) const;

    TClusterResources operator - () const;

    bool operator == (const TClusterResources& rhs) const;

    void Save(NCellMaster::TSaveContext& context) const;
    void Load(NCellMaster::TLoadContext& context);

    void Save(NCypressServer::TBeginCopyContext& context) const;
    void Load(NCypressServer::TEndCopyContext& context);

private:
    //! Space occupied on data nodes in bytes per medium.
    /*!
     *  This takes replication into account. At intermediate stages
     *  the actual space may be different.
     *
     *  Zero disk space for a medium is considered equivalent to that medium
     *  missing an entry in this map. In particular, setting zero disk space for
     *  a medium leads to erasing it from the map altogether.
     */
    NChunkClient::TMediumMap<i64> DiskSpace_;
};

////////////////////////////////////////////////////////////////////////////////

//! A helper for (de)serializing TClusterResources.
//! This cannot be done directly as serialization requires converting medium
//! indexes to names, which is impossible without the chunk manager.
class TSerializableClusterResources
    : public virtual NYTree::TYsonSerializable
{
public:
    // For deserialization.
    explicit TSerializableClusterResources(bool serializeTabletResources = true);
    // For serialization.
    TSerializableClusterResources(
        const NChunkServer::TChunkManagerPtr& chunkManager,
        const TClusterResources& clusterResources,
        bool serializeTabletResources = true);

    TClusterResources ToClusterResources(const NChunkServer::TChunkManagerPtr& chunkManager) const;

    void AddToMediumDiskSpace(const TString& mediumName, i64 mediumDiskSpace);

private:
    i64 NodeCount_ = 0;
    i64 ChunkCount_ = 0;
    int TabletCount_ = 0;
    i64 TabletStaticMemory_ = 0;
    THashMap<TString, i64> DiskSpacePerMedium_;
    // COMPAT(shakurov)
    i64 DiskSpace_;
    i64 MasterMemory_ = 0;
    TDetailedMasterMemory DetailedMasterMemory_;
};

DEFINE_REFCOUNTED_TYPE(TSerializableClusterResources)

////////////////////////////////////////////////////////////////////////////////

//! Both generic and tablet resources combined into a single class.
//! Used for serialization purposes and should not be saved to snapshot.
class TRichClusterResources
{
public:
    TRichClusterResources() = default;

    TRichClusterResources(
        const TClusterResources& clusterResources,
        const NTabletServer::TTabletResources& tabletResources);

    // TODO(ifsmirnov): use inheritance instead of composition
    // when TClusterResources::TabletStaticMemory and ::TabletCount vanish.
    TClusterResources ClusterResources;
    NTabletServer::TTabletResources TabletResources;
};

////////////////////////////////////////////////////////////////////////////////

//! Helper for serializing rich cluster resources.
class TSerializableRichClusterResources
    : public TSerializableClusterResources
    , public NTabletServer::TSerializableTabletResources
{
public:
    TSerializableRichClusterResources();

    TSerializableRichClusterResources(
        const NChunkServer::TChunkManagerPtr& chunkManager,
        const TRichClusterResources& richClusterResources);

    TRichClusterResources ToRichClusterResources(const NChunkServer::TChunkManagerPtr& chunkManager) const;
};

DEFINE_REFCOUNTED_TYPE(TSerializableRichClusterResources)

////////////////////////////////////////////////////////////////////////////////

void ToProto(NProto::TClusterResources* protoResources, const TClusterResources& resources);
void FromProto(TClusterResources* resources, const NProto::TClusterResources& protoResources);

void FormatValue(TStringBuilderBase* builder, const TClusterResources& resources, TStringBuf /*format*/);
TString ToString(const TClusterResources& resources);

////////////////////////////////////////////////////////////////////////////////

TRichClusterResources& operator += (TRichClusterResources& lhs, const TRichClusterResources& rhs);
TRichClusterResources  operator +  (const TRichClusterResources& lhs, const TRichClusterResources& rhs);

////////////////////////////////////////////////////////////////////////////////

NTabletServer::TTabletResources ConvertToTabletResources(
    const TClusterResources& clusterResources);
TClusterResources ConvertToClusterResources(
    const NTabletServer::TTabletResources& tabletResources);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSecurityServer
