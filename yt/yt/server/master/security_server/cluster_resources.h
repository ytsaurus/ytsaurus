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

#define FOR_EACH_CLUSTER_RESOURCE(XX) \
    XX(NodeCount) \
    XX(ChunkCount) \
    XX(TabletCount) \
    XX(TabletStaticMemory) \
    XX(ChunkHostCellMasterMemory) \
    XX(DetailedMasterMemory)

//! Cluster resources occupied by a particular user or object.
class TClusterResources
{
public:
    TClusterResources();

    TClusterResources&& SetDetailedMasterMemory(const TDetailedMasterMemory&) &&;
    TClusterResources&& SetDetailedMasterMemory(EMasterMemoryType type, i64 masterMemory) &&;

    TClusterResources&& SetMediumDiskSpace(int mediumIndex, i64 diskSpace) &&;
    void SetMediumDiskSpace(int mediumIndex, i64 diskSpace) &;

    i64 GetMediumDiskSpace(int mediumIndex) const;

    //! Increases medium disk space by a given amount.
    //! NB: the amount may be negative.
    void AddToMediumDiskSpace(int mediumIndex, i64 diskSpaceDelta);

    //! Completely empties disk space counts for all media.
    void ClearDiskSpace();

    //! Sets master memory usage to zero.
    void ClearMasterMemory();

    const NChunkClient::TMediumMap<i64>& DiskSpace() const;

    i64 GetTotalMasterMemory() const;


    using TMediumDiskSpace = std::pair<const NChunkServer::TMedium*, i64>;
    using TMediaDiskSpace = TCompactVector<TMediumDiskSpace, 4>;

    TMediaDiskSpace GetPatchedDiskSpace(
        const NChunkServer::IChunkManagerPtr& chunkManager,
        const TCompactVector<int, 4>& additionalMediumIndexes) const;

    DEFINE_BYVAL_RW_PROPERTY_WITH_FLUENT_SETTER(TClusterResources, i64, NodeCount);
    DEFINE_BYVAL_RW_PROPERTY_WITH_FLUENT_SETTER(TClusterResources, i64, ChunkCount);
    DEFINE_BYVAL_RW_PROPERTY_WITH_FLUENT_SETTER(TClusterResources, int, TabletCount);
    DEFINE_BYVAL_RW_PROPERTY_WITH_FLUENT_SETTER(TClusterResources, i64, TabletStaticMemory);
    DEFINE_BYVAL_RW_PROPERTY_WITH_FLUENT_SETTER(TClusterResources, i64, ChunkHostCellMasterMemory);

    DEFINE_BYREF_RW_PROPERTY(TDetailedMasterMemory, DetailedMasterMemory);

public:
    TClusterResources& operator += (const TClusterResources& other);
    TClusterResources operator + (const TClusterResources& other) const;

    TClusterResources& operator -= (const TClusterResources& rhs);
    TClusterResources operator - (const TClusterResources& rhs) const;

    TClusterResources& operator *= (i64 rhs);
    TClusterResources operator * (i64 rhs) const;

    TClusterResources operator - () const;

    #define XX(Name) void Increase##Name(NMpl::TCallTraits<decltype(Name##_)>::TType delta);
    FOR_EACH_CLUSTER_RESOURCE(XX)
    #undef XX

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

void ToProto(NProto::TClusterResources* protoResources, const TClusterResources& resources);
void FromProto(TClusterResources* resources, const NProto::TClusterResources& protoResources);

void FormatValue(TStringBuilderBase* builder, const TClusterResources& resources, TStringBuf /*format*/);
TString ToString(const TClusterResources& resources);

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

TRichClusterResources& operator += (TRichClusterResources& lhs, const TRichClusterResources& rhs);
TRichClusterResources  operator +  (const TRichClusterResources& lhs, const TRichClusterResources& rhs);

////////////////////////////////////////////////////////////////////////////////

// NB: this serialization requires access to chunk and multicell managers and
// cannot be easily integrated into yson serialization framework.

void SerializeClusterResources(
    const TClusterResources& resources,
    NYson::IYsonConsumer* consumer,
    const NCellMaster::TBootstrap* bootstrap);

void DeserializeClusterResources(
    TClusterResources& clusterResources,
    NYTree::INodePtr node,
    const NCellMaster::TBootstrap* bootstrap);

void SerializeRichClusterResources(
    const TRichClusterResources& resources,
    NYson::IYsonConsumer* consumer,
    const NCellMaster::TBootstrap* bootstrap);

void DeserializeRichClusterResources(
    TRichClusterResources& clusterResources,
    NYTree::INodePtr node,
    const NCellMaster::TBootstrap* bootstrap);

void SerializeAccountClusterResourceUsage(
    const TAccount* account,
    bool committed,
    bool recursive,
    NYson::IYsonConsumer* consumer,
    const NCellMaster::TBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

NTabletServer::TTabletResources ConvertToTabletResources(
    const TClusterResources& clusterResources);
TClusterResources ConvertToClusterResources(
    const NTabletServer::TTabletResources& tabletResources);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSecurityServer
