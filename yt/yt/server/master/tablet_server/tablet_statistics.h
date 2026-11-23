#pragma once

#include "public.h"

#include <yt/yt/server/master/chunk_server/public.h>

#include <yt/yt/server/master/cell_master/public.h>

#include <yt/yt/core/misc/aggregate_property.h>

#include <yt/yt/core/ytree/yson_struct.h>

#include <yt/yt/core/phoenix/type_decl.h>

namespace NYT::NTabletServer {

////////////////////////////////////////////////////////////////////////////////

struct TTabletCellStatisticsBase
{
    i64 UnmergedRowCount = 0;
    i64 UncompressedDataSize = 0;
    i64 CompressedDataSize = 0;
    i64 HunkUncompressedDataSize = 0;
    i64 HunkCompressedDataSize = 0;
    i64 MemorySize = 0;
    i64 DynamicMemoryPoolSize = 0;
    NChunkClient::TCompactMediumMap<i64> DiskSpacePerMedium;
    int ChunkCount = 0;
    int PartitionCount = 0;
    int StoreCount = 0;
    int PreloadPendingStoreCount = 0;
    int PreloadCompletedStoreCount = 0;
    int PreloadFailedStoreCount = 0;
    int TabletCount = 0;
    TEnumIndexedArray<NTabletClient::EInMemoryMode, int> TabletCountPerMemoryMode;

    using TSaveContext = NCellMaster::TSaveContext;
    using TLoadContext = NCellMaster::TLoadContext;

    PHOENIX_DECLARE_TYPE(TTabletCellStatisticsBase, 0xb09150a6);
};

struct TTabletCellStatistics
    : public TTabletCellStatisticsBase
{
    using TSaveContext = NCellMaster::TSaveContext;
    using TLoadContext = NCellMaster::TLoadContext;

    PHOENIX_DECLARE_TYPE(TTabletCellStatistics, 0x490aba59);
};

struct TTabletStatisticsBase
{
    int OverlappingStoreCount = 0;

    using TSaveContext = NCellMaster::TSaveContext;
    using TLoadContext = NCellMaster::TLoadContext;

    PHOENIX_DECLARE_TYPE(TTabletStatisticsBase, 0x645b3c05);
};

struct TTabletStatistics
    : public TTabletCellStatisticsBase
    , public TTabletStatisticsBase
{
    using TSaveContext = NCellMaster::TSaveContext;
    using TLoadContext = NCellMaster::TLoadContext;

    PHOENIX_DECLARE_TYPE(TTabletStatistics, 0xf11f1c0c);
};

class TTabletStatisticsAggregate
    : public TNonCopyable
{
public:
    TTabletStatistics Get() const;

    void Account(const TTabletStatistics& tabletStatistics);
    void Discount(const TTabletStatistics& tabletStatistics);
    void AccountDelta(const TTabletStatistics& tabletStatistics);

    void Reset();

private:
    TSumAggregate<TTabletStatistics> CellStatistics_;
    TMaxAggregate<int> OverlappingStoreCount_{0};

    using TSaveContext = NCellMaster::TSaveContext;
    using TLoadContext = NCellMaster::TLoadContext;

    PHOENIX_DECLARE_TYPE(TTabletStatisticsAggregate, 0x21c3d803);
};

////////////////////////////////////////////////////////////////////////////////

TTabletCellStatisticsBase& operator+=(TTabletCellStatisticsBase& lhs, const TTabletCellStatisticsBase& rhs);
TTabletCellStatisticsBase  operator +  (const TTabletCellStatisticsBase& lhs, const TTabletCellStatisticsBase& rhs);

TTabletCellStatisticsBase& operator-=(TTabletCellStatisticsBase& lhs, const TTabletCellStatisticsBase& rhs);
TTabletCellStatisticsBase  operator -  (const TTabletCellStatisticsBase& lhs, const TTabletCellStatisticsBase& rhs);

bool operator==(const TTabletCellStatisticsBase& lhs, const TTabletCellStatisticsBase& rhs);

TTabletStatistics& operator+=(TTabletStatistics& lhs, const TTabletStatistics& rhs);
TTabletStatistics  operator +  (const TTabletStatistics& lhs, const TTabletStatistics& rhs);

TTabletStatistics& operator+=(TTabletStatistics& lhs, const TTabletStatistics& rhs);
TTabletStatistics  operator +  (const TTabletStatistics& lhs, const TTabletStatistics& rhs);

bool operator==(const TTabletStatistics& lhs, const TTabletStatistics& rhs);

void ToProto(NProto::TTabletCellStatistics* protoStatistics, const TTabletCellStatistics& statistics);
void FromProto(TTabletCellStatistics* statistics, const NProto::TTabletCellStatistics& protoStatistics);

std::string ToString(const TTabletStatistics& statistics, const NChunkServer::IChunkManagerPtr& chunkManager);

////////////////////////////////////////////////////////////////////////////////

// COMPAT(akozhikhov)
class TSerializableTabletCellStatisticsBase
    : public virtual NYTree::TYsonStruct
    , public TTabletCellStatisticsBase
{
public:
    TSerializableTabletCellStatisticsBase(
        const TTabletCellStatisticsBase& statistics,
        const NChunkServer::IChunkManagerPtr& chunkManager);

    REGISTER_YSON_STRUCT(TSerializableTabletCellStatisticsBase);

    static void Register(TRegistrar registrar);

public:
    // Resolve ambiguity with Phoenix-generated persistence methods of the statistics base.
    using NYTree::TYsonStruct::Save;
    using NYTree::TYsonStruct::Load;

private:
    i64 DiskSpace_ = 0;
    THashMap<std::string, i64> DiskSpacePerMediumMap_;
};

class TSerializableTabletStatisticsBase
    : public virtual NYTree::TYsonStruct
    , public TTabletStatisticsBase
{
public:
    explicit TSerializableTabletStatisticsBase(const TTabletStatisticsBase& statistics);

    REGISTER_YSON_STRUCT(TSerializableTabletStatisticsBase);

    static void Register(TRegistrar registrar);

public:
    // Resolve ambiguity with Phoenix-generated persistence methods of the statistics base.
    using NYTree::TYsonStruct::Save;
    using NYTree::TYsonStruct::Load;

private:
    void InitParameters();
};

struct TSerializableTabletCellStatistics
    : public TSerializableTabletCellStatisticsBase
{
    TSerializableTabletCellStatistics(
        const TTabletCellStatistics& statistics,
        const NChunkServer::IChunkManagerPtr& chunkManager);

    REGISTER_YSON_STRUCT(TSerializableTabletCellStatistics);

    static void Register(TRegistrar)
    { }

public:
    // Resolve ambiguity with Phoenix-generated persistence methods of the statistics base.
    using NYTree::TYsonStruct::Save;
    using NYTree::TYsonStruct::Load;
};

struct TSerializableTabletStatistics
    : public TSerializableTabletCellStatisticsBase
    , public TSerializableTabletStatisticsBase
{
    TSerializableTabletStatistics(
        const TTabletStatistics& statistics,
        const NChunkServer::IChunkManagerPtr& chunkManager);

    REGISTER_YSON_STRUCT(TSerializableTabletStatistics);

    static void Register(TRegistrar)
    { }

public:
    // Resolve ambiguity with Phoenix-generated persistence methods of the statistics base.
    using NYTree::TYsonStruct::Save;
    using NYTree::TYsonStruct::Load;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletServer
