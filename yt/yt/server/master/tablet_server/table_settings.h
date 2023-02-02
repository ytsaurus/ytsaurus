#pragma once

#include "public.h"

#include <yt/yt/server/master/object_server/public.h>

#include <yt/yt/server/master/chunk_server/public.h>

#include <yt/yt/server/master/table_server/public.h>

#include <yt/yt/server/lib/tablet_node/table_settings.h>

namespace NYT::NTabletServer {

////////////////////////////////////////////////////////////////////////////////

struct TTableSettings
    : public NTabletNode::TRawTableSettings
{
    NTabletNode::TTableMountConfigPtr EffectiveMountConfig;
};

struct TSerializedTableSettings
{
    NYson::TYsonString MountConfig;
    NYson::TYsonString ExtraMountConfig;
    NYson::TYsonString StoreReaderConfig;
    NYson::TYsonString HunkReaderConfig;
    NYson::TYsonString StoreWriterConfig;
    NYson::TYsonString StoreWriterOptions;
    NYson::TYsonString HunkWriterConfig;
    NYson::TYsonString HunkWriterOptions;
    NYson::TYsonString GlobalPatch;
    NYson::TYsonString Experiments;
};

struct THunkStorageSettings
{
    NTabletNode::THunkStorageMountConfigPtr MountConfig;
    NTabletNode::THunkStoreWriterConfigPtr HunkStoreConfig;
    NTabletNode::THunkStoreWriterOptionsPtr HunkStoreOptions;
};

struct TSerializedHunkStorageSettings
{
    NYson::TYsonString MountConfig;
    NYson::TYsonString HunkStoreConfig;
    NYson::TYsonString HunkStoreOptions;
};

using TTabletOwnerSettings = std::variant<
    TTableSettings,
    THunkStorageSettings
>;

using TSerializedTabletOwnerSettings = std::variant<
    TSerializedTableSettings,
    TSerializedHunkStorageSettings
>;

////////////////////////////////////////////////////////////////////////////////

TTableSettings GetTableSettings(
    NTableServer::TTableNode* table,
    const NObjectServer::IObjectManagerPtr& objectManager,
    const NChunkServer::IChunkManagerPtr& chunkManager,
    const TDynamicTabletManagerConfigPtr& dynamicConfig);

TSerializedTableSettings SerializeTableSettings(const TTableSettings& tableSettings);

template <class TRequest>
void FillTableSettings(
    TRequest* request,
    const TSerializedTableSettings& serializedTableSettings);

THunkStorageSettings GetHunkStorageSettings(
    THunkStorageNode* hunkStorage,
    const NObjectServer::IObjectManagerPtr& objectManager,
    const NChunkServer::IChunkManagerPtr& chunkManager,
    const TDynamicTabletManagerConfigPtr& dynamicConfig);

TSerializedHunkStorageSettings SerializeHunkStorageSettings(const THunkStorageSettings& settings);

template <class TRequest>
void FillHunkStorageSettings(
    TRequest* request,
    const TSerializedHunkStorageSettings& settings);

TTabletOwnerSettings GetTabletOwnerSettings(
    TTabletOwnerBase* table,
    const NObjectServer::IObjectManagerPtr& objectManager,
    const NChunkServer::IChunkManagerPtr& chunkManager,
    const TDynamicTabletManagerConfigPtr& dynamicConfig);

TSerializedTabletOwnerSettings SerializeTabletOwnerSettings(const TTabletOwnerSettings& settings);

void ValidateTableMountConfig(
    const NTableServer::TTableNode* table,
    const NTabletNode::TTableMountConfigPtr& mountConfig,
    const TDynamicTabletManagerConfigPtr& dynamicConfig);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletServer

#define TABLE_SETTINGS_INL_H_
#include "table_settings-inl.h"
#undef TABLE_SETTINGS_INL_H_
