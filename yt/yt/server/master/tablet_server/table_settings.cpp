#include "table_settings.h"

#include "config.h"
#include "helpers.h"
#include "hunk_storage_node.h"
#include "mount_config_storage.h"
#include "public.h"
#include "tablet_owner_base.h"

#include <yt/yt/server/master/chunk_server/chunk_manager.h>

#include <yt/yt/server/master/object_server/object_manager.h>

#include <yt/yt/server/master/table_server/table_node.h>
#include <yt/yt/server/master/table_server/table_node_proxy.h>

#include <yt/yt/server/lib/tablet_node/config.h>

#include <yt/yt/server/lib/misc/interned_attributes.h>

#include <yt/yt/ytlib/tablet_client/config.h>

#include <yt/yt/client/table_client/helpers.h>

#include <library/cpp/yt/misc/variant.h>

namespace NYT::NTabletServer {

using namespace NCellMaster;
using namespace NChunkServer;
using namespace NObjectClient;
using namespace NObjectServer;
using namespace NTableClient;
using namespace NTableServer;
using namespace NTabletClient;
using namespace NTabletNode;
using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

TTableSettings GetTableSettings(
    TTableNode* table,
    const IObjectManagerPtr& objectManager,
    const IChunkManagerPtr& chunkManager,
    const TDynamicTabletManagerConfigPtr& dynamicConfig)
{
    TTableSettings result;

    auto tableProxy = objectManager->GetProxy(table);
    const auto& tableAttributes = tableProxy->Attributes();

    // Parse and prepare mount config.
    try {
        // Handle builtin attributes.
        auto builtinMountConfig = ConvertTo<TBuiltinTableMountConfigPtr>(tableAttributes);
        if (!table->GetProfilingMode()) {
            builtinMountConfig->ProfilingMode = dynamicConfig->DynamicTableProfilingMode;
        }
        builtinMountConfig->EnableDynamicStoreRead =
            IsDynamicStoreReadEnabled(table, dynamicConfig);

        // Extract custom attributes and build combined node.
        auto combinedConfigNode = ConvertTo<IMapNodePtr>(builtinMountConfig);

        if (const auto* storage = table->FindMountConfigStorage();
            storage && !storage->IsEmpty())
        {
            auto [customConfigNode, unrecognizedCustomConfigNode] = storage->GetRecognizedConfig();

            if (unrecognizedCustomConfigNode->GetChildCount() > 0) {
                result.Provided.ExtraMountConfig = unrecognizedCustomConfigNode;
            }

            combinedConfigNode = PatchNode(combinedConfigNode, customConfigNode)->AsMap();
        }

        // The next line is important for validation.
        result.EffectiveMountConfig = ConvertTo<TTableMountConfigPtr>(combinedConfigNode);

        result.Provided.MountConfigNode = combinedConfigNode;
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Error parsing table mount configuration")
            << ex;
    }

    // Parse and prepare store reader config.
    try {
        result.Provided.StoreReaderConfig = UpdateYsonStruct(
            dynamicConfig->StoreChunkReader,
            // TODO(babenko): rename to store_chunk_reader
            tableAttributes.FindYson(EInternedAttributeKey::ChunkReader.Unintern()));
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Error parsing store reader config")
            << ex;
    }

    // Parse and prepare hunk reader config.
    try {
        result.Provided.HunkReaderConfig = UpdateYsonStruct(
            dynamicConfig->HunkChunkReader,
            tableAttributes.FindYson(EInternedAttributeKey::HunkChunkReader.Unintern()));
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Error parsing hunk reader config")
            << ex;
    }

    const auto& chunkReplication = table->Replication();
    auto primaryMediumIndex = table->GetPrimaryMediumIndex();
    auto* primaryMedium = chunkManager->GetMediumByIndex(primaryMediumIndex);
    auto replicationFactor = chunkReplication.Get(primaryMediumIndex).GetReplicationFactor();

    try {
        // Prepare store writer options.
        result.Provided.StoreWriterOptions = New<NTabletNode::TTabletStoreWriterOptions>();
        result.Provided.StoreWriterOptions->ReplicationFactor = replicationFactor;
        result.Provided.StoreWriterOptions->MediumName = primaryMedium->GetName();
        result.Provided.StoreWriterOptions->Account = table->GetAccount()->GetName();
        result.Provided.StoreWriterOptions->CompressionCodec = table->GetCompressionCodec();
        result.Provided.StoreWriterOptions->ErasureCodec = table->GetErasureCodec();
        result.Provided.StoreWriterOptions->EnableStripedErasure = table->GetEnableStripedErasure();
        result.Provided.StoreWriterOptions->ChunksVital = chunkReplication.GetVital();
        result.Provided.StoreWriterOptions->OptimizeFor = table->GetOptimizeFor();
        result.Provided.StoreWriterOptions->ChunkFormat = table->TryGetChunkFormat();
        result.Provided.StoreWriterOptions->SingleColumnGroupByDefault = result.EffectiveMountConfig->SingleColumnGroupByDefault;
        result.Provided.StoreWriterOptions->EnableSegmentMetaInBlocks = result.EffectiveMountConfig->EnableSegmentMetaInBlocks;
        if (result.Provided.StoreWriterOptions->ChunkFormat) {
            ValidateTableChunkFormatVersioned(*result.Provided.StoreWriterOptions->ChunkFormat, /*versioned*/ true);
        }
        result.Provided.StoreWriterOptions->Postprocess();
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Error preparing store writer options")
            << ex;
    }

    try {
        // Prepare hunk writer options.
        result.Provided.HunkWriterOptions = New<NTabletNode::TTabletHunkWriterOptions>();
        result.Provided.HunkWriterOptions->ReplicationFactor = replicationFactor;
        result.Provided.HunkWriterOptions->MediumName = primaryMedium->GetName();
        result.Provided.HunkWriterOptions->Account = table->GetAccount()->GetName();
        result.Provided.HunkWriterOptions->CompressionCodec = table->GetCompressionCodec();
        result.Provided.HunkWriterOptions->ErasureCodec = table->GetHunkErasureCodec();
        result.Provided.HunkWriterOptions->ChunksVital = chunkReplication.GetVital();
        result.Provided.HunkWriterOptions->Postprocess();
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Error preparing hunk writer options")
            << ex;
    }

    // Parse and prepare store writer config.
    try {
        auto config = CloneYsonStruct(dynamicConfig->StoreChunkWriter);
        if (primaryMedium->IsDomestic()) {
            const auto& mediumConfig = primaryMedium->AsDomestic()->Config();
            config->PreferLocalHost = mediumConfig->PreferLocalHostForDynamicTables;
        }
        if (dynamicConfig->IncreaseUploadReplicationFactor ||
            table->TabletCellBundle()->GetDynamicOptions()->IncreaseUploadReplicationFactor)
        {
            config->UploadReplicationFactor = replicationFactor;
        }

        result.Provided.StoreWriterConfig = UpdateYsonStruct(
            config,
            // TODO(babenko): rename to store_chunk_writer
            tableAttributes.FindYson(EInternedAttributeKey::ChunkWriter.Unintern()));
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Error preparing store writer config")
            << ex;
    }

    // Parse and prepare hunk writer config.
    try {
        auto config = CloneYsonStruct(dynamicConfig->HunkChunkWriter);
        if (primaryMedium->IsDomestic()) {
            const auto& mediumConfig = primaryMedium->AsDomestic()->Config();
            config->PreferLocalHost = mediumConfig->PreferLocalHostForDynamicTables;
        }
        config->UploadReplicationFactor = replicationFactor;

        result.Provided.HunkWriterConfig = UpdateYsonStruct(
            config,
            tableAttributes.FindYson(EInternedAttributeKey::HunkChunkWriter.Unintern()));
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Error preparing hunk writer config")
            << ex;
    }

    // Set global patch and experiments.
    result.GlobalPatch = CloneYsonStruct(
        ConvertTo<TTableConfigPatchPtr>(dynamicConfig));
    result.Experiments = dynamicConfig->TableConfigExperiments;

    return result;
}

TSerializedTableSettings SerializeTableSettings(const TTableSettings& tableSettings)
{
    return {
        .MountConfig = ConvertToYsonString(tableSettings.Provided.MountConfigNode),
        .ExtraMountConfig = tableSettings.Provided.ExtraMountConfig
            ? ConvertToYsonString(tableSettings.Provided.ExtraMountConfig)
            : TYsonString{},
        .StoreReaderConfig = ConvertToYsonString(tableSettings.Provided.StoreReaderConfig),
        .HunkReaderConfig = ConvertToYsonString(tableSettings.Provided.HunkReaderConfig),
        .StoreWriterConfig = ConvertToYsonString(tableSettings.Provided.StoreWriterConfig),
        .StoreWriterOptions = ConvertToYsonString(tableSettings.Provided.StoreWriterOptions),
        .HunkWriterConfig = ConvertToYsonString(tableSettings.Provided.HunkWriterConfig),
        .HunkWriterOptions = ConvertToYsonString(tableSettings.Provided.HunkWriterOptions),
        .GlobalPatch = ConvertToYsonString(tableSettings.GlobalPatch),
        .Experiments = ConvertToYsonString(tableSettings.Experiments),
    };
}

THunkStorageSettings GetHunkStorageSettings(
    THunkStorageNode* hunkStorage,
    const IObjectManagerPtr& objectManager,
    const IChunkManagerPtr& chunkManager,
    const TDynamicTabletManagerConfigPtr& dynamicConfig)
{
    THunkStorageSettings result;

    auto hunkStorageProxy = objectManager->GetProxy(hunkStorage);
    const auto& tableAttributes = hunkStorageProxy->Attributes();

    // Parse and prepare mount config.
    try {
        result.MountConfig = ConvertTo<NTabletNode::THunkStorageMountConfigPtr>(tableAttributes);
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Error parsing hunk storage mount configuration")
            << ex;
    }

    // Parse and prepare store writer config.
    try {
        result.HunkStoreConfig = UpdateYsonStruct(
            dynamicConfig->HunkStoreWriter,
            tableAttributes.FindYson(EInternedAttributeKey::HunkStoreWriter.Unintern()));
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Error parsing hunk store writer config")
            << ex;
    }

    // Prepare store writer options.
    try {
        const auto& chunkReplication = hunkStorage->Replication();
        auto primaryMediumIndex = hunkStorage->GetPrimaryMediumIndex();
        auto* primaryMedium = chunkManager->GetMediumByIndex(primaryMediumIndex);
        auto replicationFactor = chunkReplication.Get(primaryMediumIndex).GetReplicationFactor();

        auto storeWriterOptions = New<NTabletNode::THunkStoreWriterOptions>();
        storeWriterOptions->MediumName = primaryMedium->GetName();
        storeWriterOptions->Account = hunkStorage->Account()->GetName();
        storeWriterOptions->ErasureCodec = hunkStorage->GetErasureCodec();
        storeWriterOptions->ReplicationFactor = replicationFactor;
        storeWriterOptions->ReadQuorum = hunkStorage->GetReadQuorum();
        storeWriterOptions->WriteQuorum = hunkStorage->GetWriteQuorum();
        storeWriterOptions->EnableMultiplexing = false;
        storeWriterOptions->Postprocess();

        result.HunkStoreOptions = std::move(storeWriterOptions);
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Error parsing hunk store writer options")
            << ex;
    }

    return result;
}

TSerializedHunkStorageSettings SerializeHunkStorageSettings(const THunkStorageSettings& settings)
{
    return {
        .MountConfig = ConvertToYsonString(settings.MountConfig),
        .HunkStoreConfig = ConvertToYsonString(settings.HunkStoreConfig),
        .HunkStoreOptions = ConvertToYsonString(settings.HunkStoreOptions),
    };
}

TTabletOwnerSettings GetTabletOwnerSettings(
    TTabletOwnerBase* table,
    const IObjectManagerPtr& objectManager,
    const IChunkManagerPtr& chunkManager,
    const TDynamicTabletManagerConfigPtr& dynamicConfig)
{
    if (IsTableType(table->GetType())) {
        return GetTableSettings(table->As<TTableNode>(), objectManager, chunkManager, dynamicConfig);
    } else if (table->GetType() == EObjectType::HunkStorage) {
        return GetHunkStorageSettings(table->As<THunkStorageNode>(), objectManager, chunkManager, dynamicConfig);
    } else {
        YT_ABORT();
    }
}

TSerializedTabletOwnerSettings SerializeTabletOwnerSettings(const TTabletOwnerSettings& settings)
{
    return Visit(settings,
        [] (const TTableSettings& settings) -> TSerializedTabletOwnerSettings { return SerializeTableSettings(settings); },
        [] (const THunkStorageSettings& settings) -> TSerializedTabletOwnerSettings { return SerializeHunkStorageSettings(settings); },
        [] (auto) { YT_ABORT(); });
}

void ValidateTableMountConfig(
    const TTableNode* table,
    const TTableMountConfigPtr& mountConfig,
    const TDynamicTabletManagerConfigPtr& dynamicConfig)
{
    if (table->IsPhysicallyLog() && mountConfig->InMemoryMode != EInMemoryMode::None) {
        THROW_ERROR_EXCEPTION("Cannot mount dynamic table of type %Qlv in memory",
            table->GetType());
    }
    if (!table->IsPhysicallySorted() && mountConfig->EnableLookupHashTable) {
        THROW_ERROR_EXCEPTION("\"enable_lookup_hash_table\" can be \"true\" only for sorted dynamic table");
    }

    if (dynamicConfig->ForbidArbitraryDataVersionsInRetentionConfig) {
        if (mountConfig->MinDataVersions > 1) {
            THROW_ERROR_EXCEPTION("\"min_data_versions\" must be not greater than 1");
        }

        if (mountConfig->MaxDataVersions > 1) {
            THROW_ERROR_EXCEPTION("\"max_data_versions\" must be not greater than 1");
        }

        if (mountConfig->MinDataVersions > mountConfig->MaxDataVersions) {
            THROW_ERROR_EXCEPTION("\"min_data_versions\" must be not greater than \"max_data_versions\"");
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletServer
