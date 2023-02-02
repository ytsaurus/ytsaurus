#ifndef TABLE_SETTINGS_INL_H_
#error "Direct inclusion of this file is not allowed, include table_settings.h"
// For the sake of sane code completion.
#include "table_settings.h"
#endif

namespace NYT::NTabletServer {

////////////////////////////////////////////////////////////////////////////////

template <class TRequest>
void FillTableSettings(TRequest* request, const TSerializedTableSettings& serializedTableSettings)
{
    auto* tableSettings = request->mutable_table_settings();
    tableSettings->set_mount_config(serializedTableSettings.MountConfig.ToString());
    if (serializedTableSettings.ExtraMountConfig) {
        tableSettings->set_extra_mount_config_attributes(
            serializedTableSettings.ExtraMountConfig.ToString());
    }
    tableSettings->set_store_reader_config(serializedTableSettings.StoreReaderConfig.ToString());
    tableSettings->set_hunk_reader_config(serializedTableSettings.HunkReaderConfig.ToString());
    tableSettings->set_store_writer_config(serializedTableSettings.StoreWriterConfig.ToString());
    tableSettings->set_store_writer_options(serializedTableSettings.StoreWriterOptions.ToString());
    tableSettings->set_hunk_writer_config(serializedTableSettings.HunkWriterConfig.ToString());
    tableSettings->set_hunk_writer_options(serializedTableSettings.HunkWriterOptions.ToString());
    tableSettings->set_global_patch(serializedTableSettings.GlobalPatch.ToString());
    tableSettings->set_experiments(serializedTableSettings.Experiments.ToString());
}

template <class TRequest>
void FillHunkStorageSettings(TRequest* request, const TSerializedHunkStorageSettings& settings)
{
    auto* hunkStorageSettings = request->mutable_hunk_storage_settings();
    hunkStorageSettings->set_mount_config(settings.MountConfig.ToString());
    hunkStorageSettings->set_hunk_store_config(settings.HunkStoreConfig.ToString());
    hunkStorageSettings->set_hunk_store_options(settings.HunkStoreOptions.ToString());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletServer
