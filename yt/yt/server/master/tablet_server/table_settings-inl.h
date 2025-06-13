#ifndef TABLE_SETTINGS_INL_H_
#error "Direct inclusion of this file is not allowed, include table_settings.h"
// For the sake of sane code completion.
#include "table_settings.h"
#endif

#include <yt/yt/core/yson/protobuf_helpers.h>

namespace NYT::NTabletServer {

////////////////////////////////////////////////////////////////////////////////

template <class TRequest>
void FillTableSettings(TRequest* request, const TSerializedTableSettings& serializedTableSettings)
{
    using NYT::ToProto;
    auto* tableSettings = request->mutable_table_settings();
    tableSettings->set_mount_config(ToProto(serializedTableSettings.MountConfig));
    if (serializedTableSettings.ExtraMountConfig) {
        tableSettings->set_extra_mount_config_attributes(
            serializedTableSettings.ExtraMountConfig.ToString());
    }
    tableSettings->set_store_reader_config(ToProto(serializedTableSettings.StoreReaderConfig));
    tableSettings->set_hunk_reader_config(ToProto(serializedTableSettings.HunkReaderConfig));
    tableSettings->set_store_writer_config(ToProto(serializedTableSettings.StoreWriterConfig));
    tableSettings->set_store_writer_options(ToProto(serializedTableSettings.StoreWriterOptions));
    tableSettings->set_hunk_writer_config(ToProto(serializedTableSettings.HunkWriterConfig));
    tableSettings->set_hunk_writer_options(ToProto(serializedTableSettings.HunkWriterOptions));
    tableSettings->set_global_patch(ToProto(serializedTableSettings.GlobalPatch));
    tableSettings->set_experiments(ToProto(serializedTableSettings.Experiments));
}

template <class TRequest>
void FillHunkStorageSettings(TRequest* request, const TSerializedHunkStorageSettings& settings)
{
    using NYT::ToProto;
    auto* hunkStorageSettings = request->mutable_hunk_storage_settings();
    hunkStorageSettings->set_mount_config(ToProto(settings.MountConfig));
    hunkStorageSettings->set_hunk_store_config(ToProto(settings.HunkStoreConfig));
    hunkStorageSettings->set_hunk_store_options(ToProto(settings.HunkStoreOptions));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletServer
